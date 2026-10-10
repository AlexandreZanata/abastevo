//! Raw 13-column ANP registry reader: local-file streaming CSV to typed
//! rows. Header matches by name, never position; a changed header
//! quarantines the whole run. Membership in the snapshot is recorded as
//! evidence (`auth_state: unknown`, `eligibility: pending`); it never
//! mints an authorization grant.

use crate::cnpj::normalize_cnpj;
use crate::municipality::{AliasTable, MunicipalityError};
use crate::types::{reason, sha256_hex, Counts, Limits, QuarantineRow, RunState};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

/// Row source label for `station-assertion-v1` output.
pub const REGISTRY_SOURCE: &str = "registry-13col";

/// Frozen raw header, in observed order. Matching is by name.
const RAW_HEADER: [&str; 13] = [
    "CODIGOISIMP",
    "AUTORIZACAO",
    "DATAPUBLICACAO",
    "RAZAOSOCIAL",
    "CNPJ",
    "ENDERECO",
    "COMPLEMENTO",
    "BAIRRO",
    "CEP",
    "UF",
    "MUNICIPIO",
    "BANDEIRA",
    "DATAVINCULACAO",
];

/// Transport-level failures. `Err` carries no partial output; quarantined
/// runs are `Ok` with state [`RunState::Quarantined`].
#[derive(Debug)]
pub enum RegistryError {
    /// Input exceeds `Limits::max_bytes`.
    Oversize,
    /// More data rows than `Limits::max_rows`.
    RowCap,
    /// CSV decoding broke mid-file (truncation included).
    Truncated(String),
    /// Input is not valid UTF-8.
    Encoding,
}

/// Best-effort street/number split of the preserved combined address.
/// The raw string is always kept; a trailing numeric token becomes the
/// number, otherwise the number stays empty (road/km forms included).
fn split_street_number(raw: &str) -> (String, String) {
    let trimmed = raw.trim();
    match trimmed.rsplit_once(' ') {
        Some((street, number))
            if !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()) =>
        {
            (street.to_string(), number.to_string())
        }
        _ => (trimmed.to_string(), String::new()),
    }
}

fn collapse_whitespace(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Canonical row checksum: SHA256 over typed values joined with `\x1f`.
/// Whitespace-only differences outside typed values cannot churn it;
/// byte-identical rows always collide. The form is frozen: changing it
/// changes every checksum, so the golden test in `tests/deltas.rs` pins
/// one production-shaped vector.
fn row_checksum(fields: &[&str]) -> String {
    sha256_hex(format!("station-assertion-v2\x1f{}", fields.join("\x1f")).as_bytes())
}

// Strict calendar parsing without locale guesses. The official CSV uses
// DD/MM/YYYY; older synthetic fixtures use ISO. Brand linkage is independent.
fn normalize_calendar_date(raw: &str) -> Result<String, &'static str> {
    if raw.is_empty() {
        return Ok(String::new());
    }
    let bytes = raw.as_bytes();
    if bytes.len() != 10 || !raw.is_ascii() {
        return Err("expected DD/MM/YYYY or YYYY-MM-DD");
    }
    let parts = if bytes[2] == b'/' && bytes[5] == b'/' {
        [&raw[6..10], &raw[3..5], &raw[0..2]]
    } else if bytes[4] == b'-' && bytes[7] == b'-' {
        [&raw[0..4], &raw[5..7], &raw[8..10]]
    } else {
        return Err("expected DD/MM/YYYY or YYYY-MM-DD");
    };
    if !parts.iter().all(|p| p.bytes().all(|b| b.is_ascii_digit())) {
        return Err("non-digit calendar component");
    }
    let year: u32 = parts[0].parse().map_err(|_| "invalid year")?;
    let month: u32 = parts[1].parse().map_err(|_| "invalid month")?;
    let day: u32 = parts[2].parse().map_err(|_| "invalid day")?;
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    if year == 0 || day == 0 || day > days {
        return Err("invalid Gregorian date");
    }
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

/// Normalized address projection of one accepted row.
#[derive(Debug, Clone, Serialize)]
pub struct NormalizedAddress {
    pub street: String,
    pub number: String,
    pub complement: String,
    pub district: String,
    pub postal_code: String,
}

/// One accepted registry row, serializable as `station-assertion-v2`.
#[derive(Debug, Clone, Serialize)]
pub struct RegistryRow {
    pub schema_version: &'static str,
    pub source: &'static str,
    pub source_key: String,
    pub checksum: String,
    pub simp_ref: String,
    pub authorization_ref: String,
    pub business_name_raw: String,
    pub business_name_normalized: String,
    pub address_raw: String,
    pub address_normalized: NormalizedAddress,
    pub uf: String,
    pub municipality_name_raw: String,
    pub municipality_ibge: String,
    pub brand_raw: String,
    pub published_at: String,
    pub effective_at: String,
    pub brand_linked_at: String,
    pub auth_state: String,
    pub eligibility: String,
    pub auth_evidence: String,
    pub location_quality: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub crs: String,
    pub supersedes: Option<String>,
}

impl RegistryRow {
    pub fn to_jsonl(&self) -> String {
        serde_json::to_string(self).expect("registry row must serialize")
    }
}

/// Parsed registry batch with reconciled counts.
#[derive(Debug)]
/// Parsed registry batch. `accepted` holds unique rows; `duplicates`
/// retains one byte-identical repeat per within-run exact repeat (in
/// repeat order) so emission carries the full accepted+duplicate
/// multiset the manifest accounts for and the Go loader
/// independently re-verifies. Quarantined rows never enter either.
pub struct RegistryBatch {
    pub state: RunState,
    pub error_code: String,
    pub counts: Counts,
    pub accepted: Vec<RegistryRow>,
    pub duplicates: Vec<RegistryRow>,
    pub quarantine: Vec<QuarantineRow>,
}

fn quarantine_row(
    file: &str,
    logical_row: usize,
    reason: &'static str,
    detail: String,
    source_key: Option<String>,
) -> QuarantineRow {
    QuarantineRow {
        schema_version: "station-quarantine-v1",
        source: REGISTRY_SOURCE,
        row_locator: format!("{file}:{logical_row}"),
        reason,
        detail,
        source_key,
    }
}

/// Parse one raw registry snapshot. `file` names the input for row
/// locators only; bytes are never read from disk here.
pub fn parse_registry(
    file: &str,
    raw: Vec<u8>,
    limits: &Limits,
    aliases: &AliasTable,
) -> Result<RegistryBatch, RegistryError> {
    if raw.len() as u64 > limits.max_bytes {
        return Err(RegistryError::Oversize);
    }
    let text = std::str::from_utf8(&raw).map_err(|_| RegistryError::Encoding)?;
    let text = text.strip_prefix('\u{FEFF}').unwrap_or(text);
    // Framing rule: a complete snapshot ends with a record terminator.
    // A cut mid-row or mid-quote is otherwise indistinguishable from a
    // shorter valid file here; residual boundary cuts are caught later by
    // manifest row counts in the Go-owned loader (RST-05).
    if !text.ends_with('\n') {
        return Err(RegistryError::Truncated(
            "missing final record terminator; truncated snapshot".to_string(),
        ));
    }

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(text.as_bytes());

    let header = reader
        .headers()
        .map_err(|err| RegistryError::Truncated(err.to_string()))?
        .clone();
    let names: Vec<String> = header
        .iter()
        .map(|name| name.trim().to_uppercase())
        .collect();
    let position = |wanted: &str| names.iter().position(|name| name == wanted);
    let index: HashMap<&str, usize> = RAW_HEADER
        .iter()
        .filter_map(|wanted| position(wanted).map(|at| (*wanted, at)))
        .collect();
    let missing: Vec<&str> = RAW_HEADER
        .iter()
        .filter(|wanted| !index.contains_key(**wanted))
        .copied()
        .collect();
    let unknown: Vec<String> = names
        .iter()
        .filter(|name| !RAW_HEADER.contains(&name.as_str()))
        .cloned()
        .collect();
    if !missing.is_empty() || !unknown.is_empty() {
        return Ok(RegistryBatch {
            state: RunState::Quarantined,
            error_code: "header_mismatch".to_string(),
            counts: Counts::default(),
            accepted: Vec::new(),
            duplicates: Vec::new(),
            quarantine: vec![QuarantineRow {
                schema_version: "station-quarantine-v1",
                source: REGISTRY_SOURCE,
                row_locator: format!("{file}:header"),
                reason: reason::HEADER_MISMATCH,
                detail: format!("missing {missing:?}; unknown {unknown:?}; run quarantined"),
                source_key: None,
            }],
        });
    }
    let at = |record: &csv::StringRecord, wanted: &str| -> String {
        record.get(index[wanted]).unwrap_or("").trim().to_string()
    };

    let mut counts = Counts::default();
    let mut accepted = Vec::new();
    let mut quarantine = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    // Checksums of exact repeats, in repeat order; resolved to
    // byte-identical accepted twins after succession linking so the
    // emitted multiset stays order-independent.
    let mut dup_checksums: Vec<String> = Vec::new();

    for (offset, next) in reader.records().enumerate() {
        // Logical record number: the header is record 1.
        let logical_row = offset + 2;
        if counts.input >= limits.max_rows {
            return Err(RegistryError::RowCap);
        }
        counts.input += 1;
        let record = next.map_err(|err| RegistryError::Truncated(err.to_string()))?;
        let fields: Vec<String> = RAW_HEADER
            .iter()
            .map(|wanted| at(&record, wanted))
            .collect();
        if let Some(oversize) = RAW_HEADER
            .iter()
            .zip(&fields)
            .find(|(_, value)| value.len() > limits.max_field_bytes)
        {
            counts.quarantined += 1;
            quarantine.push(quarantine_row(
                file,
                logical_row,
                reason::RECORD_TOO_LARGE,
                format!(
                    "field {} exceeds {} bytes",
                    oversize.0, limits.max_field_bytes
                ),
                None,
            ));
            continue;
        }
        // Positions follow RAW_HEADER order.
        let simp = fields[0].clone();
        let authorization_ref = fields[1].clone();
        let published = fields[2].clone();
        let business = fields[3].clone();
        let cnpj_raw = fields[4].clone();
        let address_raw = fields[5].clone();
        let complement = fields[6].clone();
        let district = fields[7].clone();
        let postal = fields[8].clone();
        let uf_raw = fields[9].clone();
        let municipality_raw = fields[10].clone();
        let brand = fields[11].clone();
        let effective = fields[12].clone();

        let cnpj = match normalize_cnpj(&cnpj_raw) {
            Ok(cnpj) => cnpj,
            Err(_) => {
                counts.quarantined += 1;
                quarantine.push(quarantine_row(
                    file,
                    logical_row,
                    reason::INVALID_CNPJ,
                    "identifier fails checksum vector".to_string(),
                    None,
                ));
                continue;
            }
        };
        if business.is_empty() {
            counts.quarantined += 1;
            quarantine.push(quarantine_row(
                file,
                logical_row,
                reason::INVALID_FIELD,
                "empty business name".to_string(),
                Some(cnpj),
            ));
            continue;
        }
        let uf = uf_raw.to_uppercase();
        if uf.len() != 2 {
            counts.quarantined += 1;
            quarantine.push(quarantine_row(
                file,
                logical_row,
                reason::INVALID_FIELD,
                format!("malformed UF {uf_raw:?}"),
                Some(cnpj),
            ));
            continue;
        }
        let ibge = match aliases.resolve(&municipality_raw, &uf) {
            Ok(ibge) => ibge,
            Err(MunicipalityError::Unknown) => {
                counts.quarantined += 1;
                quarantine.push(quarantine_row(
                    file,
                    logical_row,
                    reason::UNKNOWN_CITY,
                    format!("municipality {municipality_raw:?} has no reference entry"),
                    Some(cnpj),
                ));
                continue;
            }
            Err(MunicipalityError::Ambiguous) => {
                counts.quarantined += 1;
                quarantine.push(quarantine_row(
                    file,
                    logical_row,
                    reason::AMBIGUOUS_MUNICIPALITY,
                    format!("municipality {municipality_raw:?} matches several codes"),
                    Some(cnpj),
                ));
                continue;
            }
        };
        let published = match normalize_calendar_date(&published) {
            Ok(value) => value,
            Err(detail) => {
                counts.quarantined += 1;
                quarantine.push(quarantine_row(
                    file,
                    logical_row,
                    reason::INVALID_FIELD,
                    format!("DATAPUBLICACAO: {detail}"),
                    Some(cnpj),
                ));
                continue;
            }
        };
        let effective = match normalize_calendar_date(&effective) {
            Ok(value) => value,
            Err(detail) => {
                counts.quarantined += 1;
                quarantine.push(quarantine_row(
                    file,
                    logical_row,
                    reason::INVALID_FIELD,
                    format!("DATAVINCULACAO: {detail}"),
                    Some(cnpj),
                ));
                continue;
            }
        };
        let checksum = row_checksum(&[
            &cnpj,
            &simp,
            &authorization_ref,
            &business,
            &address_raw,
            &complement,
            &district,
            &postal,
            &uf,
            &municipality_raw,
            &brand,
            &published,
            &effective,
        ]);
        if !seen.insert(checksum.clone()) {
            counts.duplicates += 1;
            dup_checksums.push(checksum);
            continue;
        }
        let (street, number) = split_street_number(&address_raw);
        counts.accepted += 1;
        accepted.push(RegistryRow {
            schema_version: "station-assertion-v2",
            source: REGISTRY_SOURCE,
            source_key: cnpj,
            checksum,
            simp_ref: simp,
            authorization_ref,
            business_name_raw: business.clone(),
            business_name_normalized: collapse_whitespace(&business),
            address_raw,
            address_normalized: NormalizedAddress {
                street,
                number,
                complement,
                district,
                postal_code: postal,
            },
            uf,
            municipality_name_raw: municipality_raw,
            municipality_ibge: ibge,
            brand_raw: brand,
            published_at: published.clone(),
            effective_at: published.clone(),
            brand_linked_at: effective,
            auth_state: "unknown".to_string(),
            eligibility: "pending".to_string(),
            auth_evidence: format!(
                "operating-membership {} snapshot; not an authorization grant",
                if published.is_empty() {
                    "unknown"
                } else {
                    &published
                }
            ),
            location_quality: "unknown".to_string(),
            latitude: None,
            longitude: None,
            crs: String::new(),
            supersedes: None,
        });
    }

    // Succession: same establishment, several snapshots. The newest
    // publication wins the projection; older evidence stays accepted as
    // history linked to its superseding checksum, never overwritten.
    let mut by_cnpj: HashMap<String, Vec<usize>> = HashMap::new();
    for (position, row) in accepted.iter().enumerate() {
        by_cnpj
            .entry(row.source_key.clone())
            .or_default()
            .push(position);
    }
    for positions in by_cnpj.values() {
        if positions.len() < 2 {
            continue;
        }
        let mut ordered = positions.clone();
        ordered.sort_by(|a, b| {
            accepted[*a]
                .published_at
                .cmp(&accepted[*b].published_at)
                .then_with(|| accepted[*a].checksum.cmp(&accepted[*b].checksum))
        });
        let newest = accepted[*ordered.last().expect("non-empty")]
            .checksum
            .clone();
        for position in &ordered[..ordered.len() - 1] {
            accepted[*position].supersedes = Some(newest.clone());
        }
    }

    // Deterministic output order, independent of input order.
    accepted.sort_by(|a, b| {
        a.source_key
            .cmp(&b.source_key)
            .then_with(|| a.checksum.cmp(&b.checksum))
    });

    // Resolve repeats to their accepted twins (post-link, so twins
    // carry identical succession links and the multiset is
    // order-independent).
    let mut by_checksum: HashMap<&str, &RegistryRow> = HashMap::new();
    for row in &accepted {
        by_checksum.entry(row.checksum.as_str()).or_insert(row);
    }
    let mut duplicates = Vec::with_capacity(dup_checksums.len());
    for checksum in &dup_checksums {
        duplicates.push(
            (*by_checksum
                .get(checksum.as_str())
                .expect("duplicate checksum was accepted"))
            .clone(),
        );
    }

    Ok(RegistryBatch {
        state: RunState::Complete,
        error_code: String::new(),
        counts,
        accepted,
        duplicates,
        quarantine,
    })
}
