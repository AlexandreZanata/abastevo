//! RST-02 acceptance against the frozen RST-01 fixtures in
//! `contracts/testdata/station-prep/`. Every expectation below mirrors a
//! decision in `docs/planning/RUST_STATION_INGESTION_RST01.md`.

use station_prep::{parse_pmqc, parse_registry, AliasTable, Limits, RunState};
use std::path::PathBuf;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/testdata/station-prep")
}

fn bytes(name: &str) -> Vec<u8> {
    std::fs::read(dir().join(name)).expect("fixture must exist")
}

fn text(name: &str) -> String {
    String::from_utf8(bytes(name)).expect("fixture must be UTF-8")
}

fn aliases() -> AliasTable {
    AliasTable::from_json(&text("ibge-mapping-sample.json")).expect("alias sample must load")
}

fn jsonl_keys(line: &str) -> Vec<String> {
    let value: serde_json::Value = serde_json::from_str(line).expect("must be JSON");
    let mut keys: Vec<String> = value
        .as_object()
        .expect("must be an object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}

#[test]
fn registry_sample_counts_reconcile() {
    let batch = parse_registry(
        "registry-13col-sample.csv",
        bytes("registry-13col-sample.csv"),
        &Limits::default(),
        &aliases(),
    )
    .expect("sample must parse");
    assert_eq!(batch.state, RunState::Complete);
    assert_eq!(batch.counts.input, 12);
    assert_eq!(batch.counts.accepted, 8);
    assert_eq!(batch.counts.duplicates, 1);
    assert_eq!(batch.counts.quarantined, 3);
    assert_eq!(
        batch.counts.accepted + batch.counts.duplicates + batch.counts.quarantined,
        batch.counts.input
    );
    // Older conflicting evidence is retained as superseded history.
    let alfa: Vec<_> = batch
        .accepted
        .iter()
        .filter(|row| row.source_key == "48151623000191")
        .collect();
    assert_eq!(alfa.len(), 2);
    let (older, newer) = if alfa[0].published_at < alfa[1].published_at {
        (alfa[0], alfa[1])
    } else {
        (alfa[1], alfa[0])
    };
    assert_eq!(older.supersedes.as_deref(), Some(newer.checksum.as_str()));
    assert_eq!(older.address_raw, "AV PAULISTA 900");
    assert_eq!(newer.supersedes, None);
    // Accepted rows keep a stable order independent of input order.
    let keys: Vec<_> = batch
        .accepted
        .iter()
        .map(|row| (row.source_key.clone(), row.checksum.clone()))
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    // Quarantine locators use logical record numbers (header is 1).
    let reasons: Vec<_> = batch
        .quarantine
        .iter()
        .map(|row| (row.row_locator.clone(), row.reason))
        .collect();
    assert!(reasons.contains(&("registry-13col-sample.csv:5".to_string(), "unknown_city")));
    assert!(reasons.contains(&("registry-13col-sample.csv:8".to_string(), "invalid_cnpj")));
    assert!(reasons.contains(&("registry-13col-sample.csv:9".to_string(), "invalid_cnpj")));
    assert!(!reasons
        .iter()
        .any(|(locator, _)| locator == "registry-13col-sample.csv:10"));
    // Quoted separator and embedded newline survive as typed values.
    let quebra = batch
        .accepted
        .iter()
        .find(|row| row.source_key == "04218406000104")
        .expect("quoted row must be accepted");
    assert_eq!(quebra.address_raw, "RUA DAS FLORES; BLOCO \"B\"");
    assert!(quebra.address_normalized.complement.contains('\n'));
    // Case/accent alias row resolves through normalization.
    let baixo = batch
        .accepted
        .iter()
        .find(|row| row.source_key == "55881177000136")
        .expect("alias row must be accepted");
    assert_eq!(baixo.uf, "SP");
    assert_eq!(baixo.municipality_ibge, "3550308");
    // Extreme field stays bounded and accepted.
    let longo = batch
        .accepted
        .iter()
        .find(|row| row.source_key == "77215333000162")
        .expect("long row must be accepted");
    assert!(longo.address_normalized.complement.len() > 1000);
}

#[test]
fn registry_reordered_input_keeps_identical_accepted_output() {
    // Split on logical rows: every data row starts with `SIMP-` at a line
    // start, so the embedded newline inside R5's quoted field survives.
    let raw = text("registry-13col-sample.csv");
    let (header, body) = raw.split_once('\n').expect("header line");
    let mut rows: Vec<String> = body
        .split("\nSIMP-")
        .enumerate()
        .map(|(position, row)| {
            if position == 0 {
                row.to_string()
            } else {
                format!("SIMP-{row}")
            }
        })
        .collect();
    assert_eq!(rows.len(), 12);
    rows.reverse();
    let reordered = format!("{header}\n{}\n", rows.join("\n"));
    let first = parse_registry(
        "registry-13col-sample.csv",
        bytes("registry-13col-sample.csv"),
        &Limits::default(),
        &aliases(),
    )
    .expect("sample must parse");
    let second = parse_registry(
        "registry-13col-sample.csv",
        reordered.into_bytes(),
        &Limits::default(),
        &aliases(),
    )
    .expect("reordered sample must parse");
    let to_jsonl = |batch: &station_prep::RegistryBatch| {
        batch
            .accepted
            .iter()
            .map(|row| row.to_jsonl())
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(to_jsonl(&first), to_jsonl(&second));
    assert_eq!(second.counts, first.counts);
}

#[test]
fn registry_bom_prefix_is_stripped() {
    let mut raw = vec![0xEF, 0xBB, 0xBF];
    raw.extend(bytes("registry-13col-sample.csv"));
    let batch = parse_registry(
        "registry-13col-sample.csv",
        raw,
        &Limits::default(),
        &aliases(),
    )
    .expect("BOM must strip");
    assert_eq!(batch.state, RunState::Complete);
    assert_eq!(batch.counts.input, 12);
    assert_eq!(batch.counts.accepted, 8);
}

#[test]
fn registry_header_variant_quarantines_the_run() {
    let batch = parse_registry(
        "registry-13col-header-variant.csv",
        bytes("registry-13col-header-variant.csv"),
        &Limits::default(),
        &aliases(),
    )
    .expect("variant must report, not fail");
    assert_eq!(batch.state, RunState::Quarantined);
    assert_eq!(batch.error_code, "header_mismatch");
    assert_eq!(batch.counts.input, 0);
    assert_eq!(batch.quarantine.len(), 1);
    assert_eq!(
        batch.quarantine[0].row_locator,
        "registry-13col-header-variant.csv:header"
    );
}

#[test]
fn registry_truncated_input_fails_without_partial_output() {
    // Cut inside R5's quoted field, leaving an unterminated quote: the
    // stream is undecodable, so the whole run fails with no partial rows.
    let raw = bytes("registry-13col-sample.csv");
    let text = String::from_utf8(raw.clone()).expect("sample is UTF-8");
    let cut = text.find("SALA 1").expect("quoted field must exist") + "SALA 1".len();
    let err = parse_registry(
        "registry-13col-sample.csv",
        raw[..cut].to_vec(),
        &Limits::default(),
        &aliases(),
    );
    assert!(err.is_err(), "truncated quote must fail, got {err:?}");
}

#[test]
fn pmqc_sample_counts_reconcile() {
    let batch = parse_pmqc(
        "pmqc-sample.csv",
        bytes("pmqc-sample.csv"),
        &Limits::default(),
    )
    .expect("sample must parse");
    assert_eq!(batch.state, RunState::Complete);
    assert_eq!(batch.counts.input, 11);
    assert_eq!(batch.counts.accepted, 5);
    assert_eq!(batch.counts.duplicates, 1);
    assert_eq!(batch.counts.quarantined, 5);
    // Duplicate assays collapse to one candidate.
    assert_eq!(
        batch
            .candidates
            .iter()
            .filter(|row| row.sample_ref == "A-001")
            .count(),
        1
    );
    // Empty coordinates stay an accepted candidate without a point.
    let empty = batch
        .candidates
        .iter()
        .find(|row| row.sample_ref == "A-003")
        .expect("empty coords stay a candidate");
    assert_eq!(empty.latitude, None);
    // Stale observations stay candidates; review owns freshness.
    assert!(batch.candidates.iter().any(|row| row.sample_ref == "A-008"));
    let mut reasons: Vec<&str> = batch.quarantine.iter().map(|row| row.reason).collect();
    reasons.sort();
    assert_eq!(
        reasons,
        vec![
            "invalid_cnpj",
            "invalid_point",
            "invalid_point",
            "out_of_brazil",
            "suspect_swap"
        ]
    );
    let locators: Vec<&str> = batch
        .quarantine
        .iter()
        .map(|row| row.row_locator.as_str())
        .collect();
    for locator in [
        "pmqc-sample.csv:6",
        "pmqc-sample.csv:7",
        "pmqc-sample.csv:8",
        "pmqc-sample.csv:9",
        "pmqc-sample.csv:12",
    ] {
        assert!(locators.contains(&locator), "missing {locator}");
    }
}

#[test]
fn pmqc_reordered_input_keeps_identical_candidates() {
    let raw = text("pmqc-sample.csv");
    let mut lines: Vec<&str> = raw.lines().collect();
    let header = lines.remove(0);
    lines.reverse();
    let reordered = format!("{header}\n{}\n", lines.join("\n"));
    let first = parse_pmqc(
        "pmqc-sample.csv",
        bytes("pmqc-sample.csv"),
        &Limits::default(),
    )
    .expect("sample parses");
    let second = parse_pmqc(
        "pmqc-sample.csv",
        reordered.into_bytes(),
        &Limits::default(),
    )
    .expect("reordered parses");
    let to_jsonl = |batch: &station_prep::PmqcBatch| {
        batch
            .candidates
            .iter()
            .map(|row| row.to_jsonl())
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(to_jsonl(&first), to_jsonl(&second));
}

#[test]
fn emitted_jsonl_matches_frozen_schema_keys() {
    let registry = parse_registry(
        "registry-13col-sample.csv",
        bytes("registry-13col-sample.csv"),
        &Limits::default(),
        &aliases(),
    )
    .expect("sample must parse");
    let sample_assertion = text("output-assertions-v2-sample.jsonl");
    assert_eq!(
        jsonl_keys(&registry.accepted[0].to_jsonl()),
        jsonl_keys(sample_assertion.lines().next().expect("sample line"))
    );
    let pmqc = parse_pmqc(
        "pmqc-sample.csv",
        bytes("pmqc-sample.csv"),
        &Limits::default(),
    )
    .expect("sample parses");
    let sample_candidate = text("output-candidates-sample.jsonl");
    assert_eq!(
        jsonl_keys(&pmqc.candidates[0].to_jsonl()),
        jsonl_keys(sample_candidate.lines().next().expect("sample line"))
    );
    let sample_quarantine = text("output-quarantine-sample.jsonl");
    assert_eq!(
        jsonl_keys(&registry.quarantine[0].to_jsonl()),
        jsonl_keys(sample_quarantine.lines().next().expect("sample line"))
    );
}

#[test]
fn bounds_are_enforced() {
    let raw = bytes("registry-13col-sample.csv");
    let tiny = Limits {
        max_bytes: 10,
        ..Limits::default()
    };
    assert!(parse_registry("registry-13col-sample.csv", raw.clone(), &tiny, &aliases()).is_err());
    let few = Limits {
        max_rows: 2,
        ..Limits::default()
    };
    assert!(parse_registry("registry-13col-sample.csv", raw.clone(), &few, &aliases()).is_err());
    let narrow = Limits {
        max_field_bytes: 100,
        ..Limits::default()
    };
    let batch = parse_registry("registry-13col-sample.csv", raw, &narrow, &aliases())
        .expect("narrow fields quarantine rows");
    assert!(
        batch
            .quarantine
            .iter()
            .any(|row| row.reason == "record_too_large"),
        "2000-char field must quarantine at 100 bytes"
    );
}

#[test]
fn anp_calendar_dates_are_normalized_with_independent_brand_linkage() {
    let header = String::from_utf8(bytes("registry-13col-sample.csv"))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();
    for (published, linked, want) in [
        ("15/03/2001", "04/11/2009", "2001-03-15"),
        ("01/05/2024", "01/04/2024", "2024-05-01"),
        ("2024-02-29", "2023-01-01", "2024-02-29"),
    ] {
        let csv=format!("{header}\nSIMP;PRC;{published};[RST-H-TEST] DATE;04218406000104;RUA 1;;CENTRO;01000000;SP;SÃO PAULO;BRANCA;{linked}\n");
        let batch =
            parse_registry("date.csv", csv.into_bytes(), &Limits::default(), &aliases()).unwrap();
        assert_eq!(
            batch.counts.accepted, 1,
            "calendar/linkage semantics: {published} {linked}"
        );
        assert_eq!(batch.accepted[0].published_at, want);
        assert_eq!(batch.accepted[0].effective_at, want);
    }
}

#[test]
fn invalid_calendar_dates_quarantine_without_panicking() {
    let header = String::from_utf8(bytes("registry-13col-sample.csv"))
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();
    for invalid in [
        "2023-02-29",
        "31/04/2024",
        "00/12/2024",
        "2024-13-01",
        "0000-01-01",
        "é024-01-0",
        "02/03/abcd",
    ] {
        let csv=format!("{header}\nSIMP;PRC;{invalid};[RST-H-TEST] INVALID DATE;04218406000104;RUA 1;;CENTRO;01000000;SP;SÃO PAULO;BRANCA;01/01/2000\n");
        let batch =
            parse_registry("date.csv", csv.into_bytes(), &Limits::default(), &aliases()).unwrap();
        assert_eq!(batch.counts.accepted, 0, "{invalid}");
        assert_eq!(batch.counts.quarantined, 1, "{invalid}");
        assert_eq!(batch.quarantine[0].reason, "invalid_field");
    }
}
