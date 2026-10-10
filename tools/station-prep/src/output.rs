//! Deterministic output: sorted JSONL streams, a hashed batch manifest
//! and atomic publication to a directory. Identical logical content
//! always yields identical bytes, whether rows sort in memory or spill
//! through temp runs first.
//!
//! Parse batches stay bounded by [`Limits::max_rows`](crate::Limits), so
//! the in-memory path is the default. The spill path exists for larger
//! measured snapshots (RST-07 decides when it pays): past `spill_rows`
//! per stream, sorted runs spill to caller-provided scratch space and
//! merge back in key order. Both paths are covered by the same
//! byte-equivalence tests.

use crate::pmqc::PmqcBatch;
use crate::registry::RegistryBatch;
use crate::types::{sha256_hex, Counts};
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Parser and policy versions stamped on every manifest.
pub const PARSER_VERSION: &str = "station-prep-v0.3.0";
pub const POLICY_VERSION: &str = "station-policy-v2";

/// Published stream file names.
pub const ASSERTIONS_FILE: &str = "assertions.jsonl";
pub const CANDIDATES_FILE: &str = "candidates.jsonl";
pub const QUARANTINE_FILE: &str = "quarantine.jsonl";
pub const MANIFEST_FILE: &str = "manifest.json";

/// One consumed input for manifest provenance.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct SourceMeta {
    pub key: String,
    pub reference: String,
    pub edition: String,
    pub edition_seq: u64,
    pub sha256: String,
    pub bytes: u64,
    pub rows: usize,
    pub encoding: String,
}

impl SourceMeta {
    /// Fingerprint `raw` bytes as a manifest input entry.
    pub fn fingerprint(
        key: &str,
        reference: &str,
        edition: &str,
        edition_seq: u64,
        raw: &[u8],
        rows: usize,
    ) -> Self {
        Self {
            key: key.to_string(),
            reference: reference.to_string(),
            edition: edition.to_string(),
            edition_seq,
            sha256: sha256_hex(raw),
            bytes: raw.len() as u64,
            rows,
            encoding: "UTF-8 without BOM".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ManifestCounts {
    pub input: usize,
    pub accepted: usize,
    pub duplicates: usize,
    pub quarantined: usize,
}

impl From<Counts> for ManifestCounts {
    fn from(counts: Counts) -> Self {
        Self {
            input: counts.input,
            accepted: counts.accepted,
            duplicates: counts.duplicates,
            quarantined: counts.quarantined,
        }
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct MunicipalityReference {
    pub reference: String,
    pub reference_hash: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Completeness {
    pub eof_validated: bool,
    pub expected_manifest: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct OutputEntry {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub rows: usize,
}

/// Versioned batch manifest (`station-batch-v1` lineage). `inputs` keeps
/// insertion order; `counts` sorts by key for deterministic bytes.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Manifest {
    pub format_version: String,
    pub run_id: String,
    pub parser_version: String,
    pub policy_version: String,
    pub municipality_reference: MunicipalityReference,
    pub started_at: String,
    pub ended_at: String,
    pub inputs: Vec<SourceMeta>,
    pub counts: BTreeMap<String, ManifestCounts>,
    pub completeness: Completeness,
    pub outputs: Vec<OutputEntry>,
}

impl Manifest {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("manifest must serialize")
    }

    pub fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }
}

/// Emission options. `spill_rows` bounds one in-memory sort run;
/// `usize::MAX` keeps the pure in-memory path.
#[derive(Debug, Clone)]
pub struct EmitOptions {
    pub run_id: String,
    pub started_at: String,
    pub ended_at: String,
    pub spill_rows: usize,
}

/// One emitted run: stream bytes plus the manifest describing them.
/// `spill_runs` counts temporary sort runs written during emission and
/// `spill_bytes` their framed bytes (both zero on the in-memory path);
/// together they bound temporary disk for the RST-13 campaign.
#[derive(Debug, Clone)]
pub struct EmittedRun {
    pub assertions: Vec<u8>,
    pub candidates: Vec<u8>,
    pub quarantine: Vec<u8>,
    pub manifest: Manifest,
    pub spill_runs: usize,
    pub spill_bytes: u64,
}

impl EmittedRun {
    pub fn manifest_json(&self) -> String {
        self.manifest.to_json()
    }
}

/// Write `(sort key, JSONL line)` pairs in key order. Past `spill_rows`
/// pairs, sorted runs spill to `scratch` and merge back; otherwise the
/// whole stream sorts in memory with no files touched. Returns
/// `(written rows, spill run files, framed spill bytes)`.
fn write_sorted(
    pairs: impl IntoIterator<Item = (String, String)>,
    writer: &mut dyn Write,
    spill_rows: usize,
    scratch: &Path,
    stream: &str,
) -> std::io::Result<(usize, usize, u64)> {
    if spill_rows == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "spill_rows must be positive",
        ));
    }
    // Serialized working set is bounded independently of batch size.
    let mut chunk = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut runs = Vec::new();
    let mut spill_bytes = 0u64;
    let mut spill_count = 0;
    let flush = |chunk: &mut Vec<(String, String)>,
                 runs: &mut Vec<PathBuf>,
                 index: usize|
     -> std::io::Result<u64> {
        if runs.len() >= 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "spill run cap exceeded",
            ));
        }
        std::fs::create_dir_all(scratch)?;
        chunk.sort();
        let path = scratch.join(format!("{stream}-run-{index:06}.tmp"));
        let mut file = std::io::BufWriter::new(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?,
        );
        for (key, line) in chunk.iter() {
            writeln!(file, "{:08x}{}{}", key.len(), key, line)?;
        }
        file.flush()?;
        let size = std::fs::metadata(&path)?.len();
        runs.push(path);
        chunk.clear();
        Ok(size)
    };
    for (key, line) in pairs {
        if key.len() + line.len() > 1 << 20 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "emission row exceeds 1 MiB",
            ));
        }
        if !chunk.is_empty()
            && (chunk.len() >= spill_rows.min(4096)
                || chunk_bytes + key.len() + line.len() > 4 << 20)
        {
            spill_bytes += flush(&mut chunk, &mut runs, spill_count)?;
            spill_count += 1;
            if spill_bytes > 512 << 20 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "spill byte cap exceeded",
                ));
            }
            chunk_bytes = 0;
        }
        chunk_bytes += key.len() + line.len();
        chunk.push((key, line));
    }
    if runs.is_empty() {
        chunk.sort();
        for (_, line) in &chunk {
            writeln!(writer, "{line}")?;
        }
        return Ok((chunk.len(), 0, 0));
    }
    if !chunk.is_empty() {
        spill_bytes += flush(&mut chunk, &mut runs, spill_count)?;
        spill_count += 1;
        if spill_bytes > 512 << 20 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "spill byte cap exceeded",
            ));
        }
    }
    // Multi-pass fan-in bounds open descriptors and one lookahead row per run.
    let mut pass = 0;
    while runs.len() > 32 {
        let mut next = Vec::new();
        for (index, group) in runs.chunks(32).enumerate() {
            let path = scratch.join(format!("{stream}-merge-{pass}-{index}.tmp"));
            let mut file = std::io::BufWriter::new(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?,
            );
            merge_runs(group, &mut file, true)?;
            file.flush()?;
            spill_bytes += std::fs::metadata(&path)?.len();
            spill_count += 1;
            if spill_bytes > 512 << 20 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "spill byte cap exceeded",
                ));
            }
            next.push(path);
            for input in group {
                std::fs::remove_file(input)?;
            }
        }
        runs = next;
        pass += 1;
    }
    let written = merge_runs(&runs, writer, false)?;
    for path in &runs {
        std::fs::remove_file(path)?;
    }
    Ok((written, spill_count, spill_bytes))
}

/// K-way merge of spilled length-prefixed runs in key order.
fn merge_runs(runs: &[PathBuf], writer: &mut dyn Write, framed: bool) -> std::io::Result<usize> {
    /// Split one framed run line into its sort key and JSONL payload.
    fn split_framed(raw: &str) -> Option<(String, String)> {
        let (hex, rest) = raw.split_at_checked(8)?;
        let len = usize::from_str_radix(hex, 16).ok()?;
        let (key, line) = rest.split_at_checked(len)?;
        Some((key.to_string(), line.trim_end_matches('\n').to_string()))
    }

    let mut readers: Vec<(BufReader<std::fs::File>, String, String, bool)> = Vec::new();
    for path in runs {
        let mut reader = BufReader::new(std::fs::File::open(path)?);
        let mut raw = String::new();
        let (key, line, live) = if reader.read_line(&mut raw)? > 0 {
            let (key, line) = split_framed(&raw).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt spill run")
            })?;
            (key, line, true)
        } else {
            (String::new(), String::new(), false)
        };
        readers.push((reader, key, line, live));
    }
    let mut written = 0;
    loop {
        let mut best: Option<usize> = None;
        for (position, (_, key, _, live)) in readers.iter().enumerate() {
            if !live {
                continue;
            }
            if best.is_none_or(|current| *key < readers[current].1) {
                best = Some(position);
            }
        }
        let Some(position) = best else {
            break;
        };
        let (reader, key, line, live) = &mut readers[position];
        if framed {
            write!(writer, "{:08x}{}", key.len(), key)?;
        }
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        written += 1;
        let mut raw = String::new();
        if reader.read_line(&mut raw)? == 0 {
            *live = false;
        } else if let Some((key, next)) = split_framed(&raw) {
            readers[position].1 = key;
            readers[position].2 = next;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "corrupt spill run",
            ));
        }
    }
    Ok(written)
}

fn base_manifest(alias_reference: &str, alias_digest: &str, options: &EmitOptions) -> Manifest {
    Manifest {
        format_version: "station-batch-v1".to_string(),
        run_id: options.run_id.clone(),
        parser_version: PARSER_VERSION.to_string(),
        policy_version: POLICY_VERSION.to_string(),
        municipality_reference: MunicipalityReference {
            reference: alias_reference.to_string(),
            reference_hash: alias_digest.to_string(),
        },
        started_at: options.started_at.clone(),
        ended_at: options.ended_at.clone(),
        inputs: Vec::new(),
        counts: BTreeMap::new(),
        completeness: Completeness {
            eof_validated: true,
            expected_manifest: true,
        },
        outputs: Vec::new(),
    }
}

/// Emit one run combining an optional registry batch and an optional
/// PMQC batch. Quarantine rows merge into a single stream ordered by
/// `(source, locator)`. Assertion/candidate streams carry the full
/// accepted+duplicate multiset (duplicate lines byte-equal their
/// accepted twins) so the loader independently re-verifies dedup
/// against manifest accounting; accepted rows keep their parse-time
/// stable order and are re-sorted here only to prove spill equivalence.
#[allow(clippy::too_many_arguments)]
pub fn emit_run(
    registry: Option<(&SourceMeta, &RegistryBatch)>,
    pmqc: Option<(&SourceMeta, &PmqcBatch)>,
    alias_reference: &str,
    alias_digest: &str,
    options: &EmitOptions,
    scratch: &Path,
) -> std::io::Result<EmittedRun> {
    let mut manifest = base_manifest(alias_reference, alias_digest, options);

    let mut assertions: Vec<u8> = Vec::new();
    let mut candidates: Vec<u8> = Vec::new();
    let mut quarantine_pairs: Vec<(String, String)> = Vec::new();
    let mut spill_runs = 0usize;
    let mut spill_bytes = 0u64;

    if let Some((meta, batch)) = registry {
        manifest.inputs.push(meta.clone());
        manifest
            .counts
            .insert(meta.key.clone(), batch.counts.into());
        let pairs: Vec<(String, String)> = batch
            .accepted
            .iter()
            .chain(batch.duplicates.iter())
            .map(|row| {
                (
                    format!("{}\x1f{}", row.source_key, row.checksum),
                    row.to_jsonl(),
                )
            })
            .collect();
        write_sorted(
            pairs,
            &mut assertions,
            options.spill_rows,
            scratch,
            "assertions",
        )
        .map(|(_, runs, bytes)| {
            spill_runs += runs;
            spill_bytes += bytes;
        })?;
        for row in &batch.quarantine {
            quarantine_pairs.push((
                format!("{}\x1f{}", row.source, row.row_locator),
                row.to_jsonl(),
            ));
        }
    }
    if let Some((meta, batch)) = pmqc {
        manifest.inputs.push(meta.clone());
        manifest
            .counts
            .insert(meta.key.clone(), batch.counts.into());
        let pairs: Vec<(String, String)> = batch
            .candidates
            .iter()
            .chain(batch.duplicates.iter())
            .map(|row| {
                (
                    format!(
                        "{}\x1f{}\x1f{}",
                        row.source_key, row.observed_at, row.sample_ref
                    ),
                    row.to_jsonl(),
                )
            })
            .collect();
        write_sorted(
            pairs,
            &mut candidates,
            options.spill_rows,
            scratch,
            "candidates",
        )
        .map(|(_, runs, bytes)| {
            spill_runs += runs;
            spill_bytes += bytes;
        })?;
        for row in &batch.quarantine {
            quarantine_pairs.push((
                format!("{}\x1f{}", row.source, row.row_locator),
                row.to_jsonl(),
            ));
        }
    }
    let mut quarantine: Vec<u8> = Vec::new();
    write_sorted(
        quarantine_pairs,
        &mut quarantine,
        options.spill_rows,
        scratch,
        "quarantine",
    )
    .map(|(_, runs, bytes)| {
        spill_runs += runs;
        spill_bytes += bytes;
    })?;

    for (path, bytes) in [
        (ASSERTIONS_FILE, &assertions),
        (CANDIDATES_FILE, &candidates),
        (QUARANTINE_FILE, &quarantine),
    ] {
        manifest.outputs.push(OutputEntry {
            path: path.to_string(),
            sha256: sha256_hex(bytes),
            bytes: bytes.len() as u64,
            rows: bytes.iter().filter(|byte| **byte == b'\n').count(),
        });
    }
    Ok(EmittedRun {
        assertions,
        candidates,
        quarantine,
        manifest,
        spill_runs,
        spill_bytes,
    })
}

/// Publish emitted bytes to `dir` atomically: every stream lands as a
/// temp file first and renames into place only after a full flush. Any
/// failure leaves no partial final file behind.
pub fn write_outputs(dir: &Path, emitted: &EmittedRun) -> std::io::Result<()> {
    for (name, bytes) in [
        (ASSERTIONS_FILE, emitted.assertions.as_slice()),
        (CANDIDATES_FILE, emitted.candidates.as_slice()),
        (QUARANTINE_FILE, emitted.quarantine.as_slice()),
    ] {
        let tmp = dir.join(format!("{name}.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, dir.join(name))?;
    }
    let tmp = dir.join(format!("{MANIFEST_FILE}.tmp"));
    std::fs::write(&tmp, emitted.manifest_json())?;
    std::fs::rename(&tmp, dir.join(MANIFEST_FILE))?;
    Ok(())
}

/// File-backed emission: no full serialized pair list or output Vec. Parse
/// batches remain subject to Limits; emission adds <=4 MiB sort chunks and
/// <=32 merge lookahead rows, <=1024 run paths, and <=100 MiB per final stream.
/// A unique unpublished sibling directory contains all spill/output artifacts.
/// Existing output directories are never reused. Manifest is committed last.
#[allow(clippy::too_many_arguments)]
pub fn emit_to_directory(
    registry: Option<(&SourceMeta, &RegistryBatch)>,
    pmqc: Option<(&SourceMeta, &PmqcBatch)>,
    alias_reference: &str,
    alias_digest: &str,
    options: &EmitOptions,
    dir: &Path,
) -> std::io::Result<Manifest> {
    use sha2::Digest;
    if dir.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "output directory exists",
        ));
    }
    let parent = dir.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "output parent missing")
    })?;
    std::fs::create_dir_all(parent)?;
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let staging = parent.join(format!(
        ".station-prep-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    std::fs::create_dir(&staging)?;
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let cleanup = Cleanup(staging.clone());
    let mut manifest = base_manifest(alias_reference, alias_digest, options);
    if let Some((meta, batch)) = registry {
        manifest.inputs.push(meta.clone());
        manifest
            .counts
            .insert(meta.key.clone(), batch.counts.into());
    }
    if let Some((meta, batch)) = pmqc {
        manifest.inputs.push(meta.clone());
        manifest
            .counts
            .insert(meta.key.clone(), batch.counts.into());
    }
    struct MeasuredWriter {
        file: std::io::BufWriter<std::fs::File>,
        hash: sha2::Sha256,
        bytes: u64,
        rows: usize,
    }
    impl Write for MeasuredWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.bytes + bytes.len() as u64 > 100 << 20 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "output exceeds 100 MiB",
                ));
            }
            let n = self.file.write(bytes)?;
            self.hash.update(&bytes[..n]);
            self.bytes += n as u64;
            self.rows += bytes[..n].iter().filter(|b| **b == b'\n').count();
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    for name in [ASSERTIONS_FILE, CANDIDATES_FILE, QUARANTINE_FILE] {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(staging.join(name))?;
        let mut writer = MeasuredWriter {
            file: std::io::BufWriter::new(file),
            hash: sha2::Sha256::new(),
            bytes: 0,
            rows: 0,
        };
        match name {
            ASSERTIONS_FILE => {
                let pairs = registry
                    .iter()
                    .flat_map(|(_, batch)| batch.accepted.iter().chain(batch.duplicates.iter()))
                    .map(|row| {
                        (
                            format!("{}\x1f{}", row.source_key, row.checksum),
                            row.to_jsonl(),
                        )
                    });
                write_sorted(
                    pairs,
                    &mut writer,
                    options.spill_rows,
                    &staging,
                    "assertions",
                )?;
            }
            CANDIDATES_FILE => {
                let pairs = pmqc
                    .iter()
                    .flat_map(|(_, batch)| batch.candidates.iter().chain(batch.duplicates.iter()))
                    .map(|row| {
                        (
                            format!(
                                "{}\x1f{}\x1f{}",
                                row.source_key, row.observed_at, row.sample_ref
                            ),
                            row.to_jsonl(),
                        )
                    });
                write_sorted(
                    pairs,
                    &mut writer,
                    options.spill_rows,
                    &staging,
                    "candidates",
                )?;
            }
            _ => {
                let pairs = registry
                    .iter()
                    .flat_map(|(_, batch)| batch.quarantine.iter())
                    .chain(pmqc.iter().flat_map(|(_, batch)| batch.quarantine.iter()))
                    .map(|row| {
                        (
                            format!("{}\x1f{}", row.source, row.row_locator),
                            row.to_jsonl(),
                        )
                    });
                write_sorted(
                    pairs,
                    &mut writer,
                    options.spill_rows,
                    &staging,
                    "quarantine",
                )?;
            }
        }
        writer.flush()?;
        writer.file.get_ref().sync_all()?;
        let digest = writer.hash.finalize();
        let sha256 = digest.iter().map(|b| format!("{b:02x}")).collect();
        manifest.outputs.push(OutputEntry {
            path: name.to_string(),
            sha256,
            bytes: writer.bytes,
            rows: writer.rows,
        });
    }
    let manifest_path = staging.join(MANIFEST_FILE);
    std::fs::write(&manifest_path, manifest.to_json())?;
    std::fs::File::open(manifest_path)?.sync_all()?;
    std::fs::File::open(&staging)?.sync_all()?;
    if dir.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "output appeared during publication",
        ));
    }
    std::fs::rename(&staging, dir)?;
    std::fs::File::open(parent)?.sync_all()?;
    drop(cleanup);
    Ok(manifest)
}
