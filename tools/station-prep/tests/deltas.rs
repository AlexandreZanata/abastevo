//! RST-03 acceptance: determinism, deltas and replay over the RST-01
//! fixtures. Every expectation mirrors the delta contract in
//! `docs/planning/RUST_STATION_INGESTION_RST02.md` section 5.

use station_prep::{
    emit_run, parse_pmqc, parse_registry, replay_decision, write_outputs, AliasTable, EmitOptions,
    EmittedRun, Limits, Manifest, ReplayDecision, SourceMeta,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const REGISTRY: &str = "registry-13col-sample.csv";
const PMQC: &str = "pmqc-sample.csv";

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

fn scratch(suite: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "station-prep-{suite}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("scratch must create");
    dir
}

fn options(spill_rows: usize) -> EmitOptions {
    EmitOptions {
        run_id: "a3f1c2b4-5d6e-4f7a-8b9c-0d1e2f3a4b5c".to_string(),
        started_at: "2024-05-02T10:00:00Z".to_string(),
        ended_at: "2024-05-02T10:00:07Z".to_string(),
        spill_rows,
    }
}

fn emit(
    registry_raw: &[u8],
    pmqc_raw: &[u8],
    registry_seq: u64,
    pmqc_seq: u64,
    spill_rows: usize,
    scratch: &Path,
) -> (EmittedRun, Vec<SourceMeta>, String) {
    let table = aliases();
    let registry = parse_registry(REGISTRY, registry_raw.to_vec(), &Limits::default(), &table)
        .expect("registry parses");
    let pmqc = parse_pmqc(PMQC, pmqc_raw.to_vec(), &Limits::default()).expect("pmqc parses");
    let metas = vec![
        SourceMeta::fingerprint(
            "registry-13col",
            REGISTRY,
            "synthetic-2024-03",
            registry_seq,
            registry_raw,
            registry.counts.input,
        ),
        SourceMeta::fingerprint(
            "pmqc",
            PMQC,
            "synthetic-2024-04",
            pmqc_seq,
            pmqc_raw,
            pmqc.counts.input,
        ),
    ];
    let emitted = emit_run(
        Some((&metas[0], &registry)),
        Some((&metas[1], &pmqc)),
        "ibge-localidades-v1",
        table.digest(),
        &options(spill_rows),
        scratch,
    )
    .expect("emit succeeds");
    (emitted, metas, table.digest().to_string())
}

#[test]
fn golden_row_checksum_pins_canonical_form() {
    let batch = parse_registry(REGISTRY, bytes(REGISTRY), &Limits::default(), &aliases())
        .expect("sample parses");
    let alfa = batch
        .accepted
        .iter()
        .find(|row| row.simp_ref == "SIMP-0001")
        .expect("SIMP-0001 accepted");
    // Frozen vector: changing the canonical form changes every checksum.
    assert_eq!(
        alfa.checksum,
        "4a6b42a25a72710f6a4838cc168b150e318d14f04bc00572099dcce230edaacc"
    );
}

#[test]
fn reordered_sources_yield_identical_run_bytes() {
    let raw = text(REGISTRY);
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
    rows.reverse();
    let reordered = format!("{header}\n{}\n", rows.join("\n")).into_bytes();
    let scratch = scratch("reorder");
    let (first, _, _) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    let (second, _, _) = emit(&reordered, &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    // Same logical content yields identical streams and output hashes.
    assert_eq!(first.assertions, second.assertions);
    assert_eq!(first.candidates, second.candidates);
    // Quarantine locators are positional by design (`file:logical-row`),
    // so reordered inputs compare by quarantined content, not bytes.
    let content = |run: &EmittedRun| {
        let parsed: Vec<serde_json::Value> = String::from_utf8(run.quarantine.clone())
            .expect("quarantine is UTF-8")
            .lines()
            .map(|line| serde_json::from_str(line).expect("quarantine line is JSON"))
            .collect();
        let mut content: Vec<(String, String, Option<String>)> = parsed
            .iter()
            .map(|row| {
                (
                    row["reason"].as_str().expect("reason").to_string(),
                    row["detail"].as_str().expect("detail").to_string(),
                    row["source_key"].as_str().map(str::to_string),
                )
            })
            .collect();
        content.sort();
        content
    };
    assert_eq!(content(&first), content(&second));
    // Output hashes match for order-independent streams; the quarantine
    // hash binds positional locators and therefore differs by design.
    let output = |run: &EmittedRun, path: &str| {
        run.manifest
            .outputs
            .iter()
            .find(|entry| entry.path == path)
            .expect("output entry")
            .sha256
            .clone()
    };
    assert_eq!(
        output(&first, "assertions.jsonl"),
        output(&second, "assertions.jsonl")
    );
    assert_eq!(
        output(&first, "candidates.jsonl"),
        output(&second, "candidates.jsonl")
    );
    // The manifest still binds the exact input bytes: reordered sources
    // fingerprint differently, so identical semantics stay distinguishable
    // from identical bytes.
    assert_ne!(
        first.manifest.inputs[0].sha256,
        second.manifest.inputs[0].sha256
    );
    assert_eq!(first.manifest.counts, second.manifest.counts);
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn corrected_bytes_publish_dated_corrections_retaining_identity() {
    let scratch = scratch("corrected");
    let corrected = text(REGISTRY).replace("AV PAULISTA 1000", "AV PAULISTA 1001");
    assert!(corrected.contains("AV PAULISTA 1001"));
    let (original, _, digest) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    let (revised, _, _) = emit(
        corrected.as_bytes(),
        &bytes(PMQC),
        3,
        4,
        usize::MAX,
        &scratch,
    );
    assert_ne!(original.assertions, revised.assertions);
    // Same establishment, new checksum: identity retained, evidence dated.
    let original_batch = parse_registry(REGISTRY, bytes(REGISTRY), &Limits::default(), &aliases())
        .expect("original parses");
    let original_alfa = original_batch
        .accepted
        .iter()
        .find(|row| row.simp_ref == "SIMP-0001")
        .expect("original SIMP-0001");
    let revised_batch = parse_registry(
        REGISTRY,
        corrected.into_bytes(),
        &Limits::default(),
        &aliases(),
    )
    .expect("corrected parses");
    assert_eq!(revised_batch.counts.input, 12);
    let alfa: Vec<_> = revised_batch
        .accepted
        .iter()
        .filter(|row| row.source_key == "48151623000191" && row.supersedes.is_none())
        .collect();
    assert_eq!(alfa.len(), 1);
    assert_eq!(alfa[0].address_raw, "AV PAULISTA 1001");
    assert_ne!(alfa[0].checksum, original_alfa.checksum);
    assert_eq!(
        replay_decision(
            &original.manifest,
            &revised.manifest.inputs,
            station_prep::output::PARSER_VERSION,
            station_prep::output::POLICY_VERSION,
            &digest,
        ),
        ReplayDecision::Publish
    );
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn unchanged_replay_is_noop() {
    let scratch = scratch("noop");
    let (first, metas, digest) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    assert_eq!(
        replay_decision(
            &first.manifest,
            &metas,
            station_prep::output::PARSER_VERSION,
            station_prep::output::POLICY_VERSION,
            &digest,
        ),
        ReplayDecision::Noop
    );
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn older_evidence_never_overwrites_fresher_facts() {
    let scratch = scratch("stale");
    let corrected = text(REGISTRY).replace("AV PAULISTA 1000", "AV PAULISTA 1001");
    // Accepted manifest at edition 5 with corrected bytes.
    let (accepted, _, digest) = emit(
        corrected.as_bytes(),
        &bytes(PMQC),
        5,
        5,
        usize::MAX,
        &scratch,
    );
    // The same corrected bytes relabeled at edition 4 arrive late.
    let table = aliases();
    let registry = parse_registry(REGISTRY, corrected.into_bytes(), &Limits::default(), &table)
        .expect("corrected parses");
    let pmqc = parse_pmqc(PMQC, bytes(PMQC), &Limits::default()).expect("pmqc parses");
    let late = vec![
        SourceMeta::fingerprint(
            "registry-13col",
            REGISTRY,
            "synthetic-2024-03",
            4,
            &bytes(REGISTRY),
            registry.counts.input,
        ),
        SourceMeta::fingerprint(
            "pmqc",
            PMQC,
            "synthetic-2024-04",
            4,
            &bytes(PMQC),
            pmqc.counts.input,
        ),
    ];
    // Note: late registry bytes are the *original* snapshot at an older
    // edition while fresher corrected evidence is accepted: refuse.
    assert_eq!(
        replay_decision(
            &accepted.manifest,
            &late,
            station_prep::output::PARSER_VERSION,
            station_prep::output::POLICY_VERSION,
            &digest,
        ),
        ReplayDecision::Stale
    );
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn spill_matches_in_memory_bytes_and_cleans_runs() {
    let scratch = scratch("spill");
    let (memory, _, _) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    let (spilled, _, _) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, 2, &scratch);
    assert_eq!(memory.assertions, spilled.assertions);
    assert_eq!(memory.candidates, spilled.candidates);
    assert_eq!(memory.quarantine, spilled.quarantine);
    assert_eq!(memory.manifest_json(), spilled.manifest_json());
    let leftovers: Vec<_> = std::fs::read_dir(&scratch)
        .expect("scratch readable")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "tmp"))
        .collect();
    assert!(leftovers.is_empty(), "spill runs must clean up");
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn interrupted_write_leaves_no_partial_files() {
    let scratch = scratch("interrupted");
    let (emitted, _, _) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    // A regular file is not a directory: every stream write must fail.
    let blocker = scratch.join("blocker");
    std::fs::write(&blocker, b"x").expect("blocker writes");
    assert!(write_outputs(&blocker, &emitted).is_err());
    for name in [
        "assertions.jsonl",
        "candidates.jsonl",
        "quarantine.jsonl",
        "manifest.json",
    ] {
        assert!(
            !scratch.join(name).exists(),
            "{name} must not exist after failure"
        );
    }
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn manifest_counts_match_written_files() {
    let scratch = scratch("roundtrip");
    let (emitted, _, _) = emit(&bytes(REGISTRY), &bytes(PMQC), 3, 4, usize::MAX, &scratch);
    let published = scratch.join("published");
    std::fs::create_dir_all(&published).expect("published dir");
    write_outputs(&published, &emitted).expect("publish succeeds");
    let manifest_raw = std::fs::read_to_string(published.join("manifest.json")).expect("manifest");
    let manifest = Manifest::from_json(&manifest_raw).expect("manifest reloads");
    assert_eq!(manifest.run_id, emitted.manifest.run_id);
    for output in &manifest.outputs {
        let body = std::fs::read(published.join(&output.path)).expect("stream file");
        assert_eq!(body.len() as u64, output.bytes, "{}", output.path);
        assert_eq!(
            body.iter().filter(|byte| **byte == b'\n').count(),
            output.rows,
            "{}",
            output.path
        );
    }
    let counts = &manifest.counts;
    assert_eq!(counts["registry-13col"].input, 12);
    assert_eq!(counts["registry-13col"].accepted, 8);
    assert_eq!(counts["pmqc"].input, 11);
    assert_eq!(counts["pmqc"].accepted, 5);
    std::fs::remove_dir_all(&scratch).ok();
}

#[test]
fn alias_digest_is_stable_hex() {
    let first = aliases().digest().to_string();
    let second = aliases().digest().to_string();
    assert_eq!(first.len(), 64);
    assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(first, second);
}
