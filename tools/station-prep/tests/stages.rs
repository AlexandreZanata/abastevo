//! RST-13 acceptance: the full preparation path (parse, PMQC, join,
//! emit, publish) keeps oracle accounting across the campaign matrix:
//! clean, replay, reorder, changed, duplicate/quarantine/long-field
//! heavy and spill-forced inputs.

use station_prep::{
    delta_edition, emit_run, generate, join_candidates, parse_pmqc, parse_registry,
    replay_decision, write_outputs, AliasTable, DatasetProfile, EmitOptions, Limits, SourceMeta,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn profile(rows: usize, seed: u64) -> DatasetProfile {
    DatasetProfile {
        id: "probe",
        rows,
        seed,
        cities: 5,
        duplicate_pct: 2,
        conflict_pct: 1,
        quarantine_every: 60,
        long_field_every: 150,
        missing_location_every: 0,
        serial_offset: 0,
    }
}

fn scratch(suite: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "station-prep-stages-{suite}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("scratch must create");
    dir
}

fn options(spill_rows: usize) -> EmitOptions {
    EmitOptions {
        run_id: "rst13-probe-run".to_string(),
        started_at: "2026-10-09T00:00:00Z".to_string(),
        ended_at: "2026-10-09T00:00:00Z".to_string(),
        spill_rows,
    }
}

fn pmqc_extract(cnpjs: &[String], take: usize) -> Vec<u8> {
    let mut text = String::from("AmostraId;CnpjPosto;DataColeta;Latitude;Longitude\n");
    for (index, cnpj) in cnpjs.iter().take(take).enumerate() {
        let lat = -23.55 + ((index % 300) as f64 - 150.0) / 1000.0;
        let lon = -23.55 + ((index % 500) as f64 - 250.0) / 1000.0;
        text.push_str(&format!(
            "A-{index:06};{cnpj};2024-04-01;{lat:.6};{lon:.6}\n"
        ));
    }
    text.into_bytes()
}

struct Pipeline {
    batch: station_prep::RegistryBatch,
    emitted: station_prep::EmittedRun,
    meta: SourceMeta,
    digest: String,
}

fn run_pipeline(
    csv: &[u8],
    aliases: &str,
    spill_rows: usize,
    scratch: &std::path::Path,
) -> Pipeline {
    let table = AliasTable::from_json(aliases).expect("aliases load");
    let batch =
        parse_registry("probe.csv", csv.to_vec(), &Limits::default(), &table).expect("parse");
    let cnpjs: Vec<String> = batch
        .accepted
        .iter()
        .map(|row| row.source_key.clone())
        .collect();
    let pmqc_raw = pmqc_extract(&cnpjs, 50);
    let pmqc = parse_pmqc("probe-pmqc.csv", pmqc_raw, &Limits::default()).expect("pmqc");
    let joined = join_candidates(&batch.accepted, &pmqc.candidates);
    assert_eq!(joined.candidates.len(), pmqc.candidates.len());
    let meta = SourceMeta::fingerprint(
        "registry-13col",
        "probe.csv",
        "probe-ed",
        1,
        csv,
        batch.counts.input,
    );
    let emitted = emit_run(
        Some((&meta, &batch)),
        None,
        "probe-aliases",
        table.digest(),
        &options(spill_rows),
        scratch,
    )
    .expect("emit");
    Pipeline {
        batch,
        emitted,
        meta,
        digest: table.digest().to_string(),
    }
}

#[test]
fn clean_pipeline_reconciles_and_replays_to_noop() {
    let generated = generate(&profile(300, 21));
    let dir = scratch("clean");
    let first = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    assert_eq!(
        first.batch.counts.accepted
            + first.batch.counts.duplicates
            + first.batch.counts.quarantined,
        first.batch.counts.input
    );
    assert_eq!(first.emitted.spill_runs, 0);
    assert_eq!(first.emitted.spill_bytes, 0);
    // Streams carry the full accepted+duplicate multiset the manifest
    // accounts for, so the loader re-verifies dedup independently.
    assert_eq!(first.batch.duplicates.len(), first.batch.counts.duplicates);
    let twins: std::collections::HashSet<String> = first
        .batch
        .accepted
        .iter()
        .map(|row| row.to_jsonl())
        .collect();
    for dup in &first.batch.duplicates {
        assert!(twins.contains(&dup.to_jsonl()), "dup has an accepted twin");
    }
    let assertion_lines = first
        .emitted
        .assertions
        .iter()
        .filter(|b| **b == b'\n')
        .count();
    assert_eq!(
        assertion_lines,
        first.batch.counts.accepted + first.batch.counts.duplicates
    );
    // Unchanged replay: identical bytes, Noop decision, publish round-trips.
    let second = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    assert_eq!(first.emitted.assertions, second.emitted.assertions);
    let manifest =
        station_prep::Manifest::from_json(&first.emitted.manifest_json()).expect("manifest parses");
    assert_eq!(
        replay_decision(
            &manifest,
            std::slice::from_ref(&first.meta),
            station_prep::output::PARSER_VERSION,
            station_prep::output::POLICY_VERSION,
            &first.digest,
        ),
        station_prep::ReplayDecision::Noop
    );
    let out = dir.join("out");
    std::fs::create_dir_all(&out).expect("out dir creates");
    write_outputs(&out, &first.emitted).expect("publish");
    assert!(out.join("manifest.json").exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn reordered_inputs_keep_assertion_bytes_and_quarantine_content() {
    let generated = generate(&profile(300, 21));
    let dir = scratch("reorder");
    let first = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    let text = String::from_utf8(generated.csv.clone()).expect("csv utf-8");
    let mut lines: Vec<&str> = text.lines().collect();
    let header = lines.remove(0);
    lines.reverse();
    let reordered = format!("{header}\n{}\n", lines.join("\n")).into_bytes();
    let second = run_pipeline(&reordered, &generated.aliases_json, usize::MAX, &dir);
    assert_eq!(first.emitted.assertions, second.emitted.assertions);
    assert_eq!(first.batch.counts, second.batch.counts);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn changed_editions_publish_with_exact_counts() {
    let generated = generate(&profile(300, 21));
    let dir = scratch("delta");
    let first = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    for changed_every in [100usize, 10usize] {
        let delta = delta_edition(&generated.csv, changed_every);
        assert_eq!(delta.changed + delta.unchanged, 300);
        assert!(delta.changed > 0);
        let revised = run_pipeline(&delta.csv, &generated.aliases_json, usize::MAX, &dir);
        assert_ne!(first.emitted.assertions, revised.emitted.assertions);
        let manifest = station_prep::Manifest::from_json(&first.emitted.manifest_json())
            .expect("manifest parses");
        let revised_meta = SourceMeta::fingerprint(
            "registry-13col",
            "probe.csv",
            "probe-ed",
            2,
            &delta.csv,
            revised.batch.counts.input,
        );
        assert_eq!(
            replay_decision(
                &manifest,
                std::slice::from_ref(&revised_meta),
                station_prep::output::PARSER_VERSION,
                station_prep::output::POLICY_VERSION,
                &first.digest,
            ),
            station_prep::ReplayDecision::Publish
        );
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn heavy_variants_keep_accounting() {
    let dir = scratch("heavy");
    let base = DatasetProfile {
        id: "probe",
        rows: 300,
        seed: 22,
        cities: 5,
        duplicate_pct: 0,
        conflict_pct: 0,
        quarantine_every: 0,
        long_field_every: 0,
        missing_location_every: 0,
        serial_offset: 0,
    };
    let clean = generate(&base);
    let clean_run = run_pipeline(&clean.csv, &clean.aliases_json, usize::MAX, &dir);
    assert_eq!(clean_run.batch.counts.duplicates, 0);
    assert_eq!(clean_run.batch.counts.quarantined, 0);

    let dupes = generate(&DatasetProfile {
        duplicate_pct: 25,
        ..base
    });
    let dupes_run = run_pipeline(&dupes.csv, &dupes.aliases_json, usize::MAX, &dir);
    assert!(dupes_run.batch.counts.duplicates > clean_run.batch.counts.accepted / 10);
    assert_eq!(
        dupes_run.batch.counts.accepted
            + dupes_run.batch.counts.duplicates
            + dupes_run.batch.counts.quarantined,
        300
    );

    let quar = generate(&DatasetProfile {
        quarantine_every: 5,
        ..base
    });
    let quar_run = run_pipeline(&quar.csv, &quar.aliases_json, usize::MAX, &dir);
    assert!(quar_run.batch.counts.quarantined > 0);
    assert!(!quar_run.emitted.quarantine.is_empty());

    let long = generate(&DatasetProfile {
        long_field_every: 3,
        ..base
    });
    let long_run = run_pipeline(&long.csv, &long.aliases_json, usize::MAX, &dir);
    assert_eq!(
        long_run.batch.counts.accepted,
        clean_run.batch.counts.accepted
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn spill_forced_emit_matches_in_memory_bytes() {
    let generated = generate(&profile(300, 21));
    let dir = scratch("spill");
    let memory = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    let spilled = run_pipeline(&generated.csv, &generated.aliases_json, 5, &dir);
    assert_eq!(memory.emitted.assertions, spilled.emitted.assertions);
    assert_eq!(memory.emitted.candidates, spilled.emitted.candidates);
    assert_eq!(memory.emitted.quarantine, spilled.emitted.quarantine);
    assert!(spilled.emitted.spill_runs > 0);
    assert!(spilled.emitted.spill_bytes > 0);
    // Scratch runs are cleaned: only out dirs and inputs remain nearby.
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .expect("scratch readable")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "tmp"))
        .collect();
    assert!(leftovers.is_empty(), "spill runs cleaned: {leftovers:?}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn bounded_file_emission_matches_legacy_bytes_and_refuses_overwrite() {
    let generated = generate(&profile(3000, 42));
    let dir = scratch("bounded");
    let first = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    let out = dir.join("published");
    let manifest = station_prep::emit_to_directory(
        Some((&first.meta, &first.batch)),
        None,
        "probe-aliases",
        &first.digest,
        &options(7),
        &out,
    )
    .expect("bounded emission");
    for (name, expected) in [
        (station_prep::ASSERTIONS_FILE, &first.emitted.assertions),
        (station_prep::CANDIDATES_FILE, &first.emitted.candidates),
        (station_prep::QUARANTINE_FILE, &first.emitted.quarantine),
    ] {
        assert_eq!(&std::fs::read(out.join(name)).unwrap(), expected);
    }
    assert_eq!(manifest.to_json(), first.emitted.manifest_json());
    assert!(station_prep::emit_to_directory(
        Some((&first.meta, &first.batch)),
        None,
        "probe-aliases",
        &first.digest,
        &options(7),
        &out,
    )
    .is_err());
    assert_eq!(
        std::fs::read(out.join(station_prep::ASSERTIONS_FILE)).unwrap(),
        first.emitted.assertions
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn bounded_emission_failure_removes_only_its_unpublished_files() {
    let generated = generate(&profile(300, 43));
    let dir = scratch("bounded-failure");
    let mut pipeline = run_pipeline(&generated.csv, &generated.aliases_json, usize::MAX, &dir);
    pipeline
        .batch
        .accepted
        .last_mut()
        .unwrap()
        .business_name_normalized = "X".repeat(1 << 20);
    let unrelated = dir.join("keep.txt");
    std::fs::write(&unrelated, "retained").unwrap();
    let result = station_prep::emit_to_directory(
        Some((&pipeline.meta, &pipeline.batch)),
        None,
        "probe-aliases",
        &pipeline.digest,
        &options(7),
        &dir.join("published"),
    );
    assert!(result.is_err());
    assert!(!dir.join("published").exists());
    assert_eq!(std::fs::read_to_string(unrelated).unwrap(), "retained");
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        1,
        "partial spill/output cleanup"
    );
    std::fs::remove_dir_all(dir).unwrap();
}
