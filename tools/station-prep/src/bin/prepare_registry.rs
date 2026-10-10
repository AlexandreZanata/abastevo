//! Operational offline registry preparation. No synthetic PMQC generation and
//! no network/database access. Fetch official bytes and frozen IBGE aliases
//! separately, then record their provenance through this command.
use station_prep::{
    emit_to_directory, parse_registry, AliasTable, EmitOptions, Limits, RunState, SourceMeta,
};
use std::io::Read;

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 8 {
        return Err("usage: prepare_registry CSV ALIASES OUTPUT_DIR SOURCE_REFERENCE EDITION RUN_UUID UTC_TIMESTAMP".into());
    }
    let mut raw = Vec::new();
    std::fs::File::open(&args[1])?
        .take((100 << 20) + 1)
        .read_to_end(&mut raw)?;
    if raw.len() > 100 << 20 {
        return Err("registry exceeds 100 MiB".into());
    }
    let mut aliases = String::new();
    std::fs::File::open(&args[2])?
        .take((4 << 20) + 1)
        .read_to_string(&mut aliases)?;
    if aliases.len() > 4 << 20 {
        return Err("alias reference exceeds 4 MiB".into());
    }
    let table = AliasTable::from_json(&aliases).map_err(|e| e.0)?;
    let meta_raw = raw.clone();
    let source_name = std::path::Path::new(&args[1])
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("source file name must be UTF-8")?;
    let batch = parse_registry(source_name, raw, &Limits::default(), &table)
        .map_err(|e| format!("registry parse refused: {e:?}"))?;
    if batch.state != RunState::Complete {
        return Err("quarantined source/header must not publish".into());
    }
    let meta = SourceMeta::fingerprint(
        "registry-13col",
        &args[4],
        &args[5],
        1,
        &meta_raw,
        batch.counts.input,
    );
    drop(meta_raw);
    let options = EmitOptions {
        run_id: args[6].clone(),
        started_at: args[7].clone(),
        ended_at: args[7].clone(),
        spill_rows: 4096,
    };
    let manifest = emit_to_directory(
        Some((&meta, &batch)),
        None,
        "ibge-localidades-frozen",
        table.digest(),
        &options,
        std::path::Path::new(&args[3]),
    )?;
    println!(
        "input={} accepted={} duplicates={} quarantined={} assertion_bytes={} emission=file-backed",
        batch.counts.input,
        batch.counts.accepted,
        batch.counts.duplicates,
        batch.counts.quarantined,
        manifest.outputs[0].bytes
    );
    Ok(())
}
fn main() {
    if let Err(err) = run() {
        eprintln!("prepare_registry: {err}");
        std::process::exit(1);
    }
}
