# RST-13 — Rust preparation, spill and resource scaling

> 2026-10-10 correction: historical `emit_run` results include full serialized buffers. Operational bounded file emission is now separate and tested; national VPS preparation evidence belongs to [hardening](../backend/RST_VPS_HARDENING.md). Historical parse-only/emit-only rates cannot be compared directly with the new full file-backed CLI.

Status: MEASURED — offline preparation only; no database, no network,
no new dependency, no parallelism added. Date: 2026-10-09. Scope:
`RUST_STATION_BENCHMARK_PLAN.md` RST-13, dependencies RST-12 and
verified RST-03/04. Language: English document; user communication in
Portuguese.

B-BR/BUC: B-BR-RST-P01 (identical semantics checked by oracle hashes
on every case), B-BR-RST-P02 (pinned seeds/inputs, every
excluded/failed sample counted), B-BR-RST-P03 (accounting reconciles
before any speed is reported). Serves BUC-RST-P01 (per-stage import
cost) and BUC-RST-P05 (repeatable numbers for regression watch).

## 1. What was added

- `EmittedRun.spill_runs`/`spill_bytes`: additive temporary-disk
  accounting (run files + framed bytes, zero on the in-memory path).
  No behavior change: all prior byte-equivalence tests pass
  unmodified.
- `src/bin/bench_stages.rs`: complete pipeline with per-stage wall
  timing — read, parse (normalize/municipality/dedup fused in the
  streaming reader, timed as one stage), PMQC parse (deterministic
  synthetic extract anchored at accepted CNPJs), join triage, emit
  (serialize/sort/spill), separately-timed hash (same SHA256 work
  the manifest performs), publish. Prints counts, rows/s, MiB/s,
  output bytes, stream hash and spill stats. Memory/CPU come from
  external `/usr/bin/time -v`; compile/setup excluded (incremental
  release rebuild ~2 s, default profile, rustc 1.99.0).
- `tests/stages.rs` (5 cases): clean reconcile + replay Noop +
  publish round-trip; reorder keeps assertion bytes and counts;
  1%/10% deltas publish with exact changed counts; heavy variants
  keep `accepted+duplicates+quarantined=input`; spill-forced bytes
  equal in-memory bytes with runs cleaned.
- Lab: i7-13620H (16 CPUs), 31 GiB RAM, release binary, inputs on
  local disk (reads served warm from page cache).

## 2. Measurements (release, single run each, scratch only)

| Case | Input | Parse (rows/s) | Emit (rows/s) | Total | Peak RSS | CPU u+s |
|---|---|---|---|---|---|---|
| A representative 20k | 19552/430/18 | 53.8 ms (372k) | 45.2 ms (442k) | 134.5 ms | 85 MiB | 0.14 s |
| B stress-100k | 98004/1996/0 | 305 ms (327k) | 223 ms (448k) | 677 ms | 389 MiB | 0.72 s |
| C stress-1M | 979968/20032/0 | 3.81 s (262k) | 2.17 s (461k) | 8.30 s | 3.48 GiB | 8.66 s |
| D 1M spill 20k rows | same as C | 3.88 s (257k) | 3.89 s (257k) | 10.19 s | 3.50 GiB | 10.62 s |
| E 100k reordered | 98004/1996/0 | 330 ms | 231 ms | 759 ms | 388 MiB | — |
| F1 100k 1% changed | 98042/1958/0 | 324 ms | 231 ms | 723 ms | 390 MiB | — |
| F2 100k 10% changed | 98352/1648/0 | — | — | — | 392 MiB | — |
| G1 25k dup-heavy | 19552/5430/18 | 62 ms (401k) | 47.8 ms | 157 ms | 86 MiB | 0.12 s |
| G2 20k quarantine-heavy | 15719/263/4018 | 49 ms (405k) | 41.5 ms | 122 ms | 73 MiB | — |
| G3 20k long-field | 19736/246/18 | 62.5 ms (320k) | 59 ms | 160 ms | 100 MiB | — |
| H 100k spill 5k rows | same as B | 317 ms | 413 ms | 873 ms | — | — |

Join/PMQC (5000 synthetic candidates): 6–8 ms at 20k, ~21 ms at
100k, ~210–236 ms at 1M (registry hash-map build dominates).
Read MiB/s 1.3–3.0 GB/s reflects warm page cache, not device speed.

Correctness held on every case: reorder reproduces the clean
100k stream hash `896fb65c…` exactly; spill-forced runs reproduce
their in-memory hashes (`07e7827d…` at 1M) with 49 runs / 955 MB
(D) and 20 runs / 95 MB (H) of temporary disk. Changed rows
publish with different hashes; checksums shift duplicate↔accepted
counts (a changed exact-duplicate becomes distinct) while
`input` stays reconciled — checksums dedup, so this is correct,
not loss. Unchanged replay is byte-identical with a Noop decision
(tested, not timed as speedup).

## 3. Scaling and limits

- Parse rows/s 372k → 327k → 262k (mild per-row growth from map
  and allocator pressure); emit rows/s flat at ~440–460k
  in-memory, falling to ~250k spill-forced (+79–85% emit cost).
- Peak RSS ~3.6 KiB/row (85 MiB → 389 MiB → 3.48 GiB), linear in
  accepted rows: the current spill bounds sort-run size but the
  pair buffer and JSONL streams stay resident, so spill-forced
  RSS barely moves (3.48 → 3.50 GiB). True streaming emission
  remains owed (RST-07); the 256 MiB parser budget still needs it.
- No worker pool exists (single-threaded, 99–100% of one CPU);
  no thread pool was added — there is nothing to scale here yet.
  One setting varied at a time (spill budget; raised row/byte
  caps only for the 1M input). No Go comparison is claimed: the
  Go path is not an equivalent transform of join/emit/manifest.
- Reproduce: generate inputs with `bench_datasets`
  (representative/stress-100k/stress-1M, seed 7) to scratch, then
  `bench_stages <csv> <aliases> [--spill-rows N] [--max-rows N
  --max-bytes-mb N]` under `/usr/bin/time -v`.

## 4. Next task

Next (smallest, separately authorized): RST-14 database
construction and incremental load baseline (owned Go loader on an
isolated real PostGIS).
