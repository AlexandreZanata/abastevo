# RST-20 — Soak, maintenance and cost efficiency (30-minute pilot)

> 2026-10-10 correction: historical table size included indexes and index cost was counted again. Table/index cost slopes must be remeasured. Collectors now use pg_table_size and PG18 num_done and propagate errors. Import-blocked sampling, short duration and missing 24/48h maintenance evidence keep RST-20 INCOMPLETE; the old pilot is historical diagnostic evidence only.

Status: PILOT-MEASURED — isolated disposable PostGIS only; no
migration, no product change, no new dependency. Date:
2026-10-10. Scope: `RUST_STATION_BENCHMARK_PLAN.md` RST-20 pilot
only; the 24/48h campaigns stay separately scheduled (OWED) and
this pilot is never labelled soak success. Language: English
document; user communication in Portuguese.

B-BR/BUC: B-BR-RST-P02 (pinned 30 min, 20 rps, 2 min import
cadence, hot-city weights, sampler cadence), B-BR-RST-P04
(fresh disposable database, dropped after; durability never
weakened), B-BR-RST-P06 (per-window tails, slopes by least
squares, no extrapolation). Serves BUC-RST-P01 (sustained
import costs) and BUC-RST-P05 (trend baselines).

## 1. What was added

- `read_soak_bench_test.go` (integration): 20 rps reads with
  hot-city rotation (50% hot dense, 30% rotating sparse sweep,
  20% nearby), one disjoint 20k-row edition every 2 minutes
  (atomic claim, oracle counts asserted per import), VACUUM
  ANALYZE at t=10/20, simulated retention (drop 3 oldest
  editions) at t=15/25, 30 s sampler (read window p95/errors,
  import lag, pool, WAL, sizes, dead tuples, backlog,
  checkpoints), slope + headroom + budget verdict at end.
  `RST20_DURATION_MIN` overrides duration for smoke checks
  and separately scheduled campaigns only.
- 16 disjoint 20k editions to scratch (offsets 1M–2.5M,
  zero cross-edition overlap); 9 consumed, 7 spare.

## 2. Pilot measurements (30 min + seed, 100k census, pool 10)

33 samples, 9 imports error-free, 6 editions retained,
maintenance 1.5 s total, worst window p95 71.0 ms, 0 read
errors, backlog slope 0.000 (bounded by worker count, never
growing), pool headroom min 0.90, budget margin min 29.0 ms:
verdict within budgets. End state: WAL 292 MB, tables
140.6 MB, indexes 61.6 MB, dead tuples 29,552, checkpoints
unverified (counter reads ignored errors — instrumentation
gap, not a zero claim).

Slopes per minute: WAL +9.28 MiB, tables +3.75 MiB (net of
retention deleting ~147k rows — growth dominates), dead
tuples +944, backlog +0.000. Retention by row-DELETE cost
1,086 s total for ~150k rows with no observed auto-vacuum
relief: the strategy is expensive and dead-tuple heavy, owed
a dedicated task (batched deletes or partition drops).
Importer cadence slipped (9 vs 15 planned: blocking imports
stretch the 2 min cadence) and sampler points landing inside
imports are skipped — both reported, not smoothed.

Costs (explicit denominators, shared noisy host): WAL per
staged row ~1.3 KB (292 MB over seed + 9×24,552 staged +
maintenance + retention — traceable total, not purified);
import wall ~0.5 s/1k rows single-threaded (RST-14 20k
baseline, same shape); prep CPU 8.7 s/1M rows parse plus
2.2 s/1M emit (RST-13, release, i7-13620H). No currency math:
no provider prices supplied, and free shared CPU is never
costed. Workstation-vs-constrained comparison reuses the
RST-18 VPS staging envelope (20 rps on the 1-CPU pod);
re-running the pilot there needs a scheduled window, owed
with the 24/48h campaigns.

## 3. Limits and next task

- 24/48h campaigns: OWED (provisioned isolated resources +
  CI_PLAN scheduling). Checkpoint/autovacuum counters need
  verified instrumentation before the long campaigns.
- Reproduce: generate 20k editions to /tmp/rst20, then
  `go test -tags=integration -run NONE -bench
  BenchmarkSoakPilot -benchtime 1x` (~35 min).
- Next (separately authorized): RST-21 decision, report and
  regression tiers over the complete matrix; environment prep
  (bundle index + collection checker) ships with this task.
