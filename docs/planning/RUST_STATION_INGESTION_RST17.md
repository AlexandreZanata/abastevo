# RST-17 — Physical design and partitioning comparison

> 2026-10-10 correction: UF-LIST and BRIN remain exploratory hypotheses. Three short lab trials on synthetic assertions do not accept deployment or an operational partition strategy. Keep the current layout; require repeated representative mixed-load/cost/maintenance measurements before a separate adoption migration.

Status: DECIDED — lab-only DDL on disposable PostGIS; no migration,
no product change, no production migration. Date: 2026-10-09. Scope:
`RUST_STATION_BENCHMARK_PLAN.md` RST-17 over the RST-15/16
baselines. Language: English document; user communication in
Portuguese.

B-BR/BUC: B-BR-RST-P01 (B/C return identical sets to A on every
shape), B-BR-RST-P04 (fresh disposable database per trial, B/C
dropped with it), B-BR-RST-P06 (per-workload deltas plus totals;
single-trial wins never decide). Serves BUC-RST-P05 (repeatable
physical-design baselines with scale labels).

## 1. Method

- A: existing unpartitioned indexed catalog. B: rebuildable UF
  LIST projection. C: rebuildable municipality-hash projection,
  16 partitions predeclared (8/32 unexplored, stated). B/C carry
  canonical read columns and the same three read indexes
  (GiST point, city covering, municipality btree) as partitioned
  indexes; no PK/uniqueness (canonical owns identity), CNPJ
  resolution stays canonical by construction, so the join cost is
  identical for all designs and not re-measured.
- Comparability trap found and fixed mid-task: the state
  redistribution UPDATEs bloated A while B/C built clean. Every
  trial now VACUUMs/ANALYZEs A after redistribution, so the
  comparison isolates partition effects, not bloat.
- Decision spec `read-projection-v1`: ≥15% total-workload p95
  benefit with the same sign in 3/3 trials, no repeatable >10%
  single-workload regression, sane resources. Buffers compare
  top-level shared hit/read once, never summed children.

## 2. Measurements (100k census, 4 states, fresh DB per trial)

Totals p95 sums, 60 timed iterations each (3 leveled trials):
A 31.6/30.4/36.4 ms vs B 27.3/24.9/28.2 (−14/−18/−23%) vs C
30.4/25.5/48.5 (−4/−16/+33%). Per workload p95 deltas vs A:
single-city B −2/+14/+42, C +17/+25/+28; cross-partition
nearby B −3/+6/+22, C −6/+10/+7; UF-only scan B +19/+22/+21,
C +4/+16/−53 (one C blowup: 39.5 ms).

Build: B ~1.7 s + 51 MB WAL, C ~3.7 s + 51 MB WAL, one-time;
second rebuilds B 1.9 s / C 2.2 s; VACUUM B 0.6 s / C 1.3 s;
sizes A 37/21 vs B 27/15 vs C 29/16 MiB (A carries the PK the
projections shed by design). Lifecycle: detach/drop/add
1/1/2 ms. Generic prepared single-city means: A 24.8, B 28.0,
C 25.0 ms (single trial, observed only). Cold first-touch
single-city: A 15.8, B 9.4 ms. Whole-plan buffers single-city
hit A148/B1549/C465, nearby A1516/B1222/C1338 (buffer counts
and latency are separate metrics; pruning metadata costs
buffers while saving time).

BRIN on append-only location history (50k revisions, real
30-day retention query): build 7 ms, 24 KB, p95 7.29 →
1.91 ms (+73.8%).

## 3. Decision (read-projection-v1)

- B: total benefit −14/−18/−23%, same sign 3/3 (≥15% in 2/3);
  UF-only scan repeatably +19–22%; no repeatable >10%
  regression (single-trial −2/−3% excursions are noise).
  ACCEPTED AS DIRECTION.
- C (hash-16): totals flip sign including +33%; single-city
  benefit does not survive the all-city blowup. REJECTED.
- BRIN on history: +73.8% at 7 ms/24 KB.
  ACCEPTED AS DIRECTION.
- Adoption is a separate migration task in both cases (1M
  confirmation, lifecycle runbook, canonical-join costing):
  A stays production, no automatic migration. 8/32 partition
  counts, per-city tables and weakened uniqueness were never
  on the table.
- Reproduce: `go test -tags=integration -run NONE -bench
  'BenchmarkPartition(Matrix|Cold|Buffers|BRIN)' -benchtime 1x
  -count=3 ./internal/modules/directory/adapters/read/`
  (correctness via `-run 'TestPartitionOracleAgrees|
  TestProjectionMirrors'`).
- Next (smallest, separately authorized): RST-18 concurrent
  ingestion under load with saturation and failure/recovery.
