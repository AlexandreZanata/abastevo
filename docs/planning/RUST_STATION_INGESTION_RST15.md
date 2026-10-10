# RST-15 — City search, pagination and traffic distribution

> 2026-10-10 correction: the layer previously called API executes the Go Reader directly. It excludes HTTP, TLS, edge, decoding and network. Code now labels it Reader. The small diagnostic samples below are exploratory, not qualified tail/capacity claims; new real HTTPS evidence is separate.

Status: MEASURED — isolated disposable PostGIS only; no migration, no
product change, one candidate index measured and dropped. Date:
2026-10-09. Scope: `RUST_STATION_BENCHMARK_PLAN.md` RST-15,
dependencies RST-12/14 (measured read path: `Reader.Search/Nearby/
Detail` over `directory_stations`). Language: English document; user
communication in Portuguese.

B-BR/BUC: B-BR-RST-P01 (raw and API layers return identical row sets
on every workload), B-BR-RST-P05 (dense/sparse/empty/point strata
plus a stated-weight mix reported separately, never pooled blindly),
B-BR-RST-P06 (per-workload raw samples pooled for the weighted
total; timeouts/errors/drops counted, never averaged away). Serves
BUC-RST-P01 (per-stage read costs) and BUC-RST-P05 (regression
baselines with explicit scale labels).

## 1. What was added

- `read_traffic_bench_test.go` (integration): nine workloads at
  raw-sqlc and Reader layers separately (exact dense/sparse/empty
  city, common/rare text, UF-only scan, detail hit, dense/sparse
  nearby); keyset walk asserting ordered gapless duplicate-free
  pages at 5/20/50 rows with middle/last cursor re-entry;
  identical-coordinate Nearby tie stations appearing exactly once;
  negative filters and unknown-station detail asserting errors
  (never latency); closed 1/8/32 clients plus a 20 rps open
  arrival stream with counted drops; pinned cold/warm/restarted/
  rotating-unseen cache states; diagnostic EXPLAIN for sparse vs
  dense predicates under default/generic/custom plan modes with
  estimate-vs-actual capture; one-candidate-at-a-time index trials
  with fresh ANALYZE, build/storage costs and the 15%/10%
  materiality verdict.
- `seedCensus` generalized from `*testing.B` to `testing.TB`
  (3-line refactor, no behavior change) for reuse.
- No product change: candidate indexes are built, measured and
  dropped inside the disposable database.

## 2. Measurements (100k census, fresh disposable PostGIS per bench)

API layer, single warmed client (raw → api p95 ms): city_dense
9.30 → 13.37, city_sparse 0.54 → 2.10, city_empty 0.08 → 0.20,
text_common 127 → 170, text_rare 150 → 49.5, uf_only 66.2 → 54.0,
detail_hit 0.14 → 0.20, nearby_dense 4.75 → 9.31, nearby_sparse
0.18 → 0.20. Hydration costs concentrate on wide pages; sparse
and empty lookups stay sub-3 ms. Table 33.8 MiB, indexes 18.7 MiB.

Full dense-city walks (correct, gapless): 5-row pages ~23.4 s /
11.6 ms p95 from first/middle cursors and ~5 ms from the last;
20-row pages ~5.9 s / 3.6 s / 4.1 ms; 50-row pages ~2.7 s /
1.2 s / 6.9 ms. Cost scales with page count, not page size.

Clients on dense workloads: closed-1 city p95 12.0 ms, closed-8
17.0 ms, closed-32 48.6 ms (nearby 5.5 → 9.5 → 35.2 ms);
open 20 rps mixed: 100 offered, 100 achieved, 0 dropped, p95
9.6 ms. Cache states on dense city: cold 12.7, warm 10.2,
restarted 11.3 ms p95 (indistinguishable on this host; OS/DB
caches recorded unknown, never claimed cold); rotating unseen
1.9 ms (sparse/empty mix).

Plans (diagnostic): sparse city 129 rows vs dense 14,089;
default exec 0.34 vs 9.17 ms; forced generic 16.0/17.8 ms
(worse on these single samples — observed, no decision);
forced custom matches default. Limit-node estimate 1 vs
actual 21 is the LIMIT cap, not a cardinality miss.

Index trial: pg_trgm GIN on `display_name` builds in ~216 ms
(+4.0 MB) and cuts selective text p95 43 → 6.4 ms (+85.1%)
with the full-match term unchanged (+3.1%, within noise):
verdict accept-pending-migration-task — the migration itself is
a separate implementation task, nothing merged here.

Weighted mix (30/15/10/10/5/5/5/15/5 over the nine workloads,
n=200 pooled raw samples): p50 7.9, p95 81.9, p99 91.0 ms,
dominated by the text_common stratum, which is reported
alongside — the mix summarizes, it does not hide the worst
stratum. Coverage denominators: per-workload n stated; empty
cities contribute explicit zero-row samples.

## 3. Scale spot and limits

1M spot, closed-8 dense city: p50 125.7 / p95 201.5 ms
(n=80, 0 errors; table 332 MiB, indexes 182 MiB). This breaches
the provisional RST-10 100 ms hypothesis: the hypothesis needs
revision for hot dense cities at 1M under concurrent load
(RST-18 capacity work), not silent averaging. Single-client 1M
numbers remain with RST-07.

NOT_APPLICABLE with reason: same-sort-value ties on Search
(keyset orders by unique `id::text`); deep OFFSET (no OFFSET
SQL exists and none is introduced); concurrent live-index
creation (no shared traffic in lab); `pg_stat_statements`
per-statement tails (extension absent).

## 4. Regression checks and next task

- Correctness green: pagination stable at 5/20/50 with middle/
  last re-entry, Nearby ties exact-once with full-walk coverage,
  negatives error properly, raw and API layers agree modulo
  limit+1, existing read package tests pass.
- Reproduce: `go test -tags=integration -run NONE -bench
  'BenchmarkTraffic(Layers|Pages|Clients|CacheStates|Plans|
  Index|Weighted|ScaleSpot)' -benchtime 1x
  ./internal/modules/directory/adapters/read/` (fresh DB per
  bench, dropped after; ~15 min total, pages dominate).
- Next (smallest, separately authorized): RST-16 spatial reads
  and precise-location eligibility cost on real PostGIS.
