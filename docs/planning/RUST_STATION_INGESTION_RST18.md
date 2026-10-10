# RST-18 — Mixed reads, ingestion contention and sustainable capacity

> 2026-10-10 correction: concurrent importer throughput used summed service durations rather than elapsed wall time, and checkpoint collection used incompatible PG18 columns while ignoring errors. Those metrics are invalid for comparison. Reader-direct read envelopes are not HTTP or user-capacity claims. Corrected collectors and guarded HTTPS evidence are tracked in [hardening](../backend/RST_VPS_HARDENING.md).

Status: MEASURED — isolated disposable PostGIS only; one additive
loader constructor, no migration, no product change. Date:
2026-10-09. Scope: `RUST_STATION_BENCHMARK_PLAN.md` RST-18 over the
RST-17 catalog with the verified RST-08/09 read/staging paths
(`Reader.Search/Nearby`, owned `LoadBatch` staging that never
publishes). Language: English document; user communication in
Portuguese.

B-BR/BUC: B-BR-RST-P01 (every staged import agrees with its edition
manifest; identical dataset/mix/pool in every cell), B-BR-RST-P04
(fresh disposable database per cell, dropped after), B-BR-RST-P06
(per-workload tails, offered/achieved/dropped, pool/lock/WAL
signals; no single unqualified QPS). Serves BUC-RST-P01
(construction under read pressure) and BUC-RST-P05 (capacity
envelope for regression watch).

## 1. What was added

- `serial_offset` on the dataset generator plus
  `bench_datasets --serial-offset`: disjoint establishment
  identity sets across editions (pinned by a disjointness test;
  only the quarantined invalid-CNPJ marker repeats, which never
  stages). Ten 20k-row editions (~24.5k stageable rows each)
  generated to scratch through the real prep chain with distinct
  run UUIDs; cross-edition assertion overlap verified zero.
- `read_capacity_bench_test.go` (integration): open-arrival
  reads (fixed city_dense/city_sparse/nearby_dense mix with
  per-workload tails) concurrent with closed-loop importer
  workers staging one edition each (atomic claim, oracle counts
  asserted per import). Per-cell signals: pool acquire wait
  deltas, waiting-backend sampler with dominant wait event,
  non-terminal run backlog trend, WAL/sizes/dead-tuples/
  checkpoints, import throughput and max completion lag.
  Pool ceiling 10 in every cell, never tuned per cell.
- `registry.NewPGStore` additive constructor for cross-package
  staging ownership (no query or migration change).
- Budgets (lab bounds, not SLA): read p95 ≤ 100 ms (RST-10
  provisional), read errors/drops zero, imports error-free,
  backlog stable, mean pool acquire wait ≤ 50 ms. Review
  backlog is NOT_APPLICABLE (no async reviewer; freshness is
  the per-import completion lag).

## 2. Measurements (100k census + up to ~245k staged rows/cell)

Reads-alone ladder (20 s phases, all within budgets): r=5 p95
dense 64/sparse 23/nearby 47 ms; r=20: 31/12/23; r=50:
10/2.3/7.2; r=100: 12/2.3/9.3 ms. Offered equals achieved,
zero drops at every rung.

Imports-alone (10 disjoint editions): 1 worker and 4 workers
both stage all 10 with zero errors (~0.1/s throughput,
max single-import ~21–27 s; WAL ~207–293 MB total,
~850–1200 B per staged row, consistent with RST-14).

Mixed cells (25 s reads + 10 editions draining): every rung
5/20/50/100 rps with 1 and 4 importers stays within budgets
with zero read errors/drops and zero import errors. Worst
mixed read p95s: r=5/w=4 dense 73 ms, r=100/w=4 dense 37 /
sparse 15 / nearby 29 ms (dense tail penalty ~2–3x over
sparse under contention). Pool acquire wait 0.0 ms and zero
waiting backends in all cells; checkpoints 0; backlog peaks
at worker count and never grows. Import max latency stretches
under read pressure (42 s at r=100/w=4 vs 21 s alone).

Refinement (knee hunt, local host): r=200 breaches at both
importer levels — w=1 read p95 dense 582 / sparse 535 / nearby
566 ms with 1,034/4,818 dropped, w=4 read p95 514/473/496 ms
with 464/4,956 dropped; imports still complete 10/10 error-free
in both cells. The knee sits inside (100, 200] rps; r=400 was
not run (bounded: the bracket already answers the envelope).

VPS confirmation (Hostinger I9-32GB, `abastevo-temp` staging DB
pod capped at 1 CPU/1 GiB and observed at 95%+ during the runs;
test binary executed on the VPS against the ClusterIP, tunnel
numbers below discarded as path-limited — kubectl port-forward
serializes all traffic and reported seconds even at 5 rps):
reads-alone r=5 green (p95 33–46 ms, 0 drops) and r=20 green
(18–36 ms, 0 drops); r=50 breaches (p95 ~2.6–2.7 s, 261/999
dropped) and r=100 breaches (p95 ~1.2–1.3 s, 683/1,997
dropped). Mixed r=50: w=1 read p95 ~598 ms + 9 drops, w=4
p95 ~2.2 s + 129 drops, imports 10/10 error-free in both
cells (slow: max 57 s / 109 s). No leftovers (0 test DBs),
`abastevo-temp` pods untouched in steady state, scratch
artifacts removed from the VPS afterwards.

## 3. Envelope, limits and next task

- Local workstation envelope: sustainable at 100 rps with 4
  importers (all budgets green); knee bracketed in (100, 200]
  rps. Bottleneck unidentified below the knee (pool wait 0,
  no waiting backends, checkpoints 0) — reported as "at
  least", never as a maximum. Generator headroom was never
  the limit (in-process goroutines, drops zero below the knee).
- VPS staging envelope: sustainable at 20 rps reads-alone;
  knee in (20, 50] rps, resource-explained by the 1-CPU pod
  cap — a staging-sizing fact, not production capacity.
- Reads overlap only the first ~25 s of each import drain;
  longer steady contention needs bigger editions or longer
  phases (follow-up, not extrapolation). Local trials ran on
  a shared noisy host (unrelated ffmpeg/build load observed);
  single-trial p95s carry that caveat, verdicts rest on
  repeated agreement (ladder green twice: full matrix plus
  refinement baseline behavior consistent).
- Reproduce: generate 10 editions to /tmp/rst18 with
  `bench_datasets representative --serial-offset <e*100000>`
  plus `bench_stages --run-id <uuid>`, list them in
  /tmp/rst18/editions.txt, then `go test -tags=integration
  -run NONE -bench 'BenchmarkCapacity(Ladder|Imports|Mixed)'
  -benchtime 1x` with optional `RST18_RATES`/`RST18_REFINE_RATES`
  overrides (~25 min full ladder).
- Next (smallest, separately authorized): RST-19 failure,
  replay and recovery under load.
