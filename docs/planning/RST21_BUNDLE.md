# RST-21 bundle index (environment prep, not the decision)

> RST-21 remains PARTIAL / NOT_ACCEPTED until the missing long campaigns and reproducible selected matrix exist. This index is not an acceptance report. The corrections below supersede historical layer/metric/partition claims.

Staging index for the RST-21 decision/report task. Every row names
the committed evidence, its scale label and its raw-artifact status:
raw per-run logs lived in ephemeral local bundles (`/tmp/rst*`)
and are NOT committed — only sanitized summaries, manifests and
small synthetic fixtures are versioned, per the metric dictionary.
The historical commands describe attempted reproduction; missing ephemeral raw logs prevent auditing every historical number. Current reproducibility requires preserved inputs, raw artifacts, hashes and an explicit environment manifest.

| Phase | Evidence | Scale / verdict |
|---|---|---|
| RST-10 | `RUST_STATION_INGESTION_RST10.md` | protocol frozen, budgets provisional |
| RST-11 | `RUST_STATION_INGESTION_RST11.md` + `contracts/testdata/station-prep/datasets/` | deterministic datasets, frozen tiny oracle |
| RST-12 | `RUST_STATION_INGESTION_RST12.md` | harness qualified (9/9 loopback) |
| RST-13 | `RUST_STATION_INGESTION_RST13.md` | prep 20k/100k/1M matrix, spill accounting |
| RST-14 | `RUST_STATION_INGESTION_RST14.md` + `emit-tiny[/-delta1]/` | construction baseline, chain contracts fixed |
| RST-15 | `RUST_STATION_INGESTION_RST15.md` | 9 workloads raw SQL / in-process Reader, trigram accept-pending-migration |
| RST-16 | `RUST_STATION_INGESTION_RST16.md` | Vincenty oracle exact, 150m gate pins |
| RST-17 | `RUST_STATION_INGESTION_RST17.md` | EXPLORATORY: UF-LIST/BRIN hypotheses; no adoption accepted |
| RST-18 | `RUST_STATION_INGESTION_RST18.md` | Reader diagnostic only; importer/checkpoint metrics invalid |
| RST-19 | `RUST_STATION_INGESTION_RST19.md` | fault matrix green, fail-loud reload |
| RST-20 | `RUST_STATION_INGESTION_RST20.md` | 30-min pilot green; 24/48h OWED |

Omitted scenarios (never zero-filled): 8/32 partition counts,
1M spatial reads, concurrent live-index creation, upstream
429/timeout fetch, disk-full injection, 48h virtual-clock
outage, VPS soak rerun, currency costs (no prices supplied).
Operational bounded emission, unchanged membership and idempotent retry/publication are now tested separately in [hardening](../backend/RST_VPS_HARDENING.md). Unaccepted obligations include fully streaming parsing, COPY optimization, controlled durable reaping/retention, adoption migrations, HTTP budget qualification, missing historical raw artifacts and 24/48h campaigns.

Hypothesis ledger (draft for RST-21 to finalize):
retain current physical layout; evaluate UF-LIST, trigram GIN and BRIN history;
reject hash-16, generic-plan forcing;
change needed: loader resume/reap, retention strategy,
100ms budget revision, checkpoint instrumentation.


## 2026-10-10 selected VPS hardening bundle

[Operational corrections and bounded live evidence](../backend/RST_VPS_HARDENING.md)
supersede earlier aggregate/checkpoint/API-layer claims. The selected
[machine-readable report](../backend/evidence/rst-hardening-20261010/summary.json)
and [private archive manifest](../backend/evidence/rst-hardening-20261010/bundle.json)
cover actual45,617 ANP stations/profile publication, five preparation repetitions,
a single300s HTTPS window and the subsequent host-reserve stop. All six failed or
successful HTTP campaigns are retained. Zero replicated capacity campaigns were
accepted; RST20 long runs and the full RST21 matrix remain NOT_ACCEPTED.
The corrected source implements operational bounded emission, unchanged-edition
membership, transient retry and publication. Remaining resume/reaping work
concerns a durable scheduler and maintenance, not deletion of retained facts.
Keep unpartitioned deployment; evaluate UF-LIST only in a separate matched trial.
