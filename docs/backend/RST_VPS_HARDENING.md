# RST operational hardening and bounded VPS acceptance

Status: LOCAL_DONE for implemented corrections; INTEGRATION_PENDING.
Runtime acceptance: PARTIAL / RST-20 and RST-21 NOT_ACCEPTED. User authorization: 2026-10-10, only Abastevo resources.
Current origin: https://teste.abastevo.com.br; namespace: abastevo-temp.
Other apps, images, databases, shared edge, host services and global cleanup are
outside scope. No load test may disable durability, quotas or safety checks.

## Behavior before implementation

- B-BR-RST-H01: aggregate concurrent import throughput is successfully completed
  batches divided by monotonic elapsed import-window seconds, not summed service
  durations. Per-operation latency remains a separate distribution.
- B-BR-RST-H02: PostgreSQL checkpoint metrics name their semantics and use the
  supported server schema; collection errors fail the run or mark an explicit
  unavailable metric, never silently become zero. PostgreSQL 18 completed
  checkpoints use pg_stat_checkpointer.num_done.
- B-BR-RST-H03: preparation/loader resources stay bounded; checksum validation
  and canonical identity/history are preserved across unchanged deltas/retries.
  Recovery is fenced/idempotent, not deletion of partial or unrelated data.
- B-BR-RST-H04: only complete verified official registry facts may flow through
  existing Directory/Profile owners. Synthetic batches remain labelled test
  evidence; PMQC candidates do not gain reviewed trust from importing them.
- B-BR-RST-H05: capacity requests use actual HTTPS HTTP responses and semantic
  checks, fixed scenario seeds, separate warm-up and observation, offered versus
  achieved rates, errors/drops and raw timed artifacts. Request/s is not users;
  user estimates state their per-user request cadence and safety reserve.
- B-BR-RST-H06: VPS mutations and destructive test fixtures are confined to
  Abastevo and uniquely owned disposable artifacts/databases. Existing persistent
  data is retained. Every campaign has CPU/memory/concurrency/time/disk ceilings,
  health/pressure/OOM stop conditions and cleanup of only its own resources.
- B-BR-RST-H07: short pilot results never certify 24/48h stability. Long-run
  outcomes remain pending until their actual duration and accepted artifacts.

BUC-RST-H01: overlapping importers yield correct aggregate throughput.
BUC-RST-H02: denied/unsupported telemetry fails visibly; valid zero is distinct.
BUC-RST-H03: retry interrupted official ingestion without losing accepted facts.
BUC-RST-H04: serve source-separated station profiles through the owned domain.
BUC-RST-H05: stop load safely when an Abastevo/host resource budget is breached.

Execution: metric RED/GREEN and real PostgreSQL 18 checks first; bounded
preparation/loader/publication changes in separate tested checkpoints; guarded
HTTPS capacity/pilot next; truthful RST-21 report and outstanding long evidence.
Each critical persistence/concurrency change requires immediate negative/race/
real-PostGIS tests. No mutation of another namespace or global image/volume prune.

## Metric checkpoint (2026-10-10)

Status: LOCAL_DONE / INTEGRATION_PENDING for metric corrections only.
Validation: PASS (does not close this overall hardening campaign).

RED reproduced four concurrent 2-second imports incorrectly reporting 0.5/s
and the unsupported checkpoint columns. GREEN reports 2/s over the 2-second
monotonic window ending when the last importer exits (not when later reads end).
Individual maximum latency remains separate. Checkpoint failures propagate;
capacity rejects observed resets. Soak WAL/size/dead/backlog collectors now fail
loudly too, and table size excludes indexes instead of double-counting them.
PostgreSQL schema reference: [PG18 checkpointer statistics](https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-PG-STAT-CHECKPOINTER-VIEW).

Commands: targeted `go test -tags=integration`, the same pure regression cases
under `-race`, and a static integration binary in Job
`abastevo-rst-metrics-20261010` (180s deadline, 250m CPU, 256Mi, GOMEMLIMIT 192MiB).
All three tests passed; real PostgreSQL 180006 observed num_done 169 -> 169.
The uniquely created `read_test_*` database was dropped by its own cleanup;
no persistent fixture was truncated or modified. Existing Abastevo API pod
7m/239Mi, DB 22m/182Mi after the run, all service restart counts zero.
Counter is cluster-wide within Abastevo; differences cannot isolate a single
benchmark from concurrent activity. Existing shared host services were untouched.

Historical RST-18 importer-rate and checkpoint values are invalid for comparisons;
RST-20 table/index cost slopes need a rerun with these collectors. Earlier Reader
measurements omit HTTP/TLS/edge overhead. RST-17 UF results are exploratory and
do not authorize a partition migration. Existing 24,552 assertions are synthetic.

## Operational loader contract

B-BR-RST-H08: operational file loads consume at most one bounded JSONL row at a
time plus bounded manifest/input metadata. Exact per-edition membership lives in
PostgreSQL, not a process-wide dedup map. The legacy in-memory adapter remains a
lab compatibility path and is not advertised as the operational loader.
B-BR-RST-H09: one transaction owns a prepared batch under a transaction-scoped
advisory lock. Cancellation/disconnect rolls back its partial attempt; retry
reuses existing assertion IDs, including legacy partial runs. A completed run is
bound to the manifest SHA256; changing a manifest under its run identity is an
error. All inputs complete together, and no retry deletes an assertion/run.
BUC-RST-H06: unchanged deltas complete with stable canonical IDs and separate
per-edition membership; concurrent replay converges; malformed tails, checksum
changes and cancelled transactions do not become visible as completed runs.

B-BR-RST-H10: prepared registry publication pages through complete bound runs,
resolves identity through Directory and ensures an unclaimed profile through the
StationProfile port. PMQC candidates do not create stations, locations, grants or
badges. Valid source municipality/UF fill a missing canonical locality; conflicting
existing locality fails visibly for review. This path does not grant official
eligibility or change the official/community source distinction. Existing profiles
and business projections are preserved. Publication retry is idempotent.

B-BR-RST-H11: ANP metadata defines DATAPUBLICACAO as authorization publication
and DATAVINCULACAO as the distributor relationship date. Neither is an opening
date and no ordering constraint between them is supported. Parser v0.3 / policy
v2 / station-assertion-v2 normalize strict DD/MM/YYYY and ISO calendar dates;
published_at and effective_at retain authorization publication semantics and
brand_linked_at preserves distributor linkage separately. Invalid calendar dates
quarantine; independent date order does not. The v2 checksum has a version prefix
so changed semantics never reuse a v1 assertion identity. Go still accepts
historical v1 batches. The prior RST-01 date-reversal rule is superseded.
Verified source: [ANP registry metadata](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos/metadados-revendedores-varejistas-combustiveis-automoveis.pdf).

## Operational ingestion checkpoint (2026-10-10)

Status: LOCAL_DONE / INTEGRATION_PENDING for ingestion source.
Validation: PASS.

- File-backed Rust emission bounds serialized sorting to 4 MiB/4096 rows,
  fan-in 32, 1024 run paths, 512 MiB cumulative framed spill writes and 100 MiB
  per output (one final spill/merge write can cross the disk guard before refusal).
  Manifest publishes last from an owned sibling directory. Failures clean only
  unpublished owned files. The parsed batch still resides in memory under the
  parser's input/row caps; this is bounded emission, not a streaming CSV parser.
- `prepare_registry` is the operational CLI; it generates no synthetic PMQC.
  Input CSV is capped at 100 MiB, aliases at 4 MiB before reading them in full.
  Legacy `emit_run`/`LoadBatch` remain compatibility/benchmark paths.
- Migration 000053 adds per-edition immutable assertion membership and manifest
  binding without deleting/backfilling history. The operational Go loader reads
  JSONL one row at a time, uses server-side exact dedup, one transaction and an
  advisory lock per batch. An unchanged delta has its own accepted membership
  while sharing stable assertion IDs. Replay does not update existing membership.
- Loader and publication retry only transient connection/serialization failures,
  at most 3 attempts by default (hard maximum 5) under one overall deadline
  (hard maximum 30 minutes), with at most 2 DB connections. Integrity/permission
  failures and caller cancellation are terminal. Process restart reuses the
  same bound manifest and resumes idempotently; a supervisor must restart an
  exited process. Publication pages 100 rows through explicit module ports.
- Assertion v2 preserves distributor dates in the prepared artifact, separately
  from authorization publication. Database effective_date follows publication.
  Raw source and prepared files are private operational artifacts, never Git.

Immediate real PostGIS tests ran in Abastevo-only Jobs, each 250m CPU/256Mi,
180s hard deadline, disposable test databases. `abastevo-rst-prepared-accept-20261010`
passed unchanged/concurrent delta, retained partial IDs, malformed-tail/checksum/
cancel rollback, manifest conflict, migration 52->53 failure rollback/recovery,
restricted-role load plus denied canonical writes/DDL, and backend disconnection
followed by convergent retries. `abastevo-rst-publication-final-20261010` passed
loader -> city HTTP -> unclaimed profile HTTP, replay and conflict negatives.
`abastevo-rst-metrics-final-20261010` passed PG18 counters and fail-loud contention
collection; invalid historic mode aggregate was corrected and queries now scope
active current-database waits. No statistics reset or service restart.

Rust full tests/fmt/clippy passed; the added overlarge-row failure test proves
unpublished spill cleanup preserves a neighboring file. Go affected unit/race,
vet and sqlc vet/generate passed. A transient-retry negative test caught that
context.DeadlineExceeded implements net.Error; explicit cancellation refusal
fixed it. No known failure is deferred. Final scoped VPS import/HTTP evidence
follows; these source tests alone do not certify VPS capacity or long stability.

### Cross-host portability correction

The first bounded VPS preparation run correctly refused a quarantine-output hash
mismatch: quarantine row locators included the caller's absolute CSV path.
The operational CLI now uses the source basename, and a subprocess regression
proves all four artifacts are byte-identical across different parent directories.
No DB import occurred during this refused offline run. The original files remain
as audit evidence; only `prepared-portable` is an authorized load input.

## Frozen VPS HTTP protocol

H12: actual HTTPS, external workstation client, no edge/protection changes,
one TLS connection per request; <=20 offered request/s, <=8 in flight, no client
queue. Ladder 1/5/10/20 request/s uses short diagnostic windows, followed by five
independent 60s warm-up + 300s steady trials at the safe selected rate. These are
pilot windows, not a 24/48h result. A 500ms client HTTPS p95 hypothesis is frozen
before measurement and includes network/TLS/edge; the old 100ms Reader hypothesis
is a different layer. Every error/drop/missed arrival/telemetry failure stops the
campaign. Each route needs >=100 successful samples per trial for p95 qualification;
p99 is unavailable below 10,000 per route. Never average percentiles together.

The frozen 9-request cycle weights dense-city 3, Sorriso 1, single-station city 1,
actual zero-station reference city 1, national text 1, detail 1 and profile 1.
This is a chosen stress distribution, not measured human behavior. Source data:
45,617 accepted ANP assertions across 5,503 IBGE municipalities; 5,571 reference
municipalities; 121 unresolved city-name variants retained in quarantine. The
largest source city has 1,521 accepted assertions (SP/3550308); sparse AC/1200054
has one and empty RO/1100098 has zero. Existing non-ANP development station remains.

H13: no maximum-user claim without saturation evidence and a behavior model.
A tested read-rate lower bound R allows only a scenario estimate:
active users = R * reserve_fraction * seconds_per_cycle / GETs_per_cycle.
For example 70% of the tested rate with one GET/30s differs fourfold from four
GETs/30s. This excludes uploads, OCR, writes, auth and image serving; registered
users/DAU/connection concurrency cannot be inferred from read request/s.

Safety is checked every ~5s (bounded SSH latency also included): retain >=12GiB
host available RAM, >=50GiB free disk, load1<=5.2 (0.65 times 8 CPUs, a conservative load proxy, not percent CPU), every Abastevo
container below 70% of its existing memory limit, healthy/unchanged pods and
restart counts, no OOM, <=4 lock waiters and no growing run backlog. Missing
metrics stop work. Auxiliary Jobs never exceed 250m CPU; preparation 512Mi,
loader/tests 256Mi. Service limits stay API1CPU/1Gi, DB1CPU/1Gi, worker500m/512Mi,
storage500m/1Gi. Guard deletion rechecks this campaign's ownership label and
can delete only one explicitly named disposable Job in abastevo-temp.

### Staging schema compatibility incident

Backup `pre-000053.dump` (mode0600, outside Git) was restored in a uniquely owned
disposable database and retained 24,552 assertions/11 stations before deletion of
that test DB. Backup SHA256: afd5d3778bbd28e80f4d771f2b34f44ec3daa56df1999b8517d143e22aac2a37.
Migration applied only 000053. The old API's strict expected-ledger check then
refused schema53, removing readiness and causing HTTPS502. Load admission refused
while unhealthy. This was a deployment sequencing error, not an internet failure;
no data loss or resource-limit increase occurred. Compiled API/worker 2cd3fb6 were
mounted read-only from an Abastevo-owned path and only the existing Abastevo
Deployment rolled under its unchanged maxSurge0/maxUnavailable1 strategy.
The private media volume/image/env were preserved; three containers were recreated
as part of that owned rollout, not an OOM/crash. HTTPS readiness recovered 200.
Future migrations require preparing/releasing the compatible API/worker together;
never claim zero downtime for this campaign. The temporary command override must
be reflected in the final deployment record, rather than claiming old image tags
identify the running source. No other deployment, image or namespace was changed.

National route qualification initially stopped on HTTP200/detail because the
trace incorrectly expected `id`; the public contract uses `station_id`.
The trace was corrected before capacity measurement. The failed raw diagnostic
is retained and excluded from qualified trials. No protection was weakened.

### Separate pooled-transport campaign (H14)

The v1 external campaign stopped at its first timeout (5req/s warm-up, 15/16
successes). Do not discard or pool this failed trial. Post-stop VPS aggregation
returned only numeric log summaries: 152 API responses, all200, max server duration
324ms; one separately sampled internal public HTTP request took28ms versus270ms
through client HTTPS. This suggests an external-path/connection contribution,
not proof of the exact timeout cause. Only numeric log aggregates left the VPS; no API log records were exported.

A separate v2 campaign uses one reusable verified HTTP1.1/TLS connection per
worker (<=8), matching persistent-client intent more closely than v1's new TLS
connection per request. This is a different transport scenario, not a retry or a
relaxed acceptance budget. Every failed request remains counted and still stops
scheduling; failed connections close without hidden retries. Deadline3s, p95
hypothesis500ms, resource guards, rates, workload and sample rules are unchanged.
Transport regressions prove reuse on success and exactly one request on timeout.
The failed v1 remains an unaccepted connection-churn diagnostic; v2 cannot certify
v1 behavior, mobile network reliability or 24/48h stability.

## Official national publication and selected measurements

The current user request explicitly authorizes this national staging import,
superseding the earlier app-first deferral for this bounded task. Source/runtime
changes do not certify G09, a public pilot, mobile acceptance or production SLA.

The ANP registry downloaded on 2026-10-10 contains 45,738 rows / 7,890,947 bytes,
SHA256 `35101dee91a7a6398043a67d478e60f2b0e0776e5e53436ba80517d9db0c2e8e`.
The source page was updated 2026-10-09. Exact normalized official IBGE aliases
resolve 45,617 rows; 121 unresolved municipality variants remain quarantined.
No fuzzy geographic guess or coordinates were introduced. The prepared output
contains 45,617 assertions / 43,328,274 bytes; complete publication produced
45,617 distinct real stations and 45,617 profiles. Existing identities/profiles
were preserved. The directory has 45,618 stations including one development
station; all 24,552 older synthetic assertions remain separate (70,169 total
assertions). Municipality coverage is 5,503 of 5,571 IBGE reference entries;
this does not claim every city has an imported station or verified coordinates.
The existing Sorriso profile and source-separated public routes return HTTP200.
All new imported points remain absent/unreviewed; the 150m photo rule gains no
location authorization from this registry.

Five serial VPS Rust preparation trials, after one excluded cache warm-up,
measured 4.099–4.888s (median 4.609s), maximum child RSS 115,768 KiB (~113.1 MiB),
under 250m CPU / 512Mi. Every trial reproduced the three output hashes. This
measures full parse, file sorting/emission and fsync; it excludes fetching and
is not a streaming-parser memory-scaling result or a matched Go comparison.

The single actual load Job started 14:46:47Z; ingestion completed14:48:41.672Z
and paged profile publication14:56:17.600Z (~115s load including startup,
~456s publication). These are wall-clock operational milestones, n=1, not a
monotonic repeated throughput benchmark. Whole-Abastevo interval deltas were
227,592,702 WAL bytes, 52,846,592 table bytes excluding indexes, 34,766,848 index
bytes and one completed checkpoint. Background work is included. These values
cannot replace a repeated changed/unchanged delta cost matrix or currency cost.
Seventy guard samples observed >=24,455 MiB host available memory, maximum
load1 4.231, zero lock waiters/guard violations, no observed OOM or unexpected
service restart. Sampling does not prove the absence of every transient peak.
Existing container CPU/memory limits were unchanged.

See [machine-readable selected results](evidence/rst-hardening-20261010/summary.json),
[source provenance](evidence/rst-hardening-20261010/provenance-portable.json),
[deployed executable hashes](evidence/rst-hardening-20261010/deployed-binaries.sha256)
and [private raw bundle manifest](evidence/rst-hardening-20261010/bundle.json).
Source API/worker/loader revision is2cd3fb6; transport tooling revision5d42cb6.
API/worker use the documented read-only executable override; the unchanged
image tags alone do not describe their running revision. Keep those executables
and their owned host directory while the Deployment references them.

![Measured Rust preparation](evidence/rst-hardening-20261010/rust-preparation.svg)

![Observed RAM reserve during import](evidence/rst-hardening-20261010/host-reserve.svg)

## HTTPS outcome and safe stopping

All six campaigns, including failures, are preserved in the private bundle:
wrong-key semantic smoke, corrected 1req/s smoke, failed v1 connection churn,
short pooled5 and pooled10 diagnostics, and attempted replicated pooled5.
The 10req/s diagnostic exceeded the frozen500ms hypothesis (national text
p95~750ms, only33 samples); no saturation/max-user acceptance follows from it.

The first complete pooled5 trial offered/completed1,500 requests over300s,
zero errors/drops/missed arrivals. Per-route p95, in ms: dense180.05 (n501),
Sorriso202.63 (n167), sparse75.46 (n167), empty68.20 (n167), national text292.17
(n166), detail79.10 (n166), profile84.35 (n166). Each route reaches the provisional
p95 sample minimum; p99 remains unavailable. The samples include actual external
HTTPS, edge/network, SQL, serialization and persistent TLS transport. They are
not direct Reader measurements.

At 15:15:09Z during the second steady trial, the guard observed host load1
5.9409 >5.2 and stopped this campaign. Host available RAM was24,702.5 MiB;
Abastevo sampled CPU was API44m / DB97m / worker2m / storage5m, without OOM,
restart or lock queue. Host load is a shared aggregate; its cause is unknown,
and is not CPU-utilization percentage. No other app was inspected, stopped or
modified to make this benchmark pass. The numeric guard was preserved; its
wording now explicitly describes the load proxy.

Thus one valid completed request window exists, **zero accepted five-trial
capacity campaigns**. Four planned complete repetitions remain owed. The guard
worked as intended; never remove the failed diagnostics or rerun until an
attractive number appears. Twenty req/s was not attempted. Maximum users,
mobile reliability, uploads/OCR/writes and long-term headroom are UNPROVEN.
The H13 formula permits only explicitly hypothetical behavior estimates after
an accepted repeated rate; it is not a capacity promise from this pilot.

![One completed HTTPS window, not replicated capacity](evidence/rst-hardening-20261010/https-pooled-qualified-5-p95.svg)

## Reproduction and private artifact retention

The private selected bundle is retained mode0600 on the Abastevo-owned VPS path
in bundle.json, SHA256
`20ccbdd90313d834e678ca1fbe6c82734a491c886d232f2e609067335cb666b8`.
It contains all six campaign CSV/guard/manifest sets, count-only load log,
preparation samples, provenance, hashes and aggregate JSON. It excludes raw
source rows, credentials, API log records and backup dumps. The official source
snapshot and prepared files are retained separately under the same owned
`official/` directory. Retrieving current provider URLs may yield a different
edition: exact reproduction requires the preserved snapshot and its hashes.
Raw historical RST13–20 files missing from earlier ephemeral bundles remain
missing; this archive does not recreate them.

From a clean checkout containing these corrections, use Python3 stdlib for the
harness and ReportLab4.4.9 for report rendering (available in the workspace
bundled runtime; no production dependency). After retrieving/verifying the
private bundle into a unique local directory, run:

```sh
sha256sum selected-evidence.tar.gz
mkdir selected-evidence
tar -xzf selected-evidence.tar.gz -C selected-evidence
python3 infra/scripts/load/report_hardening.py --root selected-evidence --out selected-report
```

The validator checks each named raw hash, request/error/drop accounting and
per-route p95 against raw CSV. It refuses unhashed/repeated trial records and
never promotes an interrupted campaign to replicated capacity. SVG plots and
summary.json are regenerated; hashes.json records their bytes. Plot rendering
versions can change SVG bytes, so pin ReportLab for byte comparison. The source
revision, transport revision and snapshot hashes define the selected experiment.

For a **new**, separately recorded edition, the bounded official-only downloader
is `python3 infra/scripts/load/fetch_official_registry.py --out UNIQUE_DIRECTORY`.
Build the pure Rust preparer with `cargo build --release --locked --bin
prepare_registry` in tools/station-prep; use a target/runtime compatible with
the pinned Debian2.41 job image. CLI argument order is CSV, aliases, unique output
directory, source URL, edition, UUID and preparation timestamp. Frozen arguments
and 250m/512Mi deployment are in
[the preparation Job record](../../infra/scripts/load/manifests/rst-prepare-20261010.yaml).
Build its static timing driver with `CGO_ENABLED=0 go build -o DRIVER
 tools/station-prep/bench/vps_prepare.go` from the repository root. It runs one
excluded warm-up plus five measured serial children, checking output hashes.

Build the Go loader from backend using `CGO_ENABLED=0 GOOS=linux GOARCH=amd64
go build -o STATION_LOAD ./cmd/station-load`. The
[load Job record](../../infra/scripts/load/manifests/rst-load-official-20261010.yaml)
uses the existing secret reference,250m/256Mi, one supervisor retry,900s hard
wall and `--publish --timeout14m --attempts3`. Supply flags as separate tokens
as shown in YAML (`--timeout`, `14m`); never embed a DSN in commands/logs.
These YAMLs record the executed dated snapshot, not an automatic fresh-cluster
installer. Do not reapply blindly: first verify namespace, ownership, compatible
schema/API, backup, frozen files and guard admission. Migration53 already exists
in staging; never rerun or roll it back to replay this benchmark. Only explicitly
owned disposable test databases may be destroyed. No global prune/host cleanup.
The local guard for an explicitly owned new Job is:

```sh
python3 infra/scripts/load/guard_job.py --job EXACT_OWNED_JOB --out UNIQUE_GUARD.jsonl --seconds 900
```

The load guard starts immediately after Job creation; activeDeadlineSeconds and
resource limits remain independent hard fences. Do not start a new Job while
an admission snapshot violates the existing guard. In a safe reserved window,
actual HTTP replay from the workstation uses the committed frozen routes and
preserved dataset manifest, with a **unique** output directory:

```sh
python3 infra/scripts/load/guarded_http.py --routes docs/backend/evidence/rst-hardening-20261010/routes.json --dataset-manifest FROZEN_MANIFEST.json --rate 5 --p95-ms 500 --clients 8 --warmup 60 --seconds 300 --trials 5 --out UNIQUE_HTTP_DIRECTORY
```

This current v2 command cannot reproduce v1's connection-per-request transport;
use its recorded revision8dc3cd8 only for that distinct diagnostic. Neither
command modifies edge protection. A nonzero exit/guard stop remains a result,
not authorization to widen limits or stop another service.

## RST-20 / RST-21 remaining acceptance

RST-20: 24h/48h mixed ingestion/retention campaigns have **not started**. The old
30-minute Reader pilot is historical only; its loader-blocking sampling and
retention behavior do not qualify the required long protocol. Before a long
run, implement continuous nonblocking telemetry, bounded repeated edition
inputs, loss-accounted request samples and an approved retention strategy in
isolated Abastevo resources. Preserve shared-host reserves throughout. This
session's safety stop prevents claiming a professional sustained envelope.
Repeated changed/unchanged cost, maintenance/leak slopes, interval CPU/IO,
and provider price/allocation evidence remain owed; no currency price invented.

RST-21: selected corrections, aggregate machine-readable results, plots and raw
bundle are reproducible; **overall matrix remains PARTIAL / NOT_ACCEPTED**.
Retain the deployed indexed unpartitioned layout. UF-LIST/trigram/BRIN remain
explicit hypotheses requiring matched representative repeated trials plus a
separate migration/recovery decision. No UF partition adoption occurred.
Omitted: repeated qualified HTTPS, mixed HTTP+import under steady load, qualified
p99, long maintenance/retention, memory scaling vs rows, recovery timing matrix,
fully streaming CSV parser and COPY comparison, historical missing raw evidence.
No missing metric is filled with zero; no RST20/21, G09 or release gate is closed.
Next action: fix the long-run harness, then obtain a quiet provisioned Abastevo
window and complete the stopped repeated HTTP matrix before scheduling the
separately bounded 24h/48h evidence. Do not use another app's capacity or change
another container to obtain that window.
