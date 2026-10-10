# Bounded daily official registry refresh

Status: IN_PROGRESS. Selected scope: current user request (2026-10-10), P25-T02/T04/T05 operational refresh; existing staging origin and Abastevo namespace only. This does not certify G25/G09 or the outstanding RST20 long campaigns.

## Behavior before implementation

- B-BR-SYNC01: one short-lived Abastevo CronJob checks official ANP registry and IBGE references every24 hours. No resident refresh process between runs. No assertion of instantaneous upstream updates: newly validated facts become queryable during successful paged publication.
- B-BR-SYNC02: downloads stream to owned disk under byte/deadline/redirect bounds. The Rust child prepares one bounded snapshot; the Go loader validates/publishes it. Children run serially under one resource envelope. No source rows, DSNs or response bodies in logs.
- B-BR-SYNC03: unchanged source/reference/pipeline fingerprint verifies the last prepared publication in the DB and skips preparation/writes. Durable pending metadata and exact frozen files survive process death; retry resumes that batch before fetching another. Success is recorded only after publication. Failure preserves last success and canonical/history data.
- B-BR-SYNC04: CronJob Forbid plus an exclusive OS lock on the shared Abastevo state directory prevents overlapping processes; no foreign directory cleanup. Snapshot identity/hash/timestamp are immutable on resume. Only owned temporary/previous artifacts may be removed; no persistent DB facts are deleted.
- B-BR-SYNC05: source-backed name/address refresh may update the canonical projection only if its current values match previously published prepared evidence, or already match the new facts. Curated name/address differences are preserved and explicitly counted; locality conflicts fail visibly. No price, profile claims, status, reviewed coordinates, photo/media or community facts are overwritten. Missing rows never imply closure.
- B-BR-SYNC06: already published unchanged assertions are not republished. Successful verification accounts the full eligible membership and zero unpublished rows. New/changed facts create or update queryable station/profile projections through owning ports, with replay and failure recovery.
- B-BR-SYNC07: the real VPS load test uses actual HTTPS alongside the bounded sync job, frozen workload/latency budgets and existing host/service guard. Only Abastevo Jobs/cron configuration/binaries may change; preserve existing API/worker/media containers and data when their executable bytes do not need replacement. Never roll an emptyDir media pod merely to refresh identical source.

BUC-SYNC01: a daily unchanged edition is verified without preparing/inserting another batch.
BUC-SYNC02: failure after prepare/load/partial publication resumes exact files and stable IDs.
BUC-SYNC03: a changed official name/address becomes readable; conflicting curated data remains intact and causes a visible failure.
BUC-SYNC04: duplicate workers, oversized/redirected/malformed sources, cancellation and missing DB evidence fail safely.
BUC-SYNC05: mixed HTTP+sync run records CPU/RSS/time/accounting and stops on shared-host pressure without changing other systems.

Acceptance requires meaningful unit/race/negative tests, immediate real PostGIS publication/recovery/concurrency checks, capped live-source VPS execution, actual HTTPS mixed-load results and scoped deployment/source provenance. Daily schedule activation is runtime configuration, not proof that24 hours have elapsed or that24/48h soak passed.

## Implemented source checkpoint

Status: LOCAL_DONE (source) / INTEGRATION_PENDING. Validation: PASS for scoped
source acceptance; live deployment/mixed-load evidence follows separately.

The Go supervisor downloads ANP up to12MiB and IBGE up to8MiB (both wire and
decoded), writes a sorted exact alias reference up to4MiB, and runs Rust/loader
children serially. It persists source/reference/pipeline fingerprints, a unique
edition UUID, frozen timestamp/manifest hash and pending/success pointers with
fsync+rename. A filesystem lock covers the complete run. Recovery includes the
rename-before-state crash window; private retention keeps the current and one
previous complete snapshot, reaping only an explicitly recorded owned snapshot.
Owned killed download/spill directories are cleaned under the same exclusive
state lock. No assertion, canonical identity, profile, media or other app data is
reaped. Only the source's available registry fields are refreshed, not every
application table or unavailable coordinates/price facts.

Unchanged downloads invoke the loader's read-only manifest/publication check.
A changed edition loads immutable per-edition membership and publishes only
missing or differing canonical facts; facts reverting to an earlier checksum
still refresh. SQL protects curated name/address pairs and locality conflicts.
No schema migration is needed; schema53 and existing API remain compatible.

Tests: Go unit/race/vet plus sqlc generate/vet; real VPS PostgreSQL18.6/PostGIS
Jobs (250m CPU/256Mi) `abastevo-rst-sync-publication-v2-20261010` and
`abastevo-rst-sync-pipeline-20261010` passed canonical changed/reverted facts,
unchanged no-republication, explicit curated-conflict preservation, actual Rust preparation
through atomic load/publication, crash-after-load resume with no refetch, changed
read and exclusive concurrent-worker refusal. All DB tests use unique disposable
Abastevo databases; no persistent fixture is truncated. Unit negatives cover
oversize/gzip expansion, foreign redirects, malformed references, cancellation,
source tampering, overlap, missing DB evidence, rename recovery and bounded
private artifact retention without deleting a neighboring directory.

The deployment template is [the scoped CronJob](../../infra/k8s/registry-sync-cronjob.yaml).
It starts suspended, runs daily03:00 America/Cuiaba after validation, forbids
scheduled overlap, allows at most two supervisor retries, and has a20-minute hard
Job deadline. Resources are25m/32Mi requested and250m/256Mi maximum. The Go
supervisor's soft heap target is32MiB; loader64MiB. Idle daily-refresh memory is
zero. This is not a claim that the existing general app worker uses zero memory.
The already cached pinned PostGIS image supplies glibc2.41/CA certificates for
the Rust executable; its PostgreSQL entrypoint is overridden and never starts.
Tested binaries are mounted read-only from an immutable revision directory,
with hashes recorded; this is explicit artifact provenance, not a claim that the
runtime image tag embeds the current application source. No new dependency.

The worker independently checks available host RAM>=12GiB, disk>=50GiB,
load1<=0.65 times host CPU count and actual Abastevo HTTPS readiness every5s.
It cancels its child on failure. This load proxy is not CPU-utilization percent.
Container limits and a Job deadline remain hard fences. Logs contain only
stage/outcome/time/child CPU/RSS and aggregate container peak, never child output,
source rows or credentials. Failures preserve pending metadata and last success.
Daily scheduling does not establish a24/48h soak result or production certification.

### Live preflight correction: curated facts

Read-only aggregate preflight found6 existing name and10 address differences
against the earlier ANP publication, with0 locality differences. Rejecting the
whole national job for these existing curated projections would block legitimate
updates. B-BR-SYNC05 therefore records curated name/address preservation as an
explicit `preserved_curated` count, while retaining the raw official assertion
and publishing other rows. It never chooses another value or changes community
facts. Locality conflict still refuses publication. The read-only daily check
verifies all source-owned projections and separately counts preserved curated
facts; it does not mislabel them as equal. Missing DB/manifest evidence remains
a failure. Tests must prove this before any live mutation/job activation.

The revised curated policy passed immediate PostgreSQL tests in
`abastevo-rst-sync-curated-20261010` (actual Rust pipeline plus changed/reverted/
curated publication), including an explicit preserved count returned by read-only
verification. Go affected race/vet and sqlc vet passed after this correction.
Only allowlisted numeric child counters (up to4KiB buffered output) reach logs;
the new resource event records `preserved_curated` rather than hiding a difference.
The initial988740b binaries were installed for a server-side CronJob dry run
only; no live sync used that superseded refusal policy.
