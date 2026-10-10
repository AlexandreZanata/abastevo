# Bounded daily official registry refresh

Status: LOCAL_DONE / INTEGRATION_PENDING (source); DEPLOYED (bounded staging acceptance). Selected scope: current user request (2026-10-10), P25-T02/T04/T05 operational refresh; existing staging origin and Abastevo namespace only. This does not certify G25/G09 or the outstanding RST20 long campaigns.

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
BUC-SYNC03: a changed source-owned name/address becomes readable; curated differences remain intact and are counted; locality conflicts cause a visible failure.
BUC-SYNC04: duplicate workers, oversized/redirected/malformed sources, cancellation and missing DB evidence fail safely.
BUC-SYNC05: mixed HTTP+sync run records CPU/RSS/time/accounting and stops on shared-host pressure without changing other systems.

Acceptance requires meaningful unit/race/negative tests, immediate real PostGIS publication/recovery/concurrency checks, capped live-source VPS execution, actual HTTPS mixed-load results and scoped deployment/source provenance. Daily schedule activation is runtime configuration, not proof that24 hours have elapsed or that24/48h soak passed.

## Implemented source checkpoint

Status: LOCAL_DONE
Validation: PASS

Integration: INTEGRATION_PENDING. These declarations cover scoped source
acceptance; deployed runtime and mixed-load evidence follow separately.

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

## Reproduction and scoped operations

Source checks (from the repository root):

```sh
GOCACHE=/tmp/abastevo-go-cache go -C backend test -race ./cmd/station-sync ./cmd/station-load ./internal/modules/directory/adapters/registrysync ./internal/modules/directory/adapters/registry
GOCACHE=/tmp/abastevo-go-cache go -C backend vet ./cmd/station-sync ./cmd/station-load ./internal/modules/directory/adapters/registrysync ./internal/modules/directory/adapters/registry ./internal/modules/directory/adapters
```

The critical integration tests require `-tags=integration` and the documented
`ANPFUEL_TEST_DATABASE_URL` for a disposable PostgreSQL/PostGIS instance; never
supply the persistent application's DSN to a test fixture. Select
`TestDailySyncActualRustLoaderRecoveryChangedReadAndOverlap`,
`TestPreparedPublicationThroughHTTPAndProfiles`, and
`TestPreparedDailyRefreshUpdatesOwnedFactsSkipsUnchangedAndPreservesConflicts`.
The actual Rust pipeline test additionally needs its existing preparer executable;
`ANPFUEL_TEST_PREPARER_PATH` selects that executable. Each integration fixture creates and cleans
its own uniquely named database, retaining persistent Abastevo data.

Build `station-sync` and `station-load` from the tested source with
`CGO_ENABLED=0 GOOS=linux GOARCH=amd64`, `-trimpath`, and `-ldflags='-s -w'`.
Install them plus the tested Rust `prepare_registry` in an immutable, read-only
revision directory under `/opt/abastevo-temp/registry-sync/releases/`. Compare all
three SHA-256 values before deployment. Keep the separate private state directory
owned by UID1000 with mode0700. The CronJob template pins both the runtime image
digest and this revision directory; its initial suspension is intentional.

Render `__REVISION__`, validate the rendered manifest with the Kubernetes server,
and apply only this namespaced CronJob. Before a bounded manual run, check the
existing resource guard; generate a Job from the CronJob, use an explicit
`abastevo-rst-*` name, set ownership label `app=abastevo-rst-hardening` and
`backoffLimit=0`, then run `infra/scripts/load/guard_job.py` for that exact Job.
A mixed test uses `guarded_http.py` with the committed hardening routes,
`--rate 5 --clients 8 --warmup 10 --seconds 300 --trials 1 --p95-ms 500`.
Couple any HTTP failure to `guard_job.stop_owned` for only the named test Job.
These are one-window operational pilots, not the RST20/21 capacity campaign.

Enable the daily schedule only after successful live publication, unchanged
verification, resource/HTTP acceptance and provenance checks. To pause, patch
only `abastevo-registry-sync.spec.suspend=true`; an already running Job requires
separate identification and ownership verification. Keep pending/success state,
prepared snapshots and DB history for recovery. To change executables, install a
new immutable release and change only this CronJob's tools path; never overwrite
a mounted executable or roll the API/media pod for a CLI-only change. No edge,
other namespace, image cleanup or foreign workload operation belongs here.

## Live execution and final application update (2026-10-10)

Tested behavior revision: `13c7c25`. Immutable mounted executables have explicit
SHA-256 provenance; the existing API/worker image tags remain unchanged and do
not identify the running source. Existing schema53 is retained; no new migration.
The real source hash matches the earlier imported national edition, so this
first worker run proves complete edition membership/revalidation and publication
accounting, not a newly changed upstream station. Changed/reverted facts are
proved by the immediate real PostGIS integration tests.

`abastevo-rst-sync-live-20261010` succeeded in120.7796s: Rust prepare5.3026s,
1.3215 child CPU-seconds,115608KiB child RSS; Go load/publication113.5025s,
27.3946 child CPU-seconds,14336KiB child RSS. Cgroup memory peak198205440B
(189.0MiB, including file cache), under256Mi. Source45738 rows reconciled to45617
accepted and121 quarantined;10 curated projections preserved explicitly. No
coordinates promoted. Assertions remain70169 (24552 synthetic plus45617 real),
Directory stations45618 (including the existing development fixture). A new
edition membership is not45617 new canonical stations.

`abastevo-rst-sync-unchanged-20261010` succeeded in2.8713s, cgroup peak21700608B
(20.7MiB). Read-only verification0.7018s,0.0119 child CPU-seconds,14168KiB child
RSS,45617 accounted and10 preserved curated; no preparer or load/publication
stage. These child CPU numbers exclude supervisor and DB CPU; they are not total
host cost. Persistent private prepared state54031940B for this first snapshot;
future retention keeps at most current+previous completed snapshots plus pending.
DB/history storage is intentionally not reaped by this worker.

One actual HTTPS mixed pilot offered/completed1500 requests over300.0004s at5/s,
zero errors/drops/missed arrivals, >=166 samples per route and worst route
p95=274.736ms (national text), below500ms. Preparation/loading overlaps only the
first part of the window (worker120.8s), followed by reads after publication;
this is not300s of continuous import pressure. The separate10s/50-request warmup
had a cold dense-route p95=1044.151ms; it is retained and excluded from steady
acceptance rather than hidden. Route p99 is unqualified/null, and this is one
mixed trial, not five repeated capacity trials.

Mixed-job guard15 samples: maximum host load1=3.7446 under5.2, minimum available
RAM24547.2MiB, sampled DB memory<=200MiB, zero guard violations, no OOM or
unexpected service restart. Host load and PG/WAL counters are shared aggregates,
not isolated cost attribution. Client-side verified HTTPS includes network/TLS/
edge behavior; requests ask no-cache but do not prove every backend cache miss.

The final deployment also includes existing community fix `0cbedf9`, which
converges identical concurrent photo retries while refusing conflicting payloads.
Three immediate PostGIS cases passed five repetitions (15 test executions),
including eight concurrent identical submissions and divergent/subset negatives.
An explicit disposable anchor DB supplies the test child URL; fixtures migrate
only new `community_test_*` DBs. All cleanup passed. The initial attempt using
the application secret directly as the test URL was rejected by automatic review
before execution. The approved isolated bootstrap instead creates/removes only
`abastevo_rst_community_anchor_20261010`, redirects the child to it, and filters
output to case names/outcomes. No persistent application fixture was modified.
The community adapters package has no non-integration tests; its unit invocation
is not counted as race-tested behavior. Existing scoped sync/loader race tests
and the actual concurrent DB cases provide their separately named evidence.

API and existing worker now execute13c7c25 binaries using atomic, versioned
read-only mounted entrypoints. The exact container PID1 command was checked
before SIGTERM; only API and worker restarted once intentionally. Pod UID remains
`8c3c2e3b-615e-4af3-98b8-d215460a074a`; private-storage and DB restarts remain0,
and the private media emptyDir was not recreated, copied or deleted. Previous
executables remain for recovery. An aggregate filesystem hash comparison varied:
MinIO internal bookkeeping changes while running; the inspected volume contained
only internal `.minio.sys` files. That comparison is not accepted as per-photo
integrity evidence. No media preservation claim is inferred from a stable global
filesystem digest, and no storage/DB/foreign workload restart was performed.

The final13c7c25 API completed a separate300.0002s HTTPS window at5/s:1500
correct responses, zero errors/drops/missed arrivals, worst route p95=473.837ms
(national text), below500ms. This window has no ongoing ingestion and is not
statistically interchangeable with the first mixed window. Both artifacts retain
all route counts/quantiles and unqualified p99. Total steady requests3000;
this proves the tested envelope, not maximum users or replicated RST capacity.

The deployed CronJob is now unsuspended, daily03:00 America/Cuiaba, Forbid,
250m/256Mi and1200s deadline, tools pinned to13c7c25. `lastScheduleTime` is null
at activation: both successful sync runs were manual acceptance Jobs, not an
elapsed daily/24h scheduled run. Final read-only accounting:45618 stations,
45617 profiles,70169 assertions. All45617 prepared ANP station IDs have profiles;
one pre-existing non-ANP identity has no profile and remains outside this source
publication. The initial whole-directory count assertion expected45618 profiles
and failed; it was corrected to distinguish source membership rather than create
a synthetic profile or misreport acceptance. Existing API/general
worker each have one intentional restart; storage/DB remain at zero.

[Sanitized acceptance/provenance](evidence/registry-sync-20261010/acceptance.json),
[prepared manifest](evidence/registry-sync-20261010/prepared-manifest.json),
[mixed HTTPS](evidence/registry-sync-20261010/https-mixed-5.json),
[final API HTTPS](evidence/registry-sync-20261010/https-postdeploy-5.json) and
[executable hashes](evidence/registry-sync-20261010/deployed-binaries.sha256)
record the measured revision, bounds, counters and raw artifact fingerprints.
Only aggregate/method/provenance data is committed; source rows, photos,
credentials and raw API logs remain excluded. RST20/21 prolonged stability,
full costs and maximum user capacity remain PARTIAL / NOT_ACCEPTED.

The selected raw bundle (CSV timings, guard telemetry, numeric worker/test logs,
metadata and scoped operation sources; no raw station rows/photos/API logs or
credentials) is retained mode0600 at
`/opt/abastevo-temp/registry-sync/evidence/20261010/selected-evidence.tar.gz`.
SHA-256: `2539b17df9ff518b723aca0e84f25aaa2ef7548c66ec8f74782c638bb16799da`.
It preserves the failed global-media comparison as an unaccepted diagnostic,
and never treats it as successful integrity evidence.
