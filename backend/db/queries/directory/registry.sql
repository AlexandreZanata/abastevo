-- Owned by directory (registry staging, P25-T02). Staging only: publishers
-- read complete runs in P25-T04; failed/quarantined runs stay invisible.

-- name: CreateRegistryRun :one
INSERT INTO registry_source_runs (id, source, snapshot_identity, checksum, parser_version)
VALUES (@id, @source, @snapshot_identity, @checksum, @parser_version)
ON CONFLICT (source, snapshot_identity) DO NOTHING
RETURNING id, source, snapshot_identity, checksum, parser_version, state,
    accepted, duplicates, rejected, error_code, started_at, finished_at;

-- name: GetRegistryRun :one
SELECT id, source, snapshot_identity, checksum, parser_version, state,
    accepted, duplicates, rejected, error_code, started_at, finished_at
FROM registry_source_runs
WHERE source = @source AND snapshot_identity = @snapshot_identity;

-- name: FinishRegistryRun :execrows
UPDATE registry_source_runs
SET state = @state, accepted = @accepted, duplicates = @duplicates,
    rejected = @rejected, error_code = @error_code, finished_at = now()
WHERE id = @id AND state = 'running';

-- name: StageRegistryAssertion :one
INSERT INTO registry_assertions (
    id, run_id, source, source_key, checksum, display_name, address,
    municipality_code, state, auth_state, operation, eligibility,
    location_quality, source_reference, effective_date,
    latitude, longitude, crs
) VALUES (
    @id, @run_id, @source, @source_key, @checksum, @display_name,
    @address, @municipality_code, @state, @auth_state, @operation,
    @eligibility, @location_quality, @source_reference, @effective_date,
    @latitude, @longitude, @crs
)
ON CONFLICT (source, source_key, checksum) DO NOTHING
RETURNING id;

-- name: CountRegistryAssertions :one
SELECT count(*) FROM registry_assertions WHERE run_id = @run_id;

-- name: CountCompleteRegistryRuns :one
SELECT count(*) FROM registry_source_runs WHERE source = @source AND state = 'complete';

-- name: GetRegistryRunByID :one
SELECT id, source, snapshot_identity, checksum, parser_version, state,
    accepted, duplicates, rejected, error_code, started_at, finished_at
FROM registry_source_runs
WHERE id = @id;

-- name: ListRegistryAssertions :many
SELECT id, run_id, source, source_key, checksum, display_name, address,
    municipality_code, state, auth_state, operation, eligibility,
    location_quality, source_reference, latitude, longitude, crs,
    station_id
FROM registry_assertions
WHERE registry_assertions.run_id = @run_id OR EXISTS (SELECT 1 FROM registry_run_assertions AS m
    WHERE m.run_id = @run_id AND m.assertion_id = registry_assertions.id)
ORDER BY source_key, checksum;

-- name: SetAssertionStation :execrows
UPDATE registry_assertions
SET station_id = @station_id
WHERE id = @id AND station_id IS NULL;

-- name: UpdateStationStatus :execrows
UPDATE directory_stations
SET status = @status
WHERE id = @id AND status <> @status
  -- Revocation sticks: a sourced suspended/revoked status is never
  -- cleared back to active by a later snapshot; reactivation is an
  -- audited operator decision, not an import side effect.
  AND NOT (status IN ('suspended', 'revoked') AND @status = 'active');

-- name: LastCompleteRegistryRun :one
SELECT id, source, snapshot_identity, checksum, parser_version, state,
    accepted, duplicates, rejected, error_code, started_at, finished_at
FROM registry_source_runs
WHERE source = @source AND state = 'complete'
ORDER BY finished_at DESC NULLS LAST, started_at DESC
LIMIT 1;

-- name: SetAssertionSuperseded :execrows
UPDATE registry_assertions
SET superseded_by = @superseded_by
WHERE id = @id AND superseded_by IS NULL;

-- name: FindOfficialAssertion :one
SELECT a.source, a.source_key, a.display_name, a.municipality_code, a.state,
    a.auth_state, a.eligibility, r.finished_at AS run_finished_at
FROM registry_assertions AS a
JOIN registry_source_runs AS r ON r.id = a.run_id
WHERE a.source_key = @source_key
  AND a.source IN ('registry-csv', 'registry-api')
  AND r.state = 'complete'
ORDER BY r.finished_at DESC NULLS LAST, r.started_at DESC
LIMIT 1;

-- Owned by directory (location review, RST-06). The CRS transform stays
-- inside PostGIS/PROJ: Go plumbs coordinates, never math. Unknown CRS
-- names fail at the Go gate before reaching SQL.

-- name: GetReviewEvidence :one
SELECT a.id, a.run_id, a.source, a.source_key, a.checksum, a.display_name,
    a.address, a.municipality_code, a.state, a.auth_state, a.operation,
    a.eligibility, a.location_quality, a.source_reference, a.latitude,
    a.longitude, a.crs, a.station_id, a.effective_date, r.state AS run_state
FROM registry_assertions AS a
JOIN registry_source_runs AS r ON r.id = a.run_id
WHERE a.id = @id;

-- name: TransformPoint :one
SELECT ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint(@lon::float8, @lat::float8), @srid::int), 4326))::float8 AS latitude,
    ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(@lon::float8, @lat::float8), @srid::int), 4326))::float8 AS longitude;

-- Owned by directory (station suggestions, P27-T01). Private intake:
-- one row per owner idempotency key; owner-only reads; decisions land
-- in T02 (approved/rejected), cancellation stays owner-side.

-- name: CreateSuggestion :one
INSERT INTO station_suggestions (id, account_id, client_submission_id, proposal, evidence_ref)
VALUES (@id, @account_id, @client_submission_id, @proposal, @evidence_ref)
ON CONFLICT (account_id, client_submission_id) DO NOTHING
RETURNING id, account_id, client_submission_id, proposal, evidence_ref,
    state, created_at, decided_at;

-- name: GetSuggestion :one
SELECT id, account_id, client_submission_id, proposal, evidence_ref,
    state, created_at, decided_at
FROM station_suggestions
WHERE id = @id;

-- name: GetSuggestionByKey :one
SELECT id, account_id, client_submission_id, proposal, evidence_ref,
    state, created_at, decided_at
FROM station_suggestions
WHERE account_id = @account_id AND client_submission_id = @client_submission_id;

-- name: ListOwnedSuggestions :many
SELECT id, account_id, client_submission_id, proposal, evidence_ref,
    state, created_at, decided_at
FROM station_suggestions
WHERE account_id = @account_id
ORDER BY created_at DESC
LIMIT @page_limit::int OFFSET @page_offset::int;

-- name: CountRecentSuggestions :one
SELECT count(*) FROM station_suggestions
WHERE account_id = @account_id AND created_at > now() - make_interval(days => 1);

-- name: CancelSuggestion :execrows
UPDATE station_suggestions
SET state = 'cancelled'
WHERE id = @id AND account_id = @account_id AND state = 'pending';

-- Owned by directory (suggestion review, P27-T02). Audited decisions:
-- only pending suggestions transition; concurrent reviewers converge
-- on the SQL guard instead of overwriting.

-- name: CreateDecision :one
INSERT INTO suggestion_decisions (id, suggestion_id, decision, reason, reviewer, station_id)
VALUES (@id, @suggestion_id, @decision, @reason, @reviewer, @station_id)
RETURNING id, suggestion_id, decision, reason, reviewer, station_id, decided_at;

-- name: DecideSuggestion :execrows
UPDATE station_suggestions
SET state = @state, decided_at = now()
WHERE id = @id AND state = 'pending';

-- name: ListPendingSuggestions :many
SELECT id, account_id, client_submission_id, proposal, evidence_ref,
    state, created_at, decided_at
FROM station_suggestions
WHERE state = 'pending'
ORDER BY created_at ASC
LIMIT @page_limit::int;

-- name: BindPreparedManifest :one
UPDATE registry_source_runs SET prepared_manifest_sha256 = @manifest_sha256
WHERE id = @id AND checksum = @checksum
  AND (prepared_manifest_sha256 = '' OR prepared_manifest_sha256 = @manifest_sha256)
RETURNING id;

-- name: ResumePreparedRun :execrows
UPDATE registry_source_runs SET state = 'running', accepted = 0, duplicates = 0,
    rejected = 0, error_code = '', finished_at = NULL
WHERE id = @id AND source = 'station-prep' AND state IN ('failed', 'running');

-- name: AttachPreparedAssertion :one
INSERT INTO registry_run_assertions (run_id, assertion_id, supersedes_checksum)
SELECT @run_id, a.id, @supersedes_checksum FROM registry_assertions a
WHERE a.source = 'station-prep' AND a.source_key = @source_key AND a.checksum = @checksum
ON CONFLICT (run_id, assertion_id) DO NOTHING
RETURNING assertion_id;

-- name: GetPreparedMembership :one
SELECT m.assertion_id FROM registry_run_assertions m
JOIN registry_assertions a ON a.id=m.assertion_id
WHERE m.run_id = @run_id AND a.source='station-prep' AND a.source_key = @source_key
 AND a.checksum = @checksum AND m.supersedes_checksum = @supersedes_checksum;

-- name: PreparedDanglingSupersedes :one
SELECT count(*) FROM registry_run_assertions m
JOIN registry_assertions older ON older.id = m.assertion_id
WHERE m.run_id = @run_id AND m.supersedes_checksum <> '' AND NOT EXISTS (
 SELECT 1 FROM registry_run_assertions newer_membership
 JOIN registry_assertions newer ON newer.id = newer_membership.assertion_id
 WHERE newer_membership.run_id = m.run_id AND newer.source_key = older.source_key
   AND newer.checksum = m.supersedes_checksum
   AND newer.id <> older.id
   AND (older.superseded_by IS NULL OR older.superseded_by = newer.id)
);

-- name: LinkPreparedSupersedes :execrows
UPDATE registry_assertions older SET superseded_by = newer.id
FROM registry_run_assertions m, registry_assertions newer, registry_run_assertions n
WHERE m.run_id = @run_id AND m.assertion_id = older.id
  AND m.supersedes_checksum <> '' AND newer.checksum = m.supersedes_checksum
  AND newer.source_key = older.source_key AND n.run_id = m.run_id AND n.assertion_id = newer.id
  AND older.superseded_by IS NULL AND older.id <> newer.id;

-- name: GetPreparedRunBinding :one
SELECT prepared_manifest_sha256 FROM registry_source_runs
WHERE id = @id AND source = 'station-prep' AND state = 'complete';

-- name: ListPreparedRegistryPage :many
SELECT a.id, a.source_key, a.display_name, a.address, a.municipality_code, a.state
FROM registry_assertions a JOIN registry_run_assertions m ON m.assertion_id = a.id
WHERE m.run_id = @run_id AND a.id > @after_id
  AND a.municipality_code IS NOT NULL AND a.state IS NOT NULL
  AND a.source = 'station-prep' AND a.latitude IS NULL AND a.longitude IS NULL
  AND a.superseded_by IS NULL AND (a.station_id IS NULL OR EXISTS (
 SELECT 1 FROM directory_stations s WHERE s.id=a.station_id AND
 (s.display_name IS DISTINCT FROM a.display_name OR s.address IS DISTINCT FROM a.address
 OR s.municipality_code IS DISTINCT FROM a.municipality_code OR s.state IS DISTINCT FROM a.state)))
ORDER BY a.id LIMIT 100;

-- name: SetInitialRegistryLocality :one
UPDATE directory_stations
SET municipality_code = COALESCE(municipality_code, @municipality_code),
    state = COALESCE(state, @state)
WHERE id = @id
  AND (municipality_code IS NULL OR municipality_code = @municipality_code)
  AND (state IS NULL OR state = @state)
RETURNING id;

-- name: CountPreparedPublicationRows :one
SELECT count(*) FROM registry_assertions a JOIN registry_run_assertions m ON m.assertion_id=a.id
WHERE m.run_id = @run_id AND a.source='station-prep' AND a.municipality_code IS NOT NULL
  AND a.state IS NOT NULL AND a.latitude IS NULL AND a.longitude IS NULL AND a.superseded_by IS NULL;

-- name: CountUnpublishedPreparedRows :one
SELECT count(*) FROM registry_assertions a JOIN registry_run_assertions m ON m.assertion_id=a.id
WHERE m.run_id = @run_id AND a.source='station-prep' AND a.municipality_code IS NOT NULL
  AND a.state IS NOT NULL AND a.latitude IS NULL AND a.longitude IS NULL
  AND a.superseded_by IS NULL AND (a.station_id IS NULL OR EXISTS (
 SELECT 1 FROM directory_stations s WHERE s.id=a.station_id AND
 (s.display_name IS DISTINCT FROM a.display_name OR s.address IS DISTINCT FROM a.address
 OR s.municipality_code IS DISTINCT FROM a.municipality_code OR s.state IS DISTINCT FROM a.state)));

-- name: RefreshPreparedStationFacts :one
WITH refreshed AS (
 UPDATE directory_stations AS s
 SET display_name= @display_name, address= @address::jsonb
 WHERE s.id= @id AND (s.display_name IS DISTINCT FROM @display_name OR s.address IS DISTINCT FROM @address::jsonb)
   AND EXISTS (
    SELECT 1 FROM registry_assertions AS previous JOIN registry_source_runs AS r ON r.id=previous.run_id
    WHERE previous.station_id=s.id AND previous.source='station-prep'
      AND previous.display_name=s.display_name AND previous.address=s.address
      AND r.state='complete' AND r.prepared_manifest_sha256 IS NOT NULL
   )
 RETURNING s.id
)
SELECT id FROM refreshed
UNION ALL SELECT id FROM directory_stations WHERE id= @id AND display_name= @display_name AND address= @address::jsonb
LIMIT 1;
