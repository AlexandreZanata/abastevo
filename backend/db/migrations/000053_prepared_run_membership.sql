-- Prepared deltas reference immutable assertions without moving their original
-- provenance. No historical assertion or source run is deleted/backfilled.
ALTER TABLE registry_source_runs ADD COLUMN prepared_manifest_sha256 TEXT NOT NULL DEFAULT '';
ALTER TABLE registry_source_runs ADD CONSTRAINT registry_prepared_manifest_sha256_check
    CHECK (prepared_manifest_sha256 = '' OR prepared_manifest_sha256 ~ '^[0-9a-f]{64}$');
CREATE TABLE registry_run_assertions (
    run_id UUID NOT NULL REFERENCES registry_source_runs(id) ON DELETE RESTRICT,
    assertion_id UUID NOT NULL REFERENCES registry_assertions(id) ON DELETE RESTRICT,
    supersedes_checksum TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, assertion_id),
    CHECK (supersedes_checksum = '' OR supersedes_checksum ~ '^[0-9a-f]{64}$')
);
CREATE INDEX registry_run_assertions_assertion_idx ON registry_run_assertions(assertion_id);
