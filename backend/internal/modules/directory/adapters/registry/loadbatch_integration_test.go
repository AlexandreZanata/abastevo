//go:build integration

package registry

// RST-05 loader integration: bounded batches through the existing staging
// ownership on real disposable PostGIS. Every vector is synthetic with
// owned [RST05-TEST] markers. Credentials flow only through the existing
// ANPFUEL_TEST_DATABASE_URL mechanism; no new secret enters the tree.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
)

func jsonMarshal(value any) ([]byte, error) { return json.Marshal(value) }

func jsonUnmarshal(raw []byte, value any) error { return json.Unmarshal(raw, value) }

func prepIntegrationBatch(t *testing.T) ([]byte, BatchStreams) {
	t.Helper()
	streams := prepStreams(t)
	return prepManifest(t, streams), streams
}

func runState(t *testing.T, store *PGStore, source, snapshot string) Report {
	t.Helper()
	report, err := store.GetRun(context.Background(), source, snapshot)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	return report
}

func TestLoadBatchIntegrationEmptyBatchCompletes(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	empty := BatchStreams{}
	manifest := BatchManifest{
		FormatVersion: FormatBatch,
		RunID:         "22222222-2222-4333-8444-555555555555",
		ParserVersion: "station-prep-v0.2.0",
		PolicyVersion: "station-policy-v1",
		Inputs: []BatchInput{
			{Key: "registry-13col", Reference: "empty.csv", Edition: "synthetic-empty", EditionSeq: 1, SHA256: vectorChecksum("raw-empty"), Bytes: 0, Rows: 0},
		},
		Counts: map[string]BatchCounts{
			"registry-13col": {},
		},
		Outputs: []BatchOutput{
			streamOutput(PrepAssertionsFile, empty.Assertions),
			streamOutput(PrepCandidatesFile, empty.Candidates),
			streamOutput(PrepQuarantineFile, empty.Quarantine),
		},
	}
	manifest.MunicipalityReference.Reference = "ibge-localidades-v1"
	manifest.MunicipalityReference.ReferenceHash = vectorChecksum("aliases")
	manifest.Completeness.EOFValidated = true
	manifest.Completeness.ExpectedManifest = true
	manifestJSON, err := jsonMarshal(manifest)
	if err != nil {
		t.Fatal(err)
	}

	reports, err := LoadBatch(ctx, store, manifestJSON, empty)
	if err != nil {
		t.Fatalf("empty load: %v", err)
	}
	report := reports["registry-13col"]
	if report.State != "complete" {
		t.Fatalf("empty state = %q", report.State)
	}
	if got := countAssertions(t, store, report.RunID); got != 0 {
		t.Fatalf("assertions = %d, want 0", got)
	}
}

func TestLoadBatchIntegrationUpgradeConstraint(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()

	// Pre-migration sources keep staging after the 000049 upgrade.
	if _, err := StageCSV(ctx, store, "snap-csv-after-upgrade", strings.NewReader(cleanCSV), DefaultLimits()); err != nil {
		t.Fatalf("csv after upgrade: %v", err)
	}
	manifestJSON, streams := prepIntegrationBatch(t)
	reports, err := LoadBatch(ctx, store, manifestJSON, streams)
	if err != nil {
		t.Fatalf("prep load: %v", err)
	}
	if reports["registry-13col"].State != "complete" || reports["pmqc"].State != "complete" {
		t.Fatalf("reports = %+v", reports)
	}
	// Anything outside the extended enum still fails at the database.
	if _, _, err := store.CreateRun(ctx, newUUID(), "invented-source", "snap-bogus", "x"); err == nil {
		t.Fatal("bogus source accepted, want CHECK violation")
	}
}

func TestLoadBatchIntegrationRestartAndConcurrency(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	manifestJSON, streams := prepIntegrationBatch(t)

	first, err := LoadBatch(ctx, store, manifestJSON, streams)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	second := make([]map[string]Report, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			second[i], errs[i] = LoadBatch(ctx, store, manifestJSON, streams)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent %d: %v", i, err)
		}
		if second[i]["registry-13col"].RunID != first["registry-13col"].RunID {
			t.Fatalf("concurrent %d diverged run id", i)
		}
	}
	if got := countAssertions(t, store, first["registry-13col"].RunID); got != 3 {
		t.Fatalf("registry assertions after concurrency = %d, want 3", got)
	}
	if got := countAssertions(t, store, first["pmqc"].RunID); got != 2 {
		t.Fatalf("pmqc assertions after concurrency = %d, want 2", got)
	}
}

func TestLoadBatchIntegrationTamperedStreamRefused(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	manifestJSON, streams := prepIntegrationBatch(t)
	streams.Assertions[20] ^= 0xFF

	if _, err := LoadBatch(ctx, store, manifestJSON, streams); err == nil {
		t.Fatal("tampered batch loaded, want refusal")
	}
	n, err := store.Q.CountCompleteRegistryRuns(ctx, SourcePrep)
	if err != nil {
		t.Fatalf("count complete: %v", err)
	}
	if n != 0 {
		t.Fatalf("complete prep runs = %d, want 0", n)
	}
}

func TestLoadBatchIntegrationRetainedIDsAndNoPartialPublication(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	manifestJSON, streams := prepIntegrationBatch(t)

	first, err := LoadBatch(ctx, store, manifestJSON, streams)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	ids := func(runID string) map[string]string {
		t.Helper()
		uid, err := mustUUID(runID)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := store.Q.ListRegistryAssertions(ctx, uid)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, row := range rows {
			out[row.Source+"|"+row.SourceKey+"|"+row.Checksum] = uuidString(row.ID)
		}
		return out
	}
	before := ids(first["registry-13col"].RunID)

	second, err := LoadBatch(ctx, store, manifestJSON, streams)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if second["registry-13col"].RunID != first["registry-13col"].RunID {
		t.Fatal("replay changed the stable run id")
	}
	after := ids(second["registry-13col"].RunID)
	if len(before) != 3 || len(after) != 3 {
		t.Fatalf("assertion sets = %d/%d, want 3/3", len(before), len(after))
	}
	for key, id := range before {
		if after[key] != id {
			t.Fatalf("assertion identity moved for %q", key)
		}
	}

	// A store failure mid-load finishes the run as failed: nothing new
	// completes and the reconciler stays blind to the partial run.
	failing := &failAfterStore{Store: store, limit: 1}
	manifestJSON2, streams2 := prepIntegrationBatch(t)
	var decoded BatchManifest
	if err := jsonUnmarshal(manifestJSON2, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.RunID = "33333333-3333-4333-8444-555555555555"
	manifestJSON2, err = jsonMarshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBatch(ctx, failing, manifestJSON2, streams2); err == nil {
		t.Fatal("failing load succeeded, want error")
	}
	n, err := store.Q.CountCompleteRegistryRuns(ctx, SourcePrep)
	if err != nil {
		t.Fatalf("count complete: %v", err)
	}
	if n != 2 {
		t.Fatalf("complete prep runs = %d, want 2 (both inputs of the good batch)", n)
	}
}

// failAfterStore fails staging after a bounded number of rows.
type failAfterStore struct {
	Store
	limit int
	count int
}

func (s *failAfterStore) StageAssertion(ctx context.Context, assertion Assertion) (bool, error) {
	s.count++
	if s.count > s.limit {
		return false, errTest
	}
	return s.Store.StageAssertion(ctx, assertion)
}

func TestLoadBatchIntegrationRestrictedRole(t *testing.T) {
	_, dsn := freshStoreWithDSN(t)
	ctx := context.Background()

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	// Close after the DROP ROLE cleanup below (LIFO): the drop runs first.
	t.Cleanup(func() {
		admin.Close(context.Background())
	})
	role := fmt.Sprintf("loader_test_%d", time.Now().UnixNano())
	password := "test-only-pw"
	if _, err := admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" LOGIN PASSWORD '"+password+"'"); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Grants reference the role: revoke the footprint before dropping.
		if _, err := admin.Exec(ctx, "DROP OWNED BY "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Errorf("drop owned: %v", err)
			return
		}
		if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Errorf("drop role: %v", err)
		}
	})
	database := strings.TrimPrefix(parsed.Path, "/")
	for _, grant := range []string{
		"GRANT CONNECT, TEMPORARY ON DATABASE " + pgx.Identifier{database}.Sanitize() + " TO " + pgx.Identifier{role}.Sanitize(),
		"GRANT USAGE ON SCHEMA public TO " + pgx.Identifier{role}.Sanitize(),
		"GRANT SELECT, INSERT, UPDATE ON registry_source_runs, registry_assertions, registry_run_assertions TO " + pgx.Identifier{role}.Sanitize(),
	} {
		if _, err := admin.Exec(ctx, grant); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}
	parsed.User = url.UserPassword(role, password)
	rolePool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("role pool: %v", err)
	}
	defer rolePool.Close()
	roleStore := &PGStore{Q: directory.New(rolePool)}

	manifestJSON, streams := prepIntegrationBatch(t)
	reports, err := LoadBatch(ctx, roleStore, manifestJSON, streams)
	if err != nil {
		t.Fatalf("restricted load: %v", err)
	}
	if reports["registry-13col"].State != "complete" {
		t.Fatalf("restricted report = %+v", reports["registry-13col"])
	}
	if _, err := LoadPreparedBatch(ctx, rolePool, manifestJSON, preparedOpener(streams)); err != nil {
		t.Fatalf("restricted operational load: %v", err)
	}
	if _, err := rolePool.Exec(ctx, "CREATE TABLE public.forbidden_loader_ddl(id int)"); err == nil {
		t.Fatal("restricted role gained persistent DDL")
	}
	// The restricted role cannot reach canonical tables: the Store
	// boundary is staging-only.
	if _, err := rolePool.Exec(ctx, "INSERT INTO directory_stations (id, display_name) VALUES (gen_random_uuid(), 'x')"); err == nil {
		t.Fatal("restricted role wrote canonical stations, want permission denied")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unexpected canonical write error: %v", err)
	}
}

// TestRegistryRunSourceEnumCoversLineage pins the full CHECK lineage
// (000031/000033/000049/000050/000052): every historical source stages,
// anything else violates the constraint. Added after 000049 dropped the
// DOU values by evolving from the creating migration instead of the
// latest definition.
func TestRegistryRunSourceEnumCoversLineage(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	for _, source := range []string{"registry-csv", "registry-api", "dou-editions", "dou-acts", "station-prep", "review"} {
		if _, created, err := store.CreateRun(ctx, newUUID(), source, "snap-enum-"+source, "x"); err != nil || !created {
			t.Fatalf("source %q: created=%v err=%v", source, created, err)
		}
	}
	if _, _, err := store.CreateRun(ctx, newUUID(), "invented-source", "snap-bogus", "x"); err == nil {
		t.Fatal("bogus source accepted, want CHECK violation")
	}
}
