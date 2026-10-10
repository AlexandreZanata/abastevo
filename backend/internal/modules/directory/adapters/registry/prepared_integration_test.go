//go:build integration

package registry

import (
	"bytes"
	"context"
	"encoding/json"
	dbmigrations "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/migrations"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/platform/migrate"
	"io"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
)

func preparedOpener(streams BatchStreams) OpenPreparedStream {
	return func(name string) (io.ReadCloser, error) {
		raw := map[string][]byte{PrepAssertionsFile: streams.Assertions, PrepCandidatesFile: streams.Candidates, PrepQuarantineFile: streams.Quarantine}[name]
		return io.NopCloser(bytes.NewReader(raw)), nil
	}
}

func preparedPool(t *testing.T) (*pgxpool.Pool, *PGStore) {
	t.Helper()
	store, dsn := freshStoreWithDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, store
}

func TestPreparedUnchangedDeltaAndConcurrentReplay(t *testing.T) {
	pool, store := preparedPool(t)
	manifest, streams := prepIntegrationBatch(t)
	first, err := LoadPreparedBatch(context.Background(), pool, manifest, preparedOpener(streams))
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.ListAssertions(context.Background(), first["registry-13col"].RunID)
	if err != nil {
		t.Fatal(err)
	}
	var decoded BatchManifest
	if err := json.Unmarshal(manifest, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.RunID = "44444444-2222-4333-8444-555555555555"
	delta, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reports, err := LoadPreparedBatch(context.Background(), pool, delta, preparedOpener(streams))
			if err != nil {
				t.Error(err)
				return
			}
			if reports["registry-13col"].Accepted != 3 || reports["pmqc"].Accepted != 2 {
				t.Errorf("delta accounting: %+v", reports)
			}
		}()
	}
	wg.Wait()
	run, err := store.GetRun(context.Background(), SourcePrep, "station-prep:"+decoded.RunID+":registry-13col")
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.ListAssertions(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 3 || len(after) != 3 {
		t.Fatalf("membership counts %d/%d", len(before), len(after))
	}
	for i := range before {
		if before[i].ID != after[i].ID {
			t.Fatal("unchanged delta replaced assertion identity")
		}
	}
}

func TestPreparedRecoversLegacyPartialRunWithoutDeletion(t *testing.T) {
	pool, store := preparedPool(t)
	manifest, streams := prepIntegrationBatch(t)
	batch, decoded, err := validateBatch(manifest, streams)
	if err != nil {
		t.Fatal(err)
	}
	id := newUUID()
	input := decoded.Inputs[0]
	_, _, err = store.CreateRun(context.Background(), id, SourcePrep, "station-prep:"+decoded.RunID+":"+input.Key, input.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.StageAssertion(context.Background(), batch.assertions[input.Key][0].WithRun(id)); err != nil {
		t.Fatal(err)
	}
	existing, err := store.ListAssertions(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.FinishRun(context.Background(), id, "failed", 1, 0, 0, "stage_error"); err != nil {
		t.Fatal(err)
	}
	reports, err := LoadPreparedBatch(context.Background(), pool, manifest, preparedOpener(streams))
	if err != nil {
		t.Fatal(err)
	}
	if reports[input.Key].RunID != id || reports[input.Key].State != "complete" {
		t.Fatalf("recovery changed run: %+v", reports)
	}
	after, err := store.ListAssertions(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range after {
		if a.ID == existing[0].ID {
			found = true
		}
	}
	if !found {
		t.Fatal("recovery deleted/replaced accepted assertion")
	}
}

func TestPreparedAtomicFailureAndManifestBinding(t *testing.T) {
	for _, mode := range []string{"malformed-tail", "checksum", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			pool, store := preparedPool(t)
			manifest, streams := prepIntegrationBatch(t)
			var decoded BatchManifest
			if err := json.Unmarshal(manifest, &decoded); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "malformed-tail":
				streams.Quarantine = append(streams.Quarantine, []byte("{\"schema_version\":\"bad\"}\n")...)
				decoded.Outputs[2] = streamOutput(PrepQuarantineFile, streams.Quarantine)
				manifest, _ = json.Marshal(decoded)
			case "checksum":
				streams.Quarantine = append(streams.Quarantine, 'x')
			}
			open := preparedOpener(streams)
			if mode == "cancel" {
				base := open
				open = func(name string) (io.ReadCloser, error) {
					if name == PrepQuarantineFile {
						cancel()
					}
					return base(name)
				}
			}
			if _, err := LoadPreparedBatch(ctx, pool, manifest, open); err == nil {
				t.Fatal("bad/cancelled load completed")
			}
			var runs, assertions int64
			if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM registry_source_runs").Scan(&runs); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM registry_assertions").Scan(&assertions); err != nil {
				t.Fatal(err)
			}
			if runs != 0 || assertions != 0 {
				t.Fatalf("transaction leaked runs=%d assertions=%d", runs, assertions)
			}
			goodManifest, goodStreams := prepIntegrationBatch(t)
			first, err := LoadPreparedBatch(context.Background(), pool, goodManifest, preparedOpener(goodStreams))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(goodManifest, &decoded); err != nil {
				t.Fatal(err)
			}
			decoded.Inputs[0].Edition = "changed-under-same-run"
			changed, _ := json.Marshal(decoded)
			if _, err := LoadPreparedBatch(context.Background(), pool, changed, preparedOpener(goodStreams)); err == nil {
				t.Fatal("completed manifest rebound")
			}
			run, err := store.GetRun(context.Background(), SourcePrep, "station-prep:"+decoded.RunID+":registry-13col")
			if err != nil {
				t.Fatal(err)
			}
			if run.State != "complete" || run.RunID != first["registry-13col"].RunID {
				t.Fatal("failed replay changed complete run")
			}
		})
	}
}

func TestPreparedMigrationUpgradeFailureAndRecovery(t *testing.T) {
	files := fstest.MapFS{}
	entries, err := fs.ReadDir(dbmigrations.Files, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() < "000053" {
			raw, err := fs.ReadFile(dbmigrations.Files, entry.Name())
			if err != nil {
				t.Fatal(err)
			}
			files[entry.Name()] = &fstest.MapFile{Data: raw}
		}
	}
	store, dsn := freshStoreWithMigrations(t, files)
	ctx := context.Background()
	id := newUUID()
	if _, _, err := store.CreateRun(ctx, id, SourcePrep, "upgrade-retained", "hash-retained"); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	files["000053_failed_upgrade.sql"] = &fstest.MapFile{Data: []byte("ALTER TABLE registry_source_runs ADD COLUMN prepared_manifest_sha256 TEXT; SELECT 1/0;")}
	if _, err := migrate.Apply(ctx, dsn, files); err == nil {
		t.Fatal("faulty upgrade succeeded")
	}
	var columns int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_name='registry_source_runs' AND column_name='prepared_manifest_sha256'").Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 0 {
		t.Fatal("failed migration leaked partial DDL")
	}
	applied, err := migrate.Apply(ctx, dsn, dbmigrations.Files)
	if err != nil || len(applied) != 1 || applied[0] != "000053" {
		t.Fatalf("upgrade %v %v", applied, err)
	}
	run, err := store.GetRun(ctx, SourcePrep, "upgrade-retained")
	if err != nil || run.RunID != id || run.State != "running" {
		t.Fatalf("upgrade changed existing run %+v %v", run, err)
	}
	applied, err = migrate.Apply(ctx, dsn, dbmigrations.Files)
	if err != nil || len(applied) != 0 {
		t.Fatalf("migration replay %v %v", applied, err)
	}
}

func TestPreparedDatabaseDisconnectRollsBackAndRetryConverges(t *testing.T) {
	pool, _ := preparedPool(t)
	manifest, streams := prepIntegrationBatch(t)
	ctx := context.Background()
	base := preparedOpener(streams)
	open := func(name string) (io.ReadCloser, error) {
		if name == PrepCandidatesFile {
			var pid int
			// Only a backend in this uniquely owned disposable database is eligible.
			if err := pool.QueryRow(ctx, "SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='idle in transaction'").Scan(&pid); err != nil {
				return nil, err
			}
			var killed bool
			if err := pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&killed); err != nil {
				return nil, err
			}
			if !killed {
				t.Error("owned test transaction not terminated")
			}
		}
		return base(name)
	}
	if _, err := LoadPreparedBatch(ctx, pool, manifest, open); err == nil {
		t.Fatal("disconnected transaction completed")
	}
	var assertions int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM registry_assertions").Scan(&assertions); err != nil {
		t.Fatal(err)
	}
	if assertions != 0 {
		t.Fatalf("disconnected transaction leaked %d assertions", assertions)
	}
	for range 2 {
		reports, err := LoadPreparedBatch(ctx, pool, manifest, base)
		if err != nil || reports["registry-13col"].Accepted != 3 || reports["pmqc"].Accepted != 2 {
			t.Fatalf("recovery %+v %v", reports, err)
		}
	}
}
