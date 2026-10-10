//go:build integration

package registry

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	dbmigrations "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/migrations"
	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/platform/migrate"
)

func mustFixture(t *testing.T, name string) io.Reader {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller unavailable")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "contracts", "testdata", "registry", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return strings.NewReader(string(raw))
}

func truncatedFixture() io.Reader {
	return strings.NewReader("CNPJ;RAZAO_SOCIAL;COD_IBGE;UF;SITUACAO\n\"unterminated")
}

func emptyFixture() io.Reader {
	return strings.NewReader("CNPJ;RAZAO_SOCIAL;COD_IBGE;UF;SITUACAO\n")
}

// cleanCSV stages completely: no unknown columns, 4 valid rows
// (numeric, alphanumeric and leading-zero CNPJs), 1 duplicate, 1
// rejected (short CNPJ).
const cleanCSV = `CNPJ;RAZAO_SOCIAL;COD_IBGE;UF;SITUACAO;ATO_AUTORIZACAO
04218406000104;[P25-TEST] ALFA;3550308;SP;ATIVA;PRC-1
12ABC34501DE35;[P25-TEST] GAMA;3550308;SP;DESCONHECIDA;
00428184000195;[P25-TEST] ZERO;3550308;SP;ATIVA;PRC-3
04218406000104;[P25-TEST] ALFA;3550308;SP;ATIVA;PRC-1
123;[P25-TEST] CURTO;3550308;SP;ATIVA;
`

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ANPFUEL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://anpfuel:anpfuel@127.0.0.1:5434/anpfuel?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("integration database unreachable: %v", err)
	}
	defer conn.Close(ctx)
	return dsn
}

// freshStore migrates an empty disposable database (proving the
// append-only 000031 upgrade on a previous schema) and returns a
// staging store on it. The database is dropped during cleanup.
func freshStore(t *testing.T) *PGStore {
	t.Helper()
	store, _ := freshStoreWithDSN(t)
	return store
}

// freshStoreWithDSN is freshStore plus the disposable database DSN for
// tests that need raw connections (least-privilege role setup).
func freshStoreWithDSN(t *testing.T) (*PGStore, string) {
	return freshStoreWithMigrations(t, dbmigrations.Files)
}

func freshStoreWithMigrations(t *testing.T, files fs.FS) (*PGStore, string) {
	t.Helper()
	adminDSN := testDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	name := fmt.Sprintf("registry_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			t.Errorf("admin connect for drop: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Errorf("drop database: %v", err)
		}
	})
	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.Path = "/" + name
	if _, err := migrate.Apply(ctx, parsed.String(), files); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &PGStore{Q: directory.New(pool)}, parsed.String()
}

func countAssertions(t *testing.T, store *PGStore, runID string) int64 {
	t.Helper()
	uid, err := mustUUID(runID)
	if err != nil {
		t.Fatalf("run id: %v", err)
	}
	n, err := store.Q.CountRegistryAssertions(context.Background(), uid)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestStageCSVIntegrationReplayAndConcurrency(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	limits := Limits{MaxBytes: 1 << 20, MaxRows: 1000, BatchSize: 2}

	first, err := StageCSV(ctx, store, "snap-int-1", strings.NewReader(cleanCSV), limits)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if first.State != "complete" {
		t.Fatalf("state = %q", first.State)
	}
	// 5 rows — 3 accepted (alfa, gama-alnum, zero-leading), 1
	// duplicate inside the batch (alfa row 4), 1 rejected (short CNPJ).
	if first.Accepted != 3 || first.Duplicates != 1 || first.Rejected != 1 {
		t.Fatalf("counts = %+v", first)
	}

	second, err := StageCSV(ctx, store, "snap-int-1", strings.NewReader(cleanCSV), limits)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if second.RunID != first.RunID || second.Accepted != first.Accepted {
		t.Fatalf("replay diverged: %+v vs %+v", first, second)
	}
	if got := countAssertions(t, store, first.RunID); got != 3 {
		t.Fatalf("assertions = %d, want 3", got)
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = StageCSV(ctx, store, "snap-int-1", strings.NewReader(cleanCSV), limits)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent %d: %v", i, err)
		}
	}
	if got := countAssertions(t, store, first.RunID); got != 3 {
		t.Fatalf("assertions after concurrency = %d, want 3", got)
	}
}

func TestStageCSVIntegrationManifestSampleQuarantines(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	limits := Limits{MaxBytes: 1 << 20, MaxRows: 1000, BatchSize: 10}

	// The T01 manifest sample intentionally carries COLUNA_DESCONHECIDA:
	// staging must quarantine it end to end with zero assertions.
	report, err := StageCSV(ctx, store, "snap-manifest", mustFixture(t, "registry-csv-sample.csv"), limits)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if report.State != "quarantined" || report.ErrorCode != "unknown_columns" {
		t.Fatalf("report = %+v", report)
	}
	if got := countAssertions(t, store, report.RunID); got != 0 {
		t.Fatalf("assertions = %d, want 0", got)
	}
}

func TestStageCSVIntegrationTruncatedPreservesLastGood(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	limits := Limits{MaxBytes: 1 << 20, MaxRows: 1000, BatchSize: 10}

	good, err := StageCSV(ctx, store, "snap-good", strings.NewReader(cleanCSV), limits)
	if err != nil || good.State != "complete" {
		t.Fatalf("good = %+v, err = %v", good, err)
	}
	bad, err := StageCSV(ctx, store, "snap-bad", truncatedFixture(), limits)
	if err != nil {
		t.Fatalf("truncated stage: %v", err)
	}
	if bad.State == "complete" {
		t.Fatal("truncated snapshot must not complete")
	}
	n, err := store.Q.CountCompleteRegistryRuns(ctx, SourceCSV)
	if err != nil {
		t.Fatalf("count complete: %v", err)
	}
	if n != 1 {
		t.Fatalf("complete runs = %d, want 1 (last good preserved)", n)
	}
}

func TestStageCSVIntegrationEmptySnapshotQuarantines(t *testing.T) {
	store := freshStore(t)
	ctx := context.Background()
	limits := Limits{MaxBytes: 1 << 20, MaxRows: 1000, BatchSize: 10}

	report, err := StageCSV(ctx, store, "snap-empty", emptyFixture(), limits)
	if err != nil {
		t.Fatalf("empty stage: %v", err)
	}
	if report.State != "quarantined" {
		t.Fatalf("empty state = %q", report.State)
	}
	n, err := store.Q.CountCompleteRegistryRuns(ctx, SourceCSV)
	if err != nil {
		t.Fatalf("count complete: %v", err)
	}
	if n != 0 {
		t.Fatalf("complete runs = %d, want 0", n)
	}
}
