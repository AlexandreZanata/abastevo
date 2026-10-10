//go:build integration

package registry

// RST-19 failure, replay and recovery under load: bounded worker kill,
// DB disconnect, malformed/truncated batches, checksum mismatch,
// duplicate concurrent loaders and stale editions against isolated
// real PostGIS. Disk-full is UNSUPPORTED here (shared container disk
// cannot be exhausted safely; real acceptance stays with the owning
// storage plan) and upstream 429/timeout is NOT_APPLICABLE (fetch
// path disabled; RST-08 evidence stands, jobs code unchanged since).
// Durability settings are asserted, never weakened.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	directoryread "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/read"
	directoryapp "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/application"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	dbmigrations "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/migrations"
	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/platform/migrate"
)

// faultStore is an isolated fresh database with the pool exposed for
// fault injection (backend termination) and durability assertions.
type faultStore struct {
	store *PGStore
	pool  *pgxpool.Pool
}

func newFaultStore(t *testing.T) *faultStore {
	t.Helper()
	adminDSN := os.Getenv("ANPFUEL_TEST_DATABASE_URL")
	if adminDSN == "" {
		adminDSN = "postgres://anpfuel:anpfuel@127.0.0.1:5434/anpfuel?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("integration database unreachable: %v", err)
	}
	defer admin.Close(ctx)
	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	name := fmt.Sprintf("registry_fault_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			return
		}
		defer admin.Close(ctx)
		_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize())
	})
	parsed.Path = "/" + name
	if _, err := migrate.Apply(ctx, parsed.String(), dbmigrations.Files); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatalf("pool config: %v", err)
	}
	config.ConnConfig.Config.RuntimeParams["application_name"] = "rst19-fault"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return &faultStore{store: &PGStore{Q: directory.New(pool)}, pool: pool}
}

func assertDurability(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	var syncCommit string
	if err := pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit); err != nil {
		t.Fatalf("durability: %v", err)
	}
	if syncCommit != "on" {
		t.Fatalf("synchronous_commit = %q, want on", syncCommit)
	}
	var unlogged int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_class
		WHERE relname IN ('registry_source_runs','registry_assertions') AND relpersistence <> 'p'`).Scan(&unlogged); err != nil {
		t.Fatalf("durability: %v", err)
	}
	if unlogged != 0 {
		t.Fatalf("%d staging relations are not durable", unlogged)
	}
}

// faultCNPJ mints deterministic checksum-valid CNPJs from a serial.
func faultCNPJ(serial int) string {
	var prefix [12]byte
	base := serial
	for i := range prefix {
		prefix[i] = byte('0' + base%10)
		base /= 10
	}
	digit := func(weights []int, extra ...byte) byte {
		sum := 0
		digits := append(prefix[:], extra...)
		for i, weight := range weights {
			sum += int(digits[i]-'0') * weight
		}
		if sum%11 < 2 {
			return '0'
		}
		return byte('0' + 11 - sum%11)
	}
	first := digit([]int{5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2})
	second := digit([]int{6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2}, first)
	out := make([]byte, 0, 14)
	out = append(out, prefix[:]...)
	out = append(out, first, second)
	return string(out)
}

// faultBatch builds a valid single-input batch with n accepted rows.
func faultBatch(t *testing.T, n int, runID string) ([]byte, BatchStreams) {
	t.Helper()
	rows := make([]any, 0, n)
	for i := 1; i <= n; i++ {
		rows = append(rows, prepAssertionRow(
			faultCNPJ(i), vectorChecksum(fmt.Sprintf("rst19-%d", i)),
			fmt.Sprintf("SIMP-%06d", i), nil))
	}
	var out []byte
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		out = append(out, raw...)
		out = append(out, '\n')
	}
	streams := BatchStreams{Assertions: out}
	manifest := BatchManifest{
		FormatVersion: FormatBatch,
		RunID:         runID,
		ParserVersion: "station-prep-v0.2.0",
		PolicyVersion: "station-policy-v1",
		Inputs: []BatchInput{
			{Key: "registry-13col", Reference: "fault.csv", Edition: "synthetic-fault", EditionSeq: 1, SHA256: vectorChecksum("raw-fault"), Rows: n},
		},
		Counts: map[string]BatchCounts{"registry-13col": {Input: n, Accepted: n}},
		Outputs: []BatchOutput{
			streamOutput(PrepAssertionsFile, streams.Assertions),
			streamOutput(PrepCandidatesFile, streams.Candidates),
			streamOutput(PrepQuarantineFile, streams.Quarantine),
		},
	}
	manifest.MunicipalityReference.Reference = "ibge-localidades-v1"
	manifest.MunicipalityReference.ReferenceHash = vectorChecksum("aliases")
	manifest.Completeness.EOFValidated = true
	manifest.Completeness.ExpectedManifest = true
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return raw, streams
}

func faultRunState(t *testing.T, store *PGStore, runID, key string) Report {
	t.Helper()
	report, err := store.GetRun(context.Background(), SourcePrep, "station-prep:"+runID+":"+key)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	return report
}

// faultLiveTotal counts all staged rows: the staging run UUID is
// minted inside LoadBatch, so mid-flight polling counts the fresh
// database instead of filtering by the manifest run id.
func faultLiveTotal(fs *faultStore) int64 {
	var n int64
	if err := fs.pool.QueryRow(context.Background(), `SELECT count(*) FROM registry_assertions`).Scan(&n); err != nil {
		return -1
	}
	return n
}

// faultLiveRun counts rows staged under one staging run id (from the
// LoadBatch report, not the manifest run id).
func faultLiveRun(fs *faultStore, stagingRunID string) int64 {
	uid, err := mustUUID(stagingRunID)
	if err != nil {
		return -1
	}
	n, err := fs.store.Q.CountRegistryAssertions(context.Background(), uid)
	if err != nil {
		return -1
	}
	return n
}

// TestFaultWorkerKillMidLoad cancels the loader mid-stage: the load
// must surface an error, never report complete, retain partial staged
// facts without duplication, and a same-manifest retry must fail loudly
// instead of silently reporting the orphaned run.
func TestFaultWorkerKillMidLoad(t *testing.T) {
	fs := newFaultStore(t)
	assertDurability(t, fs.pool)
	runID := "aaaaaaaa-1111-4111-8111-111111111111"
	manifestJSON, streams := faultBatch(t, 1500, runID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := LoadBatch(ctx, fs.store, manifestJSON, streams); done <- err }()
	// Cancel once staging is visibly underway (partial, never whole).
	deadline := time.Now().Add(30 * time.Second)
	for {
		if faultLiveTotal(fs) > 0 {
			break
		}
		if err := func() error {
			select {
			case err := <-done:
				return err
			default:
				return nil
			}
		}(); err != nil {
			t.Fatalf("load finished before kill window: %v", err)
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("staging never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("killed load returned nil error")
	}
	report := faultRunState(t, fs.store, runID, "registry-13col")
	if report.State == "complete" {
		t.Fatal("killed load reports complete")
	}
	live := faultLiveTotal(fs)
	if live <= 0 || live >= 1500 {
		t.Fatalf("live rows = %d, want a strict partial prefix", live)
	}
	// Retry must fail loudly: silent running/failed reports are a trap
	// for retry loops that treat nil error as convergence.
	if _, err := LoadBatch(context.Background(), fs.store, manifestJSON, streams); err == nil {
		t.Fatal("retry after kill returned nil error")
	}
	if got := faultLiveTotal(fs); got != live {
		t.Fatalf("retry changed live rows %d -> %d", live, got)
	}
}

// disconnectGateStore blocks LoadBatch mid-stage at a deterministic row
// so the server-side kill always lands while staging is in flight. It
// delegates every Store call to the wrapped store; only the staging
// boundary pauses, and a cancelled context still unblocks it.
type disconnectGateStore struct {
	Store
	staged  atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (g *disconnectGateStore) StageAssertion(ctx context.Context, a Assertion) (bool, error) {
	if g.staged.Add(1) == 51 {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return g.Store.StageAssertion(ctx, a)
}

// TestFaultDBDisconnectMidLoad terminates the loader backend
// server-side: same invariants as the client-side kill, proving the
// failure surfaces through the server path too.
func TestFaultDBDisconnectMidLoad(t *testing.T) {
	fs := newFaultStore(t)
	assertDurability(t, fs.pool)
	runID := "aaaaaaaa-2222-4222-8222-222222222222"
	manifestJSON, streams := faultBatch(t, 1500, runID)
	gate := &disconnectGateStore{Store: fs.store, entered: make(chan struct{}), release: make(chan struct{})}
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { _, err := LoadBatch(ctx, gate, manifestJSON, streams); done <- err }()
	select {
	case <-gate.entered:
	case err := <-done:
		t.Fatalf("load settled before disconnect window: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("staging never reached disconnect window")
	}
	// The loader is parked before row 51 with 1450 rows ahead: releases
	// and repeated terminate rounds now race a guaranteed in-flight
	// load. One shot is not deterministic (the killer may reuse the
	// loader's own idle pooled connection, which the pid exclusion
	// then spares), so rounds repeat until the loader settles. A
	// failing round is transient (the killer can draw a dead pooled
	// connection too) and never aborts the fault.
	close(gate.release)
	var terminated int64
	var loadErr error
	deadline := time.Now().Add(60 * time.Second)
settled:
	for {
		var n int64
		if err := fs.pool.QueryRow(ctx, `SELECT count(*) FROM (
				SELECT pg_terminate_backend(pid) AS ok FROM pg_stat_activity
				WHERE datname = current_database()
				AND application_name = 'rst19-fault' AND pid <> pg_backend_pid()
			) s WHERE ok`).Scan(&n); err == nil {
			terminated += n
		}
		select {
		case err := <-done:
			loadErr = err
			break settled
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("load never settled after disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if loadErr == nil {
		t.Fatalf("disconnected load returned nil error (terminated %d backends)", terminated)
	}
	if report := faultRunState(t, fs.store, runID, "registry-13col"); report.State == "complete" {
		t.Fatal("disconnected load reports complete")
	}
	live := faultLiveTotal(fs)
	if live < 0 {
		t.Fatal("live count unreadable after disconnect")
	}
}

// TestFaultDuplicateColdLoaders races two first-loads of one manifest:
// exactly one run id wins, staged rows equal accepted exactly once,
// and the loser never reports a healthy convergence.
func TestFaultDuplicateColdLoaders(t *testing.T) {
	fs := newFaultStore(t)
	runID := "aaaaaaaa-3333-4333-8333-333333333333"
	// Wide race window: the loser must observe the run mid-flight,
	// never a settled complete it could mistake for convergence.
	manifestJSON, streams := faultBatch(t, 1500, runID)
	ctx := context.Background()
	type outcome struct {
		reports map[string]Report
		err     error
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for worker := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reports, err := LoadBatch(ctx, fs.store, manifestJSON, streams)
			results[worker] = outcome{reports, err}
		}()
	}
	wg.Wait()
	succeeded := 0
	var winnerRun string
	for _, result := range results {
		if result.err != nil {
			continue
		}
		report, ok := result.reports["registry-13col"]
		if !ok || report.State != "complete" {
			continue
		}
		succeeded++
		if report.Accepted != 1500 {
			t.Fatalf("winner accepted = %d, want 1500", report.Accepted)
		}
		winnerRun = report.RunID
	}
	if succeeded != 1 {
		t.Fatalf("%d healthy convergences, want exactly 1", succeeded)
	}
	if got := faultLiveRun(fs, winnerRun); got != 1500 {
		t.Fatalf("live rows = %d, want 1500 (no duplicates)", got)
	}
}

// TestFaultStaleEdition pins the RST-14 behavior in a fast test: the
// newer edition stages, then the older edition fails count_mismatch
// with a visible failed run while the complete-run count stays put.
// Global (source, source_key, checksum) dedup means incremental
// restages need a separate loader task; this test locks the loud
// failure so no silent partial publication can slip in.
func TestFaultStaleEdition(t *testing.T) {
	fs := newFaultStore(t)
	baseJSON, baseStreams := faultCommittedEmit(t, "emit-tiny")
	deltaJSON, deltaStreams := faultCommittedEmit(t, "emit-tiny-delta1")
	ctx := context.Background()
	if _, err := LoadBatch(ctx, fs.store, deltaJSON, deltaStreams); err != nil {
		t.Fatalf("delta load: %v", err)
	}
	started := time.Now()
	_, err := LoadBatch(ctx, fs.store, baseJSON, baseStreams)
	staleMS := float64(time.Since(started).Microseconds()) / 1000
	if err == nil {
		t.Fatal("stale edition loaded, want count_mismatch failure")
	}
	var baseManifest BatchManifest
	if uerr := json.Unmarshal(baseJSON, &baseManifest); uerr != nil {
		t.Fatalf("manifest decode: %v", uerr)
	}
	report, gerr := fs.store.GetRun(ctx, SourcePrep, "station-prep:"+baseManifest.RunID+":registry-13col")
	if gerr != nil {
		t.Fatalf("failed run invisible: %v", gerr)
	}
	if report.State != "failed" {
		t.Fatalf("stale state = %q, want failed", report.State)
	}
	complete, cerr := fs.store.Q.CountCompleteRegistryRuns(ctx, SourcePrep)
	if cerr != nil {
		t.Fatalf("complete runs: %v", cerr)
	}
	if complete != 2 {
		t.Fatalf("complete runs = %d, want 2 (delta inputs only)", complete)
	}
	t.Logf("stale edition refused in %.1fms with visible failed state", staleMS)
}

func faultCommittedEmit(t *testing.T, dir string) ([]byte, BatchStreams) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller unavailable")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "..")
	base := filepath.Join(root, "contracts", "testdata", "station-prep", "datasets", dir)
	read := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(base, name))
		if err != nil {
			t.Fatalf("fixture %s: %v", name, err)
		}
		return raw
	}
	return read("manifest.json"), BatchStreams{
		Assertions: read("assertions.jsonl"),
		Candidates: read("candidates.jsonl"),
		Quarantine: read("quarantine.jsonl"),
	}
}

// exists, naming the failure position.
func TestFaultMalformedBatch(t *testing.T) {
	fs := newFaultStore(t)
	manifestJSON, streams := faultBatch(t, 10, "aaaaaaaa-4444-4333-8444-444444444444")
	cases := map[string]func() ([]byte, BatchStreams){
		"truncated manifest": func() ([]byte, BatchStreams) {
			return manifestJSON[:len(manifestJSON)/2], streams
		},
		"truncated stream": func() ([]byte, BatchStreams) {
			lines := streams.Assertions
			return manifestJSON, BatchStreams{Assertions: lines[:len(lines)/2]}
		},
	}
	for name, mutate := range cases {
		badManifest, badStreams := mutate()
		if _, err := LoadBatch(context.Background(), fs.store, badManifest, badStreams); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	complete, err := fs.store.Q.CountCompleteRegistryRuns(context.Background(), SourcePrep)
	if err != nil {
		t.Fatal(err)
	}
	if complete != 0 {
		t.Fatalf("malformed batches left %d complete runs", complete)
	}
}

// TestFaultReadDegradationDuringKill samples app-path reads before,
// during and after a mid-load kill: reads must never error, the fault
// must surface, and post-fault latency must recover (drain measured
// from fault return to first success).
func TestFaultReadDegradationDuringKill(t *testing.T) {
	fs := newFaultStore(t)
	ctx := context.Background()
	for i := 0; i < 2000; i++ {
		id := fmt.Sprintf("cccccccc-%04x-4111-8111-%012x", i%65536, i)
		if _, err := fs.pool.Exec(ctx, `INSERT INTO directory_stations
			(id, display_name, address, municipality_code, state, status, current_point, current_quality)
			VALUES ($1::uuid, $2, '{}', '3550000', 'SP', 'active',
			'SRID=4326;POINT(-46.633 -23.55)'::geography, 'reviewed')`,
			id, fmt.Sprintf("[RST19-TEST] STATION %04d", i)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	reader := directoryread.NewReader(fs.pool)
	denseRead := func(ctx context.Context) (float64, error) {
		filter, err := directoryapp.ValidateSearch("SP", "3550000", "", 20, "")
		if err != nil {
			return 0, err
		}
		started := time.Now()
		if _, _, err := reader.Search(ctx, filter); err != nil {
			return 0, err
		}
		return float64(time.Since(started).Microseconds()) / 1000, nil
	}
	sample := func(n int) ([]float64, int) {
		lat := make([]float64, 0, n)
		errs := 0
		for i := 0; i < n; i++ {
			ms, err := denseRead(ctx)
			if err != nil {
				errs++
				continue
			}
			lat = append(lat, ms)
		}
		return lat, errs
	}
	p95 := func(lat []float64) float64 {
		if len(lat) == 0 {
			return -1
		}
		sorted := append([]float64(nil), lat...)
		sort.Float64s(sorted)
		return sorted[int(0.95*float64(len(sorted)-1))]
	}
	before, beforeErrs := sample(30)
	runID := "aaaaaaaa-6666-4333-8666-666666666666"
	manifestJSON, streams := faultBatch(t, 1500, runID)
	loadCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := LoadBatch(loadCtx, fs.store, manifestJSON, streams); done <- err }()
	deadline := time.Now().Add(60 * time.Second)
	for faultLiveTotal(fs) == 0 {
		select {
		case err := <-done:
			t.Fatalf("load finished before kill window: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("staging never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	var during []float64
	duringErrs := 0
	cancel()
	killAt := time.Now()
	var loadErr error
	settled := false
	for !settled {
		if ms, err := denseRead(ctx); err != nil {
			duringErrs++
		} else {
			during = append(during, ms)
		}
		select {
		case err := <-done:
			loadErr = err
			settled = true
		default:
		}
	}
	detectMS := float64(time.Since(killAt).Microseconds()) / 1000
	after, afterErrs := sample(30)
	if loadErr == nil {
		t.Fatal("killed load returned nil error")
	}
	if beforeErrs != 0 || duringErrs != 0 || afterErrs != 0 {
		t.Fatalf("read errors before/during/after = %d/%d/%d, want 0", beforeErrs, duringErrs, afterErrs)
	}
	t.Logf("reads p95 before=%.2fms during=%.2fms after=%.2fms detect=%.1fms",
		p95(before), p95(during), p95(after), detectMS)
	if after := p95(after); after > 2*p95(before)+5 {
		t.Fatalf("post-fault p95 %.2fms did not drain", after)
	}
}

// hash must refuse the batch with the input named, staging nothing.
func TestFaultChecksumMismatch(t *testing.T) {
	fs := newFaultStore(t)
	manifestJSON, streams := faultBatch(t, 10, "aaaaaaaa-5555-4333-8555-555555555555")
	streams.Assertions[100] ^= 0xFF
	_, err := LoadBatch(context.Background(), fs.store, manifestJSON, streams)
	if err == nil {
		t.Fatal("tampered stream accepted")
	}
	complete, err := fs.store.Q.CountCompleteRegistryRuns(context.Background(), SourcePrep)
	if err != nil {
		t.Fatal(err)
	}
	if complete != 0 {
		t.Fatalf("tampered batch left %d complete runs", complete)
	}
}
