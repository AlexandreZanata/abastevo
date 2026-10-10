//go:build integration

package read

// RST-18 mixed reads, ingestion contention and sustainable capacity.
// Reads run open arrival (5/20/50/100 rps ladder plus refinement) over
// the Go Reader directly (no HTTP) while bounded importer workers stage disjoint
// full editions through the owned Go loader on the same pool and
// database. Identical dataset, workload mix and pool configuration in
// every cell; fresh disposable database per cell.
//
// Budgets (lab bounds, not SLA): read p95 <= 100ms (RST-10
// provisional), read errors and drops zero, imports error-free,
// non-terminal run backlog stable, mean pool acquire wait <= 50ms.
// Sustainable capacity is the highest measured load meeting all of
// them without backlog growth during the steady interval. Review
// backlog is NOT_APPLICABLE (no async reviewer in the loop;
// freshness is the per-import completion lag, reported as max).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/application"

	dbmigrations "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/migrations"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/platform/migrate"
)

// capacityPoolMax is the identical pool ceiling in every cell so pool
// wait is comparable, never tuned per cell.
const capacityPoolMax = 10

func capacityFreshDB(b *testing.B) (*pgxpool.Pool, string) {
	b.Helper()
	// Same disposable-database mechanism as the registry/read
	// harnesses (env override, local default); migrate.Apply fails
	// loudly when unreachable, never silently.
	adminDSN := os.Getenv("ANPFUEL_TEST_DATABASE_URL")
	if adminDSN == "" {
		adminDSN = "postgres://anpfuel:anpfuel@127.0.0.1:5434/anpfuel?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		b.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	name := fmt.Sprintf("read_capacity_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		b.Fatalf("create database: %v", err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			b.Errorf("admin connect for drop: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			b.Errorf("drop database: %v", err)
		}
	})
	u, err := url.Parse(adminDSN)
	if err != nil {
		b.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()
	if _, err := migrate.Apply(ctx, dsn, dbmigrations.Files); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		b.Fatalf("pool config: %v", err)
	}
	config.MaxConns = capacityPoolMax
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		b.Fatalf("pool: %v", err)
	}
	b.Cleanup(pool.Close)
	return pool, dsn
}

// capacityEdition is one pre-generated disjoint emit, read once.
type capacityEdition struct {
	manifest []byte
	streams  registry.BatchStreams
	want     map[string]int64
}

func loadEditions(tb testing.TB) []capacityEdition {
	tb.Helper()
	list := os.Getenv("RST18_EDITIONS")
	if list == "" {
		list = "/tmp/rst18/editions.txt"
	}
	raw, err := os.ReadFile(list)
	if err != nil {
		tb.Skipf("editions list missing (generate 20k editions first): %v", err)
	}
	var editions []capacityEdition
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		dir := fields[0]
		read := func(name string) []byte {
			raw, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				tb.Fatalf("edition file: %v", err)
			}
			return raw
		}
		manifest := read("manifest.json")
		var decoded struct {
			Counts map[string]struct {
				Accepted int64 `json:"accepted"`
			} `json:"counts"`
		}
		if err := json.Unmarshal(manifest, &decoded); err != nil {
			tb.Fatalf("edition manifest: %v", err)
		}
		want := map[string]int64{}
		for key, counts := range decoded.Counts {
			want[key] = counts.Accepted
		}
		editions = append(editions, capacityEdition{
			manifest: manifest,
			streams: registry.BatchStreams{
				Assertions: read("assertions.jsonl"),
				Candidates: read("candidates.jsonl"),
				Quarantine: read("quarantine.jsonl"),
			},
			want: want,
		})
	}
	if len(editions) == 0 {
		tb.Skip("no editions listed")
	}
	return editions
}

// capacityReads drives the fixed three-workload mix at rate rps for
// duration, tracking per-workload latencies for the tail penalty.
func capacityReads(ctx context.Context, reader *Reader, dense string, rate float64, duration time.Duration) (byName map[string][]float64, errs, offered, dropped int) {
	cityFn := func(ctx context.Context) error {
		f, err := application.ValidateSearch("SP", dense, "", 20, "")
		if err != nil {
			return err
		}
		_, _, err = reader.Search(ctx, f)
		return err
	}
	sparseFn := func(ctx context.Context) error {
		f, err := application.ValidateSearch("SP", "3550101", "", 20, "")
		if err != nil {
			return err
		}
		_, _, err = reader.Search(ctx, f)
		return err
	}
	nearbyFn := func(ctx context.Context) error {
		f, err := application.ValidateNearby(-23.55, -46.633, 5000, 20, "")
		if err != nil {
			return err
		}
		_, _, err = reader.Nearby(ctx, f)
		return err
	}
	names := []string{"city_dense", "city_sparse", "nearby_dense"}
	fns := []func(context.Context) error{cityFn, sparseFn, nearbyFn}
	byName = map[string][]float64{}
	var mu sync.Mutex
	ticker := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer ticker.Stop()
	deadline := time.Now().Add(duration)
	slots := make(chan struct{}, 64)
	var wg sync.WaitGroup
	tick := 0
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			goto drain
		case <-ticker.C:
		}
		offered++
		select {
		case slots <- struct{}{}:
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				defer func() { <-slots }()
				name := names[n%len(names)]
				started := time.Now()
				if err := fns[n%len(fns)](ctx); err != nil {
					mu.Lock()
					errs++
					mu.Unlock()
					return
				}
				mu.Lock()
				byName[name] = append(byName[name], float64(time.Since(started).Microseconds())/1000)
				mu.Unlock()
			}(tick)
		default:
			dropped++
		}
		tick++
	}
drain:
	wg.Wait()
	return byName, errs, offered, dropped
}

// capacitySampler records contention signals every second: waiting
// backends with the dominant wait event, non-terminal run backlog and
// pool acquire totals for mean-wait math.
type capacitySample struct {
	waiting   int64
	waitEvent string
	backlog   int64
	err       error
}

func capacityWatch(ctx context.Context, pool *pgxpool.Pool, out *[]capacitySample, mu *sync.Mutex, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			waiting, event, backlog, err := capacityContention(sampleCtx, pool)
			cancel()
			mu.Lock()
			*out = append(*out, capacitySample{waiting: waiting, waitEvent: event, backlog: backlog, err: err})
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

type capacityResult struct {
	rate, workers         int
	readP95               map[string]float64
	readErrs, readDropped int
	readOffered           int
	importThroughput      float64
	importMaxMS           float64
	importErrs            int
	importsDone           int
	poolAcquireWaitMS     float64
	maxWaiting            int64
	waitEvent             string
	maxBacklog            int64
	backlogGrew           bool
	walMiB                float64
	deadTup               int64
	checkpoints           int64
	verdict               string
	verdictOK             bool
}

// capacityCell runs one (rate, importers) cell: reads-alone when
// workers is 0, imports-alone when rate is 0, mixed otherwise.
func capacityCell(b *testing.B, editions []capacityEdition, rate float64, workers int, phase time.Duration) capacityResult {
	b.Helper()
	result := capacityResult{rate: int(rate), workers: workers, readP95: map[string]float64{}}
	pool, _ := capacityFreshDB(b)
	dense, _, _ := trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	statBefore := pool.Stat()
	var walBefore string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&walBefore); err != nil {
		b.Fatal(err)
	}
	checkBefore, err := completedCheckpointCount(ctx, pool)
	if err != nil {
		b.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var samples []capacitySample
	wg.Add(1)
	go capacityWatch(ctx, pool, &samples, &mu, stop, &wg)

	// Importers: closed-loop workers over the edition pool, each edition
	// staged once globally (atomic claim). Fresh run IDs per edition come
	// from the pre-generated manifests.
	var claim atomic.Int64
	var importMu sync.Mutex
	var importLat []float64
	var importErrs atomic.Int64
	var importsDone atomic.Int64
	var importWg sync.WaitGroup
	importStarted := time.Now()
	var importFinished time.Time
	if workers > 0 {
		for w := 0; w < workers; w++ {
			importWg.Add(1)
			go func() {
				defer importWg.Done()
				defer func() {
					importMu.Lock()
					defer importMu.Unlock()
					if now := time.Now(); now.After(importFinished) {
						importFinished = now
					}
				}()
				for {
					n := int(claim.Add(1)) - 1
					if n >= len(editions) {
						return
					}
					edition := editions[n]
					started := time.Now()
					reports, err := registry.LoadBatch(ctx, newCapacityStore(pool), edition.manifest, edition.streams)
					elapsed := float64(time.Since(started).Microseconds()) / 1000
					if err != nil {
						importErrs.Add(1)
						return
					}
					for key, want := range edition.want {
						if reports[key].State != "complete" || reports[key].Accepted != want {
							importErrs.Add(1)
							return
						}
					}
					importMu.Lock()
					importLat = append(importLat, elapsed)
					importMu.Unlock()
					importsDone.Add(1)
				}
			}()
		}
	}

	if rate > 0 {
		byName, errs, offered, dropped := capacityReads(ctx, reader, dense, rate, phase)
		result.readErrs, result.readOffered, result.readDropped = errs, offered, dropped
		for name, lat := range byName {
			if len(lat) == 0 {
				continue
			}
			_, p95, _ := percentiles(lat)
			result.readP95[name] = p95
		}
	}
	importWg.Wait()
	close(stop)
	wg.Wait()

	statAfter := pool.Stat()
	acquires := statAfter.AcquireCount() - statBefore.AcquireCount()
	acquireNS := statAfter.AcquireDuration() - statBefore.AcquireDuration()
	if acquires > 0 {
		result.poolAcquireWaitMS = float64(acquireNS) / float64(acquires) / 1e6
	}
	var maxWaiting, maxBacklog int64
	var event string
	var firstBacklog, lastBacklog int64
	for i, sample := range samples {
		if sample.err != nil {
			b.Fatalf("capacity telemetry unavailable: %v", sample.err)
		}
		if sample.waiting > maxWaiting {
			maxWaiting = sample.waiting
			event = sample.waitEvent
		}
		if sample.backlog > maxBacklog {
			maxBacklog = sample.backlog
		}
		if i < len(samples)/2 {
			firstBacklog += sample.backlog
		} else {
			lastBacklog += sample.backlog
		}
	}
	result.maxWaiting, result.waitEvent, result.maxBacklog = maxWaiting, event, maxBacklog
	halves := len(samples) / 2
	if halves > 0 {
		result.backlogGrew = float64(lastBacklog)/float64(halves) > float64(firstBacklog)/float64(halves)+1
	}
	var walAfter string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&walAfter); err != nil {
		b.Fatal(err)
	}
	var walBytes int64
	if err := pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)`, walAfter, walBefore).Scan(&walBytes); err != nil {
		b.Fatal(err)
	}
	result.walMiB = float64(walBytes) / 1048576
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(n_dead_tup),0) FROM pg_stat_user_tables
 WHERE relname IN ('registry_source_runs','registry_assertions','directory_stations')`).Scan(&result.deadTup); err != nil {
		b.Fatal(err)
	}
	checkAfter, err := completedCheckpointCount(ctx, pool)
	if err != nil {
		b.Fatal(err)
	}
	if checkAfter < checkBefore {
		b.Fatal("checkpoint statistics reset during observation")
	}
	result.checkpoints = checkAfter - checkBefore
	if len(importLat) > 0 {
		sort.Float64s(importLat)
		result.importMaxMS = importLat[len(importLat)-1]
		result.importThroughput, err = aggregateImportRate(importLat, importFinished.Sub(importStarted))
		if err != nil {
			b.Fatal(err)
		}
	}
	result.importsDone = int(importsDone.Load())
	result.importErrs = int(importErrs.Load())

	worstP95 := 0.0
	for _, p95 := range result.readP95 {
		if p95 > worstP95 {
			worstP95 = p95
		}
	}
	verdicts := []string{}
	ok := true
	fail := func(reason string) {
		ok = false
		verdicts = append(verdicts, reason)
	}
	if rate > 0 {
		if worstP95 > 100 {
			fail(fmt.Sprintf("read p95 %.0fms over 100ms", worstP95))
		}
		if result.readErrs > 0 || result.readDropped > 0 {
			fail(fmt.Sprintf("read errors=%d dropped=%d", result.readErrs, result.readDropped))
		}
		if result.poolAcquireWaitMS > 50 {
			fail(fmt.Sprintf("pool acquire wait %.1fms over 50ms", result.poolAcquireWaitMS))
		}
	}
	if workers > 0 && result.importErrs > 0 {
		fail(fmt.Sprintf("import errors=%d", result.importErrs))
	}
	if result.backlogGrew {
		fail("run backlog grew")
	}
	if ok {
		verdicts = append(verdicts, "within budgets")
	}
	result.verdict = strings.Join(verdicts, "; ")
	result.verdictOK = ok
	b.ReportMetric(worstP95, fmt.Sprintf("cap_r%d_w%d_p95_ms", int(rate), workers))
	b.Logf("cell r=%d w=%d reads=%v errs=%d dropped=%d/%d imports=%d (%.1f/s max%.0fms) poolwait=%.1fms maxwaiting=%d(%s) backlogmax=%d wal=%.1fMiB dead=%d ckpt=%d verdict=%s",
		int(rate), workers, result.readP95, result.readErrs, result.readDropped, result.readOffered,
		result.importsDone, result.importThroughput, result.importMaxMS, result.poolAcquireWaitMS,
		result.maxWaiting, result.waitEvent, result.maxBacklog, result.walMiB, result.deadTup,
		result.checkpoints, result.verdict)
	return result
}

// newCapacityStore adapts the pool to the registry Store surface used
// by LoadBatch (PGStore with generated queries).
func newCapacityStore(pool *pgxpool.Pool) *registry.PGStore {
	return registry.NewPGStore(pool)
}

// BenchmarkCapacityLadder runs reads-alone across the illustrative
// 5/20/50/100 rps ladder on identical cells.
func BenchmarkCapacityLadder(b *testing.B) {
	editions := loadEditions(b)
	_ = editions
	for _, rate := range []float64{5, 20, 50, 100} {
		capacityCell(b, nil, rate, 0, 20*time.Second)
	}
}

// BenchmarkCapacityImports stages disjoint editions with 1 and 4
// closed-loop importer workers and no reads.
func BenchmarkCapacityImports(b *testing.B) {
	editions := loadEditions(b)
	for _, workers := range []int{1, 4} {
		capacityCell(b, editions, 0, workers, 0)
	}
}

// BenchmarkCapacityMixed crosses the rate ladder with 1 and 4
// importers; RST18_REFINE_RATES (e.g. "30,40") appends knee
// refinement cells at both importer levels, while RST18_RATES
// replaces the ladder outright for bounded reruns.
func BenchmarkCapacityMixed(b *testing.B) {
	editions := loadEditions(b)
	rates := []float64{5, 20, 50, 100}
	if override := os.Getenv("RST18_RATES"); override != "" {
		rates = nil
		for _, field := range strings.Split(override, ",") {
			var rate float64
			if _, err := fmt.Sscanf(strings.TrimSpace(field), "%f", &rate); err == nil && rate > 0 {
				rates = append(rates, rate)
			}
		}
	}
	if refine := os.Getenv("RST18_REFINE_RATES"); refine != "" {
		for _, field := range strings.Split(refine, ",") {
			var rate float64
			if _, err := fmt.Sscanf(strings.TrimSpace(field), "%f", &rate); err == nil && rate > 0 {
				rates = append(rates, rate)
			}
		}
	}
	for _, rate := range rates {
		for _, workers := range []int{1, 4} {
			capacityCell(b, editions, rate, workers, 25*time.Second)
		}
	}
}
