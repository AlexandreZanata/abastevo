//go:build integration

package read

// RST-20 bounded 30-minute pilot: repeated imports, retention and
// maintenance, autovacuum/checkpoints left on, hot-city rotation,
// growth/leak slopes over time. Short runs are never labelled soak
// success: the 24/48h campaigns stay separately scheduled (OWED).
// Fresh disposable database, identical pool ceiling as RST-18,
// durability never weakened.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/application"
)

const (
	soakImportEvery = 2 * time.Minute
	soakSampleEvery = 30 * time.Second
	soakReadRate    = 20.0
)

// soakDuration defaults to the bounded 30-minute pilot; override in
// minutes only for smoke checks or separately scheduled campaigns
// (a short run is never soak evidence).
func soakDuration() time.Duration {
	if raw := os.Getenv("RST20_DURATION_MIN"); raw != "" {
		var minutes int
		if _, err := fmt.Sscanf(raw, "%d", &minutes); err == nil && minutes > 0 {
			return time.Duration(minutes) * time.Minute
		}
	}
	return 30 * time.Minute
}

type soakEdition struct {
	manifest []byte
	streams  registry.BatchStreams
}

func loadSoakEditions(tb testing.TB) []soakEdition {
	tb.Helper()
	list := os.Getenv("RST20_EDITIONS")
	if list == "" {
		list = "/tmp/rst20/editions.txt"
	}
	raw, err := os.ReadFile(list)
	if err != nil {
		tb.Skipf("soak editions missing (generate 20k editions first): %v", err)
	}
	var dirs []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if dir := strings.TrimSpace(line); dir != "" {
			dirs = append(dirs, dir)
		}
	}
	editions := make([]soakEdition, 0, len(dirs))
	for _, dir := range dirs {
		read := func(name string) []byte {
			raw, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				tb.Fatalf("soak edition file: %v", err)
			}
			return raw
		}
		editions = append(editions, soakEdition{
			manifest: read("manifest.json"),
			streams: registry.BatchStreams{
				Assertions: read("assertions.jsonl"),
				Candidates: read("candidates.jsonl"),
				Quarantine: read("quarantine.jsonl"),
			},
		})
	}
	return editions
}

type soakPoint struct {
	elapsedS        float64
	readP95         float64
	readErrs        int
	importLagMS     float64
	poolAcquired    int32
	poolAcquireMS   float64
	walMiB          float64
	tableMiB        float64
	indexMiB        float64
	deadTup         int64
	backlog         int64
	checkpoints     int64
	autovacuumCount int64
}

// soakLeastSquares returns the per-minute slope of y over x (seconds).
func soakLeastSquares(x, y []float64) float64 {
	if len(x) < 2 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for i := range x {
		sx += x[i]
		sy += y[i]
		sxx += x[i] * x[i]
		sxy += x[i] * y[i]
	}
	n := float64(len(x))
	den := n*sxx - sx*sx
	if den == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / den * 60
}

// BenchmarkSoakPilot runs the bounded 30-minute pilot: 20 rps reads
// with hot-city rotation, one 20k-row edition every 2 minutes,
// maintenance at t=10/20 and simulated retention at t=15/25.
func BenchmarkSoakPilot(b *testing.B) {
	editions := loadSoakEditions(b)
	pool, _ := capacityFreshDB(b)
	dense, _, _ := trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	store := registry.NewPGStore(pool)
	ctx := context.Background()
	start := time.Now()
	deadline := start.Add(soakDuration())

	var walStart string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&walStart); err != nil {
		b.Fatal(err)
	}

	// Reads: hot dense city 50%, rotating sparse sweep 30%, nearby 20%.
	// Rotation cycles unseen cities every request (RST-15 pattern).
	rotating := []string{"3550101", "3550102", "3550103", trafficEmptyCode, "3550201", "3550202"}
	var tick atomic.Int64
	readFn := func(ctx context.Context) (string, error) {
		n := int(tick.Add(1))
		switch n % 10 {
		case 0, 1, 2, 3, 4:
			f, err := application.ValidateSearch("SP", dense, "", 20, "")
			if err != nil {
				return "", err
			}
			_, _, err = reader.Search(ctx, f)
			return "hot_dense", err
		case 5, 6, 7:
			city := rotating[n%len(rotating)]
			f, err := application.ValidateSearch("SP", city, "", 20, "")
			if err != nil {
				return "", err
			}
			_, _, err = reader.Search(ctx, f)
			return "rotating", err
		default:
			f, err := application.ValidateNearby(-23.55, -46.633, 5000, 20, "")
			if err != nil {
				return "", err
			}
			_, _, err = reader.Nearby(ctx, f)
			return "nearby", err
		}
	}

	type readOutcome struct {
		name string
		ms   float64
		err  bool
	}
	readOutcomes := make(chan readOutcome, 4096)
	stopReads := make(chan struct{})
	var readWg sync.WaitGroup
	readWg.Add(1)
	go func() {
		defer readWg.Done()
		ticker := time.NewTicker(time.Duration(float64(time.Second) / soakReadRate))
		defer ticker.Stop()
		for {
			select {
			case <-stopReads:
				return
			case <-ticker.C:
			}
			if time.Now().After(deadline) {
				return
			}
			started := time.Now()
			name, err := readFn(ctx)
			select {
			case readOutcomes <- readOutcome{name, float64(time.Since(started).Microseconds()) / 1000, err != nil}:
			default:
			}
		}
	}()

	var mu sync.Mutex
	var windowLat []float64
	windowErrs := 0
	var importLagMS float64
	var imported, retained int
	var importedRunIDs []string
	var maintenanceMS, retentionMS float64
	maintenanceDone := map[int]bool{}
	retentionDone := map[int]bool{}

	sample := func() soakPoint {
		now := time.Since(start).Seconds()
		mu.Lock()
		lat := append([]float64(nil), windowLat...)
		errs := windowErrs
		windowLat = windowLat[:0]
		windowErrs = 0
		lag := importLagMS
		mu.Unlock()
		p95 := 0.0
		if len(lat) > 0 {
			sort.Float64s(lat)
			p95 = lat[int(0.95*float64(len(lat)-1))]
		}
		stat := pool.Stat()
		point := soakPoint{elapsedS: now, readP95: p95, readErrs: errs, importLagMS: lag}
		point.poolAcquired = stat.AcquiredConns()
		var walNow string
		if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&walNow); err != nil {
			b.Fatal(err)
		}
		var walBytes int64
		if err := pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)`, walNow, walStart).Scan(&walBytes); err != nil {
			b.Fatal(err)
		}
		point.walMiB = float64(walBytes) / 1048576
		var tableBytes int64
		// Table bytes exclude indexes; historical total_relation_size double-counted them.
		if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(pg_table_size(c.oid)),0)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname IN ('directory_stations','registry_assertions')`).Scan(&tableBytes); err != nil {
			b.Fatal(err)
		}
		point.tableMiB = float64(tableBytes) / 1048576
		var indexBytes int64
		if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(pg_indexes_size(c.oid)),0)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND c.relname IN ('directory_stations','registry_assertions')`).Scan(&indexBytes); err != nil {
			b.Fatal(err)
		}
		point.indexMiB = float64(indexBytes) / 1048576
		if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(n_dead_tup),0) FROM pg_stat_user_tables
 WHERE relname IN ('directory_stations','registry_assertions')`).Scan(&point.deadTup); err != nil {
			b.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM registry_source_runs
 WHERE state NOT IN ('complete','failed')`).Scan(&point.backlog); err != nil {
			b.Fatal(err)
		}
		var err error
		point.checkpoints, err = completedCheckpointCount(ctx, pool)
		if err != nil {
			b.Fatal(err)
		}
		return point
	}

	// Drain read outcomes into the current window.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for outcome := range readOutcomes {
			mu.Lock()
			if !outcome.err {
				windowLat = append(windowLat, outcome.ms)
			} else {
				windowErrs++
			}
			mu.Unlock()
		}
	}()

	var points []soakPoint
	nextImport := start
	nextSample := start
	editionIdx := 0
	for time.Now().Before(deadline) {
		now := time.Now()
		if !now.Before(nextSample) {
			points = append(points, sample())
			nextSample = nextSample.Add(soakSampleEvery)
		}
		if !now.Before(nextImport) && editionIdx < len(editions) {
			edition := editions[editionIdx]
			editionIdx++
			imported++
			importStarted := time.Now()
			reports, err := registry.LoadBatch(ctx, store, edition.manifest, edition.streams)
			lag := float64(time.Since(importStarted).Microseconds()) / 1000
			mu.Lock()
			importLagMS = lag
			mu.Unlock()
			if err != nil {
				b.Fatalf("soak import %d: %v", imported, err)
			}
			for _, report := range reports {
				if report.State != "complete" {
					b.Fatalf("soak import %d state %s", imported, report.State)
				}
				importedRunIDs = append(importedRunIDs, report.RunID)
			}
			nextImport = nextImport.Add(soakImportEvery)
		}
		elapsed := now.Sub(start)
		for _, mark := range []int{10, 20} {
			if !maintenanceDone[mark] && elapsed >= time.Duration(mark)*time.Minute {
				maintenanceDone[mark] = true
				mStarted := time.Now()
				if _, err := pool.Exec(ctx, `VACUUM (ANALYZE) directory_stations, registry_assertions, registry_source_runs`); err != nil {
					b.Fatalf("maintenance: %v", err)
				}
				maintenanceMS += float64(time.Since(mStarted).Microseconds()) / 1000
			}
		}
		for _, mark := range []int{15, 25} {
			// Simulated retention policy (not product behavior):
			// drop assertions of the three oldest imported editions.
			if !retentionDone[mark] && elapsed >= time.Duration(mark)*time.Minute && len(importedRunIDs) >= 3 {
				retentionDone[mark] = true
				rStarted := time.Now()
				for _, runID := range importedRunIDs[:3] {
					if _, err := pool.Exec(ctx, `DELETE FROM registry_assertions WHERE run_id = $1::uuid`, runID); err != nil {
						b.Fatalf("retention: %v", err)
					}
				}
				importedRunIDs = importedRunIDs[3:]
				retained += 3
				retentionMS += float64(time.Since(rStarted).Microseconds()) / 1000
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	close(stopReads)
	readWg.Wait()
	close(readOutcomes)
	<-drainDone
	points = append(points, sample())

	// Slopes per minute and headroom over the pilot.
	xs := make([]float64, len(points))
	for i, point := range points {
		xs[i] = point.elapsedS
	}
	series := func(get func(soakPoint) float64) []float64 {
		out := make([]float64, len(points))
		for i, point := range points {
			out[i] = get(point)
		}
		return out
	}
	walSlope := soakLeastSquares(xs, series(func(p soakPoint) float64 { return p.walMiB }))
	tableSlope := soakLeastSquares(xs, series(func(p soakPoint) float64 { return p.tableMiB }))
	deadSlope := soakLeastSquares(xs, series(func(p soakPoint) float64 { return float64(p.deadTup) }))
	backlogSlope := soakLeastSquares(xs, series(func(p soakPoint) float64 { return float64(p.backlog) }))
	worstP95 := 0.0
	totalErrs := 0
	minPoolHeadroom := 1.0
	minBudgetMargin := 100.0
	for _, point := range points {
		if point.readP95 > worstP95 {
			worstP95 = point.readP95
		}
		totalErrs += point.readErrs
		headroom := 1 - float64(point.poolAcquired)/float64(capacityPoolMax)
		if headroom < minPoolHeadroom {
			minPoolHeadroom = headroom
		}
		if margin := 100 - point.readP95; margin < minBudgetMargin {
			minBudgetMargin = margin
		}
	}
	last := points[len(points)-1]
	b.ReportMetric(worstP95, "soak_worst_window_p95_ms")
	b.ReportMetric(walSlope, "soak_wal_mib_per_min")
	b.ReportMetric(tableSlope, "soak_table_mib_per_min")
	b.ReportMetric(deadSlope, "soak_dead_per_min")
	b.ReportMetric(float64(totalErrs), "soak_read_errors")
	verdict := "within budgets"
	if worstP95 > 100 || totalErrs > 0 || backlogSlope > 0.01 {
		verdict = "BUDGET-BREACH (pilot, not soak success)"
	}
	b.Logf("soak pilot: %d samples imports=%d retained_editions=%d maintenance=%.0fms retention=%.0fms worst_window_p95=%.1fms read_errors=%d",
		len(points), imported, retained, maintenanceMS, retentionMS, worstP95, totalErrs)
	b.Logf("soak slopes/min: wal_mib=%+.2f table_mib=%+.2f dead=%+.1f backlog=%+.3f",
		walSlope, tableSlope, deadSlope, backlogSlope)
	b.Logf("soak end: wal=%.0fMiB table=%.1fMiB index=%.1fMiB dead=%d backlog=%d checkpoints=%d pool_headroom_min=%.2f budget_margin_min=%.1fms verdict=%s",
		last.walMiB, last.tableMiB, last.indexMiB, last.deadTup, last.backlog, last.checkpoints,
		minPoolHeadroom, minBudgetMargin, verdict)
	if verdict != "within budgets" {
		b.Fatalf("soak pilot breached budgets: %s", verdict)
	}
}
