//go:build integration

package read

// RST-15 city search, pagination and traffic distribution over the exact
// app read path (Reader.Search/Nearby/Detail) against raw sqlc queries
// measured separately. Census seed reuses the RST-07 shape; the empty
// city code is never seeded. Same-sort-value ties on Search are
// NOT_APPLICABLE (keyset orders by unique id::text); Nearby distance
// ties are covered explicitly with identical-coordinate stations.
//
// Cache labels are pinned: cold is first-touch on a fresh pool, warm
// follows a 100-iteration warmup, restarted reconnects the same database
// with a new pool, rotating cycles unseen cities every request.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/application"

	dbmigrations "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/migrations"
	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/platform/migrate"
)

// trafficEmptyCode is never seeded: the zero-row stratum.
const trafficEmptyCode = "3550999"

// trafficFreshDB mirrors freshPool but also returns the disposable DSN so
// the restarted-pool cache state can reconnect the same database.
func trafficFreshDB(tb testing.TB) (*pgxpool.Pool, string) {
	tb.Helper()
	adminDSN := testDSN(tb)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		tb.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	name := fmt.Sprintf("read_traffic_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		tb.Fatalf("create database: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			tb.Errorf("admin connect for drop: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			tb.Errorf("drop database: %v", err)
		}
	})
	u, err := url.Parse(adminDSN)
	if err != nil {
		tb.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()
	if _, err := migrate.Apply(ctx, dsn, dbmigrations.Files); err != nil {
		tb.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		tb.Fatalf("pool: %v", err)
	}
	tb.Cleanup(pool.Close)
	return pool, dsn
}

func reopenPool(tb testing.TB, dsn string) *pgxpool.Pool {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		tb.Fatalf("reopen pool: %v", err)
	}
	tb.Cleanup(pool.Close)
	return pool
}

// trafficSeed plants the census plus three identical-coordinate tie
// stations in the dense city; it returns dense/sparse codes and one
// known station id for detail lookups.
func trafficSeed(tb testing.TB, pool *pgxpool.Pool, n int) (dense, sparse, knownID string) {
	tb.Helper()
	ctx := context.Background()
	dense, sparse, _ = seedCensus(tb, pool, n)
	ties := []string{
		"aaaaaaaa-1111-4111-8111-111111111111",
		"aaaaaaaa-2222-4222-8222-222222222222",
		"aaaaaaaa-3333-4333-8333-333333333333",
	}
	for i, id := range ties {
		var uid pgtype.UUID
		if err := uid.Scan(id); err != nil {
			tb.Fatalf("tie uuid: %v", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO directory_stations
			(id, display_name, address, municipality_code, state, status, current_point, current_quality)
			VALUES ($1, $2, '{}', $3, 'SP', 'active', 'SRID=4326;POINT(-46.633000 -23.550000)'::geography, 'reviewed')`,
			uid, fmt.Sprintf("[RST15-TEST] TIE %d", i), dense); err != nil {
			tb.Fatalf("tie insert: %v", err)
		}
	}
	var id pgtype.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM directory_stations WHERE municipality_code = $1 LIMIT 1`, dense).Scan(&id); err != nil {
		tb.Fatalf("known id: %v", err)
	}
	var buf [16]byte
	copy(buf[:], id.Bytes[:])
	knownID = fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
	return dense, sparse, knownID
}

// trafficWorkload is one measured query at raw-SQL and Reader layers.
type trafficWorkload struct {
	name    string
	stratum string
	// Page limit at the in-process Reader layer; raw queries run limit+1, so the Reader
	// row count must equal min(raw rows, limit).
	limit  int
	raw    func(ctx context.Context, q *directory.Queries) (int, error)
	reader func(ctx context.Context, r *Reader) (int, error)
}

func trafficWorkloads(dense, sparse, knownID string) []trafficWorkload {
	search := func(state, city, q string, limit int, after string) (application.SearchFilter, error) {
		return application.ValidateSearch(state, city, q, limit, after)
	}
	return []trafficWorkload{
		{"city_dense", "large", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.SearchStations(ctx, directory.SearchStationsParams{State: "SP", Municipality: dense, LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := search("SP", dense, "", 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Search(ctx, f)
			return len(rows), err
		}},
		{"city_sparse", "small", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.SearchStations(ctx, directory.SearchStationsParams{State: "SP", Municipality: sparse, LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := search("SP", sparse, "", 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Search(ctx, f)
			return len(rows), err
		}},
		{"city_empty", "empty", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.SearchStations(ctx, directory.SearchStationsParams{State: "SP", Municipality: trafficEmptyCode, LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := search("SP", trafficEmptyCode, "", 20, "")
			if err != nil {
				return 0, err
			}
			rows, next, err := r.Search(ctx, f)
			if next != "" {
				return 0, fmt.Errorf("empty city returned a cursor")
			}
			return len(rows), err
		}},
		{"text_common", "large", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.SearchStations(ctx, directory.SearchStationsParams{State: "SP", Q: "ESTACAO", LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := search("SP", "", "ESTACAO", 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Search(ctx, f)
			return len(rows), err
		}},
		{"text_rare", "small", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.SearchStations(ctx, directory.SearchStationsParams{State: "SP", Q: "00000007", LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := search("SP", "", "00000007", 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Search(ctx, f)
			return len(rows), err
		}},
		{"uf_only", "large", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.SearchStations(ctx, directory.SearchStationsParams{State: "SP", LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := search("SP", "", "", 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Search(ctx, f)
			return len(rows), err
		}},
		{"detail_hit", "point", 1, func(ctx context.Context, q *directory.Queries) (int, error) {
			var uid pgtype.UUID
			if err := uid.Scan(knownID); err != nil {
				return 0, err
			}
			_, err := q.GetStation(ctx, uid)
			return 1, err
		}, func(ctx context.Context, r *Reader) (int, error) {
			_, err := r.Detail(ctx, knownID)
			return 1, err
		}},
		{"nearby_dense", "large", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.NearbyStations(ctx, directory.NearbyStationsParams{Lon: -46.633, Lat: -23.55, RadiusM: 5000, LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := application.ValidateNearby(-23.55, -46.633, 5000, 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Nearby(ctx, f)
			return len(rows), err
		}},
		{"nearby_sparse", "small", 20, func(ctx context.Context, q *directory.Queries) (int, error) {
			rows, err := q.NearbyStations(ctx, directory.NearbyStationsParams{Lon: -40.0, Lat: -15.0, RadiusM: 5000, LimitPlusOne: 21})
			return len(rows), err
		}, func(ctx context.Context, r *Reader) (int, error) {
			f, err := application.ValidateNearby(-15.0, -40.0, 5000, 20, "")
			if err != nil {
				return 0, err
			}
			rows, _, err := r.Nearby(ctx, f)
			return len(rows), err
		}},
	}
}

// walkCity follows keyset cursors to the end; every id must appear
// exactly once in increasing order. It returns page-start cursors so
// middle/last re-entry points stay pinned to real pages.
func walkCity(tb testing.TB, r *Reader, state, city string, limit int) (ids, cursors []string) {
	tb.Helper()
	ctx := context.Background()
	after := ""
	for {
		f, err := application.ValidateSearch(state, city, "", limit, after)
		if err != nil {
			tb.Fatalf("filter: %v", err)
		}
		rows, next, err := r.Search(ctx, f)
		if err != nil {
			tb.Fatalf("search: %v", err)
		}
		cursors = append(cursors, after)
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		if next == "" {
			break
		}
		after = next
		if len(cursors) > 100000 {
			tb.Fatal("pagination did not terminate")
		}
	}
	seen := map[string]int{}
	for i, id := range ids {
		seen[id]++
		if i > 0 && ids[i-1] >= id {
			tb.Fatalf("page order broke at %d", i)
		}
	}
	for id, count := range seen {
		if count != 1 {
			tb.Fatalf("id %s appears %d times", id, count)
		}
	}
	return ids, cursors
}

func TestTrafficPaginationStable(t *testing.T) {
	pool, _ := trafficFreshDB(t)
	dense, _, _ := trafficSeed(t, pool, 2000)
	reader := NewReader(pool)
	ctx := context.Background()
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM directory_stations WHERE state='SP' AND municipality_code=$1`, dense).Scan(&total); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{5, 20, 50} {
		ids, cursors := walkCity(t, reader, "SP", dense, limit)
		if len(ids) != total {
			t.Fatalf("limit %d walked %d, want %d", limit, len(ids), total)
		}
		// Middle and last cursors re-enter the exact remaining suffix:
		// cursor k resumes after page k, i.e. at ids[k*limit].
		for _, k := range []int{len(cursors) / 2, len(cursors) - 1} {
			after := cursors[k]
			var suffix []string
			for {
				f, _ := application.ValidateSearch("SP", dense, "", limit, after)
				rows, next, err := reader.Search(ctx, f)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					suffix = append(suffix, row.ID)
				}
				if next == "" {
					break
				}
				after = next
			}
			want := ids[k*limit:]
			if len(suffix) != len(want) {
				t.Fatalf("limit %d cursor %d suffix %d, want %d", limit, k, len(suffix), len(want))
			}
			for i := range want {
				if suffix[i] != want[i] {
					t.Fatalf("limit %d cursor %d suffix diverges at %d", limit, k, i)
				}
			}
		}
	}
	// Empty city: zero rows and no cursor, never an error.
	ids, _ := walkCity(t, reader, "SP", trafficEmptyCode, 20)
	if len(ids) != 0 {
		t.Fatalf("empty city walked %d rows", len(ids))
	}
}

func TestTrafficNearbyTies(t *testing.T) {
	pool, _ := trafficFreshDB(t)
	dense, _, _ := trafficSeed(t, pool, 2000)
	reader := NewReader(pool)
	ctx := context.Background()
	// Paginate the dense cluster one row at a time: the three
	// identical-distance tie stations must each appear exactly once,
	// ordered by id within the tie, and the whole walk must hold no
	// gaps or duplicates.
	seen := map[string]int{}
	var lastDist float64
	var lastID string
	first := true
	cursor := ""
	for {
		f, err := application.ValidateNearby(-23.55, -46.633, 5000, 1, cursor)
		if err != nil {
			t.Fatal(err)
		}
		rows, next, err := reader.Nearby(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 && next != "" {
			t.Fatalf("limit-1 page returned %d rows", len(rows))
		}
		for _, row := range rows {
			seen[row.ID]++
			if !first && (row.DistanceM < lastDist || (row.DistanceM == lastDist && row.ID <= lastID)) {
				t.Fatalf("nearby order broke at %s", row.ID)
			}
			lastDist, lastID, first = row.DistanceM, row.ID, false
		}
		if next == "" {
			break
		}
		cursor = next
	}
	for _, id := range []string{
		"aaaaaaaa-1111-4111-8111-111111111111",
		"aaaaaaaa-2222-4222-8222-222222222222",
		"aaaaaaaa-3333-4333-8333-333333333333",
	} {
		if seen[id] != 1 {
			t.Fatalf("tie station %s appears %d times", id, seen[id])
		}
	}
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM directory_stations WHERE current_point IS NOT NULL AND ST_DWithin(current_point, ST_SetSRID(ST_MakePoint(-46.633, -23.55), 4326)::geography, 5000)`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("nearby walk covered %d, want %d", len(seen), total)
	}
	_ = dense
}

func TestTrafficNegative(t *testing.T) {
	pool, _ := trafficFreshDB(t)
	reader := NewReader(pool)
	ctx := context.Background()
	if _, err := application.ValidateSearch("SP", trafficEmptyCode, "x", 0, ""); err == nil {
		t.Fatal("limit 0 accepted, want invalid filter")
	}
	if _, err := application.ValidateSearch("SPP", trafficEmptyCode, "", 20, ""); err == nil {
		t.Fatal("bad state accepted, want invalid filter")
	}
	if _, err := application.ValidateSearch("SP", trafficEmptyCode, "", 20, "not-a-uuid"); err == nil {
		t.Fatal("bad cursor accepted, want invalid filter")
	}
	if _, err := reader.Detail(ctx, "00000000-0000-4000-8000-000000000000"); err != application.ErrUnknownStation {
		t.Fatalf("detail miss = %v, want unknown station", err)
	}
}

func TestTrafficRawVsReader(t *testing.T) {
	pool, _ := trafficFreshDB(t)
	dense, sparse, knownID := trafficSeed(t, pool, 2000)
	reader := NewReader(pool)
	ctx := context.Background()
	queries := directory.New(pool)
	for _, workload := range trafficWorkloads(dense, sparse, knownID) {
		rawRows, rawErr := workload.raw(ctx, queries)
		readerRows, readerErr := workload.reader(ctx, reader)
		if (rawErr == nil) != (readerErr == nil) {
			t.Fatalf("%s raw err %v vs Reader err %v", workload.name, rawErr, readerErr)
		}
		if want := min(rawRows, workload.limit); readerRows != want {
			t.Fatalf("%s Reader rows %d, want min(raw %d, limit %d)", workload.name, readerRows, rawRows, workload.limit)
		}
	}
}

// trafficSample runs fn iters times, returning millisecond latencies and
// the error count; errors never contribute a latency sample.
func trafficSample(ctx context.Context, iters int, fn func(context.Context) error) ([]float64, int) {
	lat := make([]float64, 0, iters)
	errs := 0
	for i := 0; i < iters; i++ {
		started := time.Now()
		if err := fn(ctx); err != nil {
			errs++
			continue
		}
		lat = append(lat, float64(time.Since(started).Microseconds())/1000)
	}
	return lat, errs
}

func trafficReport(b *testing.B, layer, name, stratum string, lat []float64, errs, offered, dropped int) {
	b.Helper()
	if len(lat) == 0 {
		b.Fatalf("%s %s: no successful samples (%d errors)", layer, name, errs)
	}
	p50, p95, p99 := percentiles(lat)
	b.ReportMetric(p50, layer+"_"+name+"_p50_ms")
	b.ReportMetric(p95, layer+"_"+name+"_p95_ms")
	b.ReportMetric(float64(errs), layer+"_"+name+"_errors")
	b.Logf("%s %s [%s] n=%d p50=%.2fms p95=%.2fms p99=%.2fms errors=%d offered=%d dropped=%d",
		layer, name, stratum, len(lat), p50, p95, p99, errs, offered, dropped)
}

// closedClients runs itersPerClient back-to-back iterations on each of n
// clients; the returned latencies pool every successful request.
func closedClients(ctx context.Context, n, itersPerClient int, fn func(context.Context) error) ([]float64, int) {
	var mu sync.Mutex
	var lat []float64
	var errs atomic.Int64
	var wg sync.WaitGroup
	for c := 0; c < n; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local, localErrs := trafficSample(ctx, itersPerClient, fn)
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
			errs.Add(int64(localErrs))
		}()
	}
	wg.Wait()
	return lat, int(errs.Load())
}

// openArrival schedules rate requests/s for duration over at most max
// clients; a tick finding every slot busy records a drop (missed
// scheduling is a counter, never silent demand reduction).
func openArrival(ctx context.Context, rate float64, duration time.Duration, max int, next func(int) func(context.Context) error) (lat []float64, errs, offered, dropped int) {
	ticker := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer ticker.Stop()
	deadline := time.Now().Add(duration)
	slots := make(chan struct{}, max)
	var mu sync.Mutex
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
				started := time.Now()
				if err := next(n)(ctx); err != nil {
					mu.Lock()
					errs++
					mu.Unlock()
					return
				}
				mu.Lock()
				lat = append(lat, float64(time.Since(started).Microseconds())/1000)
				mu.Unlock()
			}(tick)
		default:
			dropped++
		}
		tick++
	}
drain:
	wg.Wait()
	return lat, errs, offered, dropped
}

func trafficWarmup(ctx context.Context, iters int, fn func(context.Context) error) {
	for i := 0; i < iters; i++ {
		_ = fn(ctx)
	}
}

// BenchmarkTrafficLayers times every workload at the raw-SQL layer
// (sqlc query only) and the Reader layer (query plus per-row
// hydration), single client, warmed.
func BenchmarkTrafficLayers(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	dense, sparse, knownID := trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	queries := directory.New(pool)
	for _, workload := range trafficWorkloads(dense, sparse, knownID) {
		workload := workload
		rawFn := func(ctx context.Context) error {
			_, err := workload.raw(ctx, queries)
			return err
		}
		readerFn := func(ctx context.Context) error {
			_, err := workload.reader(ctx, reader)
			return err
		}
		trafficWarmup(ctx, 20, rawFn)
		lat, errs := trafficSample(ctx, 50, rawFn)
		trafficReport(b, "raw", workload.name, workload.stratum, lat, errs, 50, 0)
		trafficWarmup(ctx, 20, readerFn)
		lat, errs = trafficSample(ctx, 50, readerFn)
		trafficReport(b, "reader", workload.name, workload.stratum, lat, errs, 50, 0)
	}
	var table, indexes int64
	if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('directory_stations')`).Scan(&table); err != nil {
		b.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT pg_indexes_size('directory_stations')`).Scan(&indexes); err != nil {
		b.Fatal(err)
	}
	b.Logf("sizes table_mib=%.1f indexes_mib=%.1f", float64(table)/1048576, float64(indexes)/1048576)
}

// BenchmarkTrafficPages walks the dense city at 5/20/50-row pages from
// first, middle and last cursors, timing full walks.
func BenchmarkTrafficPages(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	dense, _, _ := trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	for _, limit := range []int{5, 20, 50} {
		_, cursors := walkCity(b, reader, "SP", dense, limit)
		starts := map[string]string{"first": "", "middle": cursors[len(cursors)/2], "last": cursors[len(cursors)-1]}
		for _, name := range []string{"first", "middle", "last"} {
			after := starts[name]
			fn := func(ctx context.Context) error {
				cursor := after
				for {
					f, err := application.ValidateSearch("SP", dense, "", limit, cursor)
					if err != nil {
						return err
					}
					_, next, err := reader.Search(ctx, f)
					if err != nil {
						return err
					}
					if next == "" {
						return nil
					}
					cursor = next
				}
			}
			trafficWarmup(ctx, 3, fn)
			lat, errs := trafficSample(ctx, 10, fn)
			trafficReport(b, "pages", fmt.Sprintf("dense_%d_%s", limit, name), "large", lat, errs, 10, 0)
		}
	}
}

// BenchmarkTrafficClients exercises closed 1/8/32 clients on the dense
// city and nearby workloads plus an open 20 rps mixed arrival stream.
func BenchmarkTrafficClients(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	dense, _, _ := trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	cityFn := func(ctx context.Context) error {
		f, err := application.ValidateSearch("SP", dense, "", 20, "")
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
	for _, clients := range []int{1, 8, 32} {
		for name, fn := range map[string]func(context.Context) error{"city_dense": cityFn, "nearby_dense": nearbyFn} {
			trafficWarmup(ctx, 20, fn)
			lat, errs := closedClients(ctx, clients, 25, fn)
			trafficReport(b, fmt.Sprintf("closed%d", clients), name, "large", lat, errs, clients*25, 0)
		}
	}
	mix := []func(context.Context) error{cityFn, nearbyFn}
	lat, errs, offered, dropped := openArrival(ctx, 20, 5*time.Second, 32, func(n int) func(context.Context) error {
		return mix[n%len(mix)]
	})
	b.ReportMetric(float64(offered), "open_offered")
	b.ReportMetric(float64(dropped), "open_dropped")
	trafficReport(b, "open20rps", "mixed", "mixed", lat, errs, offered, dropped)
}

// BenchmarkTrafficCacheStates pins cold (first touch), warm, restarted
// pool and rotating unseen cities on the dense-city workload.
func BenchmarkTrafficCacheStates(b *testing.B) {
	pool, dsn := trafficFreshDB(b)
	dense, sparse, _ := trafficSeed(b, pool, 100000)
	ctx := context.Background()
	cityFn := func(pool *pgxpool.Pool, city string) func(context.Context) error {
		return func(ctx context.Context) error {
			f, err := application.ValidateSearch("SP", city, "", 20, "")
			if err != nil {
				return err
			}
			_, _, err = NewReader(pool).Search(ctx, f)
			return err
		}
	}
	// Cold: first touch on the fresh pool, no warmup.
	lat, errs := trafficSample(ctx, 20, cityFn(pool, dense))
	trafficReport(b, "cache", "cold_dense", "large", lat, errs, 20, 0)
	// Warm: past-100-iteration steady state.
	trafficWarmup(ctx, 100, cityFn(pool, dense))
	lat, errs = trafficSample(ctx, 50, cityFn(pool, dense))
	trafficReport(b, "cache", "warm_dense", "large", lat, errs, 50, 0)
	// Restarted: same database, brand-new pool (OS/DB caches unknown,
	// recorded as such, never claimed cold).
	pool.Close()
	pool = reopenPool(b, dsn)
	trafficWarmup(ctx, 5, cityFn(pool, dense))
	lat, errs = trafficSample(ctx, 50, cityFn(pool, dense))
	trafficReport(b, "cache", "restarted_dense", "large", lat, errs, 50, 0)
	// Rotating: unseen cities every request (sparse sweep + empties).
	rotating := []string{sparse, "3550101", "3550102", trafficEmptyCode, "3550201"}
	var n atomic.Int64
	rotFn := func(ctx context.Context) error {
		city := rotating[int(n.Add(1))%len(rotating)]
		f, err := application.ValidateSearch("SP", city, "", 20, "")
		if err != nil {
			return err
		}
		_, _, err = NewReader(pool).Search(ctx, f)
		return err
	}
	lat, errs = trafficSample(ctx, 50, rotFn)
	trafficReport(b, "cache", "rotating_unseen", "mixed", lat, errs, 50, 0)
}

// BenchmarkTrafficPlans captures diagnostic EXPLAIN for sparse vs dense
// city predicates (estimate vs actual) under default, generic and
// custom plan modes. Diagnostic only: never part of timed requests.
func BenchmarkTrafficPlans(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	dense, sparse, _ := trafficSeed(b, pool, 100000)
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Release()
	explain := func(mode, city string) {
		tx, err := conn.Begin(ctx)
		if err != nil {
			b.Fatal(err)
		}
		// Explicit rollback per case: deferred rollbacks would stack
		// on the shared session and collide the prepared names.
		rolledBack := false
		rollback := func() {
			if !rolledBack {
				rolledBack = true
				_ = tx.Rollback(ctx)
			}
		}
		defer rollback()
		if mode != "default" {
			if _, err := tx.Exec(ctx, "SET LOCAL plan_cache_mode = '"+mode+"'"); err != nil {
				b.Fatalf("plan mode: %v", err)
			}
		}
		var plan string
		// Generic/custom modes need a prepared statement to diverge.
		if mode == "default" {
			err = tx.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
				SELECT id FROM directory_stations
				WHERE ($1::text = '' OR state = $1)
				AND ($2::text = '' OR municipality_code = $2)
				AND id::text > '' ORDER BY id::text LIMIT 21`, "SP", city).Scan(&plan)
		} else {
			// EXECUTE takes literals, not parameters: city codes are
			// digits by construction, anything else refuses loudly.
			// Unique statement names per case: the shared session
			// keeps prepared statements past transaction rollback.
			for _, digit := range city {
				if digit < '0' || digit > '9' {
					b.Fatalf("non-numeric city code %q", city)
				}
			}
			prepared := "traffic_city_" + strings.ReplaceAll(mode, "_", "") + "_" + city
			if _, err := tx.Exec(ctx, `PREPARE `+prepared+` AS
				SELECT id FROM directory_stations
				WHERE ($1::text = '' OR state = $1)
				AND ($2::text = '' OR municipality_code = $2)
				AND id::text > '' ORDER BY id::text LIMIT 21`); err != nil {
				b.Fatalf("prepare: %v", err)
			}
			err = tx.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE `+prepared+`('SP', '`+city+`')`).Scan(&plan)
			if _, derr := tx.Exec(ctx, `DEALLOCATE `+prepared); derr != nil {
				b.Fatalf("deallocate: %v", derr)
			}
		}
		if err != nil {
			b.Fatalf("explain %s %s: %v", mode, city, err)
		}
		head := plan
		if len(head) > 120 {
			head = head[:120]
		}
		b.Logf("plan mode=%s city=%s bytes=%d head=%.120s", mode, city, len(plan), strings.ReplaceAll(head, "\n", " "))
		b.Logf("plan mode=%s city=%s estimate=%s", mode, city, planEstimate(plan))
		var actual int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM directory_stations WHERE state='SP' AND municipality_code=$1`, city).Scan(&actual); err != nil {
			rollback()
			b.Fatal(err)
		}
		rollback()
		b.Logf("plan mode=%s city=%s actual_rows=%d", mode, city, actual)
	}
	for _, mode := range []string{"default", "force_generic_plan", "force_custom_plan"} {
		explain(mode, sparse)
		explain(mode, dense)
	}
}

// planEstimate pulls estimate/actual rows plus planning/execution time
// out of an EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) document for the
// campaign ledger: estimate comes from the outermost Plan node,
// actual rows from its first workers-free execution.
func planEstimate(raw string) string {
	var doc []struct {
		Plan struct {
			PlanRows   float64 `json:"Plan Rows"`
			ActualRows float64 `json:"Actual Rows"`
		} `json:"Plan"`
		PlanningTime  float64 `json:"Planning Time"`
		ExecutionTime float64 `json:"Execution Time"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil || len(doc) == 0 {
		return "unparsed"
	}
	top := doc[0]
	return fmt.Sprintf("est=%.0f actual=%.0f planning=%.2fms exec=%.2fms",
		top.Plan.PlanRows, top.Plan.ActualRows, top.PlanningTime, top.ExecutionTime)
}

// BenchmarkTrafficIndex measures one candidate index at a time with
// fresh statistics against the migrated baseline, reporting build and
// storage costs with the workload deltas. Candidates: a trigram GIN
// for ILIKE text filters when pg_trgm is available, otherwise an
// expression index matching the keyset ORDER BY id::text.
func BenchmarkTrafficIndex(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	_, _, _ = trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	var trgm string
	trgmAvailable := true
	if err := pool.QueryRow(ctx, `SELECT name FROM pg_available_extensions WHERE name='pg_trgm'`).Scan(&trgm); err != nil {
		b.Logf("index: pg_trgm unavailable (%v); trigram hypothesis NOT_AVAILABLE", err)
		trgmAvailable = false
	} else if _, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err != nil {
		b.Logf("index: pg_trgm install refused (%v); NOT_AVAILABLE", err)
		trgmAvailable = false
	}
	textFn := func(q string) func(context.Context) error {
		return func(ctx context.Context) error {
			f, err := application.ValidateSearch("SP", "", q, 20, "")
			if err != nil {
				return err
			}
			_, _, err = reader.Search(ctx, f)
			return err
		}
	}
	timeWorkload := func(label, q string, iters int) (float64, float64) {
		fn := textFn(q)
		trafficWarmup(ctx, 10, fn)
		lat, errs := trafficSample(ctx, iters, fn)
		if errs != 0 {
			b.Fatalf("text workload %s errors: %d", label, errs)
		}
		p50, p95, _ := percentiles(lat)
		b.Logf("index workload %s: p50=%.2fms p95=%.2fms", label, p50, p95)
		return p50, p95
	}
	analyze := func() {
		if _, err := pool.Exec(ctx, `ANALYZE directory_stations`); err != nil {
			b.Fatal(err)
		}
	}
	sizes := func() (table, indexes int64) {
		if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('directory_stations')`).Scan(&table); err != nil {
			b.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT pg_indexes_size('directory_stations')`).Scan(&indexes); err != nil {
			b.Fatal(err)
		}
		return table, indexes
	}
	_, baseIndexes := sizes()
	// Common term matches every row (planner should stay sequential);
	// the rare term is the selective case the GIN candidate targets.
	baseCommonP50, baseCommonP95 := timeWorkload("common", "ESTACAO", 30)
	baseRareP50, baseRareP95 := timeWorkload("rare", "00000007", 30)
	b.Logf("index baseline: common p50=%.2fms p95=%.2fms rare p50=%.2fms p95=%.2fms indexes_mib=%.1f",
		baseCommonP50, baseCommonP95, baseRareP50, baseRareP95, float64(baseIndexes)/1048576)
	tryCandidate := func(name, ddl string) {
		started := time.Now()
		if _, err := pool.Exec(ctx, ddl); err != nil {
			b.Fatalf("candidate %s: %v", name, err)
		}
		buildMS := float64(time.Since(started).Microseconds()) / 1000
		analyze()
		_, candIndexes := sizes()
		candCommonP50, candCommonP95 := timeWorkload("common", "ESTACAO", 30)
		candRareP50, candRareP95 := timeWorkload("rare", "00000007", 30)
		improve := (baseRareP95 - candRareP95) / baseRareP95 * 100
		regress := (candCommonP95 - baseCommonP95) / baseCommonP95 * 100
		verdict := "reject"
		if improve >= 15 && regress <= 10 {
			verdict = "accept-pending-migration-task"
		}
		b.ReportMetric(buildMS, "index_"+name+"_build_ms")
		b.ReportMetric(float64(candIndexes-baseIndexes), "index_"+name+"_bytes")
		b.Logf("index %s: build=%.0fms size_mib=%.1f rare p50=%.2fms p95=%.2fms delta_p95=%+.1f%% common p50=%.2fms p95=%.2fms delta_p95=%+.1f%% verdict=%s",
			name, buildMS, float64(candIndexes-baseIndexes)/1048576, candRareP50, candRareP95, improve, candCommonP50, candCommonP95, regress, verdict)
		if _, err := pool.Exec(ctx, "DROP INDEX "+name); err != nil {
			b.Fatalf("drop %s: %v", name, err)
		}
	}
	if trgmAvailable {
		tryCandidate("rst15_trgm_name", `CREATE INDEX rst15_trgm_name ON directory_stations USING gin (display_name gin_trgm_ops)`)
	} else {
		tryCandidate("rst15_id_text", `CREATE INDEX rst15_id_text ON directory_stations ((id::text))`)
	}
}

// BenchmarkTrafficScaleSpot repeats the dense-city closed-8 workload
// plus selective text on the 1M census: a bounded scale spot-check,
// not the full matrix (which runs at 100k with explicit scale labels).
func BenchmarkTrafficScaleSpot(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	dense, _, _ := trafficSeed(b, pool, 1000000)
	reader := NewReader(pool)
	ctx := context.Background()
	cityFn := func(ctx context.Context) error {
		f, err := application.ValidateSearch("SP", dense, "", 20, "")
		if err != nil {
			return err
		}
		_, _, err = reader.Search(ctx, f)
		return err
	}
	trafficWarmup(ctx, 10, cityFn)
	lat, errs := closedClients(ctx, 8, 10, cityFn)
	trafficReport(b, "spot1m-closed8", "city_dense", "large", lat, errs, 80, 0)
	var table, indexes int64
	if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('directory_stations')`).Scan(&table); err != nil {
		b.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT pg_indexes_size('directory_stations')`).Scan(&indexes); err != nil {
		b.Fatal(err)
	}
	b.Logf("spot1m sizes table_mib=%.1f indexes_mib=%.1f", float64(table)/1048576, float64(indexes)/1048576)
}

// BenchmarkTrafficWeighted pools raw per-workload samples into a
// stated-weight mix (never averaged percentiles): uniform city strata
// plus text/nearby shares on the 100k census.
func BenchmarkTrafficWeighted(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	dense, sparse, knownID := trafficSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	weights := map[string]int{
		"city_dense": 30, "city_sparse": 15, "city_empty": 10,
		"text_common": 10, "text_rare": 5, "uf_only": 5,
		"detail_hit": 5, "nearby_dense": 15, "nearby_sparse": 5,
	}
	byName := map[string]trafficWorkload{}
	for _, workload := range trafficWorkloads(dense, sparse, knownID) {
		byName[workload.name] = workload
	}
	var pooled []float64
	total := 0
	for name, weight := range weights {
		workload := byName[name]
		fn := func(ctx context.Context) error {
			_, err := workload.reader(ctx, reader)
			return err
		}
		trafficWarmup(ctx, 5, fn)
		lat, errs := trafficSample(ctx, weight*2, fn)
		if errs != 0 {
			b.Fatalf("weighted %s errors: %d", name, errs)
		}
		pooled = append(pooled, lat...)
		total += weight * 2
		b.Logf("weighted part %s [%s] n=%d", name, workload.stratum, len(lat))
	}
	sort.Float64s(pooled)
	p50, p95, p99 := percentiles(pooled)
	b.ReportMetric(p50, "weighted_p50_ms")
	b.ReportMetric(p95, "weighted_p95_ms")
	b.ReportMetric(p99, "weighted_p99_ms")
	b.Logf("weighted mix n=%d p50=%.2fms p95=%.2fms p99=%.2fms weights=%v", total, p50, p95, p99, weights)
}
