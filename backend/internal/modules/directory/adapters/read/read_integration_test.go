//go:build integration

package read

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	dbmigrations "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/migrations"
	parent "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters"
	application "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/application"
	domain "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/domain"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/platform/migrate"
)

func testDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("ANPFUEL_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://anpfuel:anpfuel@127.0.0.1:5434/anpfuel?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("integration database unreachable (check ANPFUEL_TEST_DATABASE_URL or start infra/compose.dev.yml db): %v", err)
	}
	defer conn.Close(ctx)
	return dsn
}

func freshPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	adminDSN := testDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx)
	name := fmt.Sprintf("read_test_%d", time.Now().UnixNano())
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
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()
	if _, err := migrate.Apply(ctx, dsn, dbmigrations.Files); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// seedCatalog builds A (reviewed, SP), B (reviewed nearby, SP) and C
// (no location, no identifier) and returns their IDs.
func seedCatalog(t *testing.T, pool *pgxpool.Pool) (a, b, c string) {
	t.Helper()
	ctx := context.Background()
	repo := parent.NewRepository(pool)
	mk := func(cnpj, name string, muni, state string) string {
		st, err := repo.ResolveCNPJ(ctx, cnpj, name, map[string]string{"municipio": muni})
		if err != nil {
			t.Fatalf("resolve %s: %v", cnpj, err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE directory_stations SET municipality_code = $2, state = $3 WHERE id = $1`,
			st.ID, muni, state); err != nil {
			t.Fatalf("locate %s: %v", name, err)
		}
		return st.ID
	}
	a = mk("04218406000104", "Posto Alfa", "3550308", "SP")
	b = mk("11222333000181", "Posto Beta", "3550308", "SP")
	c = mk("12ABC34501DE35", "Posto Gama", "3550308", "SP")
	locate := func(id, wkt string) {
		rev, err := repo.RecordLocation(ctx, domain.LocationRevision{
			StationID: id, PointWKT: wkt, Quality: domain.QualityReviewed, Provider: "review",
		})
		if err != nil {
			t.Fatalf("record %s: %v", id, err)
		}
		if _, err := repo.ProjectLocation(ctx, id, rev.ID); err != nil {
			t.Fatalf("project %s: %v", id, err)
		}
	}
	locate(a, "POINT(-46.633 -23.550)")
	locate(b, "POINT(-46.640 -23.555)")
	return a, b, c
}

func TestSearchFlow(t *testing.T) {
	pool := freshPool(t)
	a, _, _ := seedCatalog(t, pool)
	ctx := context.Background()
	r := NewReader(pool)
	got, next, err := r.Search(ctx, application.SearchFilter{Limit: 10})
	if err != nil || len(got) != 3 || next != "" {
		t.Fatalf("search all = %d, next=%q, %v", len(got), next, err)
	}
	if got[0].ID > got[1].ID || got[1].ID > got[2].ID {
		t.Error("search order not by ID")
	}
	filtered, _, err := r.Search(ctx, application.SearchFilter{Q: "Alfa", Limit: 10})
	if err != nil || len(filtered) != 1 || filtered[0].ID != a {
		t.Errorf("q filter = %+v, %v", filtered, err)
	}
	if filtered[0].CNPJNormalized == nil || *filtered[0].CNPJNormalized != "04218406000104" {
		t.Errorf("cnpj = %+v", filtered[0].CNPJNormalized)
	}
	if filtered[0].Coordinates == nil {
		t.Error("reviewed coordinates missing")
	}
	byState, _, err := r.Search(ctx, application.SearchFilter{State: "SP", Limit: 10})
	if err != nil || len(byState) != 3 {
		t.Errorf("state filter = %d, %v", len(byState), err)
	}
	byState, _, err = r.Search(ctx, application.SearchFilter{State: "RJ", Limit: 10})
	if err != nil || len(byState) != 0 {
		t.Errorf("state mismatch = %d, %v", len(byState), err)
	}
	// Wildcards stay literal: no crash, no match-all.
	wild, _, err := r.Search(ctx, application.SearchFilter{Q: "%", Limit: 10})
	if err != nil {
		t.Fatalf("wildcard: %v", err)
	}
	for _, st := range wild {
		if strings.Contains(st.DisplayName, "%") {
			t.Errorf("wildcard matched literally: %q", st.DisplayName)
		}
	}
	// Pagination walk with limit 1 across all three stations.
	p1, n1, err := r.Search(ctx, application.SearchFilter{Limit: 1})
	if err != nil || len(p1) != 1 || n1 == "" {
		t.Fatalf("page one = %+v, next=%q, %v", p1, n1, err)
	}
	p2, n2, err := r.Search(ctx, application.SearchFilter{Limit: 1, AfterID: n1})
	if err != nil || len(p2) != 1 || n2 == "" || p2[0].ID == p1[0].ID {
		t.Fatalf("page two = %+v, next=%q, %v", p2, n2, err)
	}
	p3, n3, err := r.Search(ctx, application.SearchFilter{Limit: 1, AfterID: n2})
	if err != nil || len(p3) != 1 || n3 != "" || p3[0].ID == p2[0].ID {
		t.Fatalf("page three = %+v, next=%q, %v", p3, n3, err)
	}
	// An empty next key terminates the walk; a fetch past the end stays empty.
	empty, noNext, err := r.Search(ctx, application.SearchFilter{Limit: 1, AfterID: p3[0].ID})
	if err != nil || len(empty) != 0 || noNext != "" {
		t.Errorf("past-end page = %+v, next=%q, %v", empty, noNext, err)
	}
}

func TestNearbyFlow(t *testing.T) {
	pool := freshPool(t)
	_, _, c := seedCatalog(t, pool)
	ctx := context.Background()
	r := NewReader(pool)
	got, _, err := r.Nearby(ctx, application.NearbyFilter{Lat: -23.551, Lon: -46.634, RadiusM: 3000, Limit: 10})
	if err != nil {
		t.Fatalf("nearby: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("nearby = %d, want 2 (missing-location excluded)", len(got))
	}
	for _, st := range got {
		if st.ID == c {
			t.Error("station without coordinates fabricated into results")
		}
		if st.Coordinates == nil || st.DistanceM <= 0 {
			t.Errorf("nearby row = %+v", st)
		}
	}
	if got[0].DistanceM > got[1].DistanceM {
		t.Error("nearby order not by distance")
	}
	tight, _, err := r.Nearby(ctx, application.NearbyFilter{Lat: -23.550, Lon: -46.633, RadiusM: 100, Limit: 10})
	if err != nil || len(tight) != 1 {
		t.Errorf("tight radius = %d, %v", len(tight), err)
	}
	// Pagination walk across both positioned stations.
	first, nextKey, err := r.Nearby(ctx, application.NearbyFilter{Lat: -23.551, Lon: -46.634, RadiusM: 3000, Limit: 1})
	if err != nil || len(first) != 1 || nextKey == "" {
		t.Fatalf("page one = %+v, next=%q, %v", first, nextKey, err)
	}
	dist, id, ok := strings.Cut(nextKey, ":")
	if !ok || dist == "" || id == "" {
		t.Fatalf("opaque key malformed: %q", nextKey)
	}
	_ = dist
	second, nextKey, err := r.Nearby(ctx, application.NearbyFilter{
		Lat: -23.551, Lon: -46.634, RadiusM: 3000, Limit: 1,
		AfterDist: mustParseDist(t, dist), AfterID: id, HasCursor: true,
	})
	if err != nil || len(second) != 1 || second[0].ID == first[0].ID || nextKey != "" {
		t.Errorf("page two = %+v, next=%q, %v", second, nextKey, err)
	}
}

func mustParseDist(t *testing.T, s string) float64 {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("distance key %q: %v", s, err)
	}
	return f
}

func TestDetailFlow(t *testing.T) {
	pool := freshPool(t)
	a, _, c := seedCatalog(t, pool)
	ctx := context.Background()
	r := NewReader(pool)
	full, err := r.Detail(ctx, a)
	if err != nil {
		t.Fatalf("detail: %v", err)
	}
	if full.LocationQuality != domain.QualityReviewed || full.Coordinates == nil {
		t.Errorf("detail = %+v", full)
	}
	if full.CurrentRevisionID == nil {
		t.Error("current revision missing")
	}
	bare, err := r.Detail(ctx, c)
	if err != nil {
		t.Fatalf("detail bare: %v", err)
	}
	if bare.Coordinates != nil || bare.LocationQuality != domain.QualityUnknown {
		t.Errorf("missing location dishonest: %+v", bare)
	}
	if bare.CNPJNormalized == nil {
		t.Error("active CNPJ missing")
	}
	if _, err := r.Detail(ctx, "d6c74c23-63db-4c24-a2e5-408cb23bad26"); err == nil {
		t.Error("unknown station accepted")
	}
}

func TestNearbyUsesIndex(t *testing.T) {
	// Proves the spatial path can use the GiST index (planner choice on tiny
	// tables aside, forced with enable_seqscan=off).
	pool := freshPool(t)
	seedCatalog(t, pool)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	var lines []string
	rows, err := tx.Query(ctx, `EXPLAIN (COSTS OFF)
		SELECT id FROM directory_stations
		WHERE current_point IS NOT NULL
		AND ST_DWithin(current_point, ST_SetSRID(ST_MakePoint(-46.634, -23.551), 4326)::geography, 3000)
		ORDER BY ST_Distance(current_point, ST_SetSRID(ST_MakePoint(-46.634, -23.551), 4326)::geography), id::text`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	rows.Close()
	plan := strings.Join(lines, "\n")
	if plan == "" {
		t.Fatal("empty plan")
	}
	if !strings.Contains(plan, "Index") {
		t.Errorf("no index in plan:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan") {
		t.Errorf("seq scan in forced-index plan:\n%s", plan)
	}
}

func TestByCNPJDoesNotCreateOrResolveRetiredIdentifier(t *testing.T) {
	pool := freshPool(t)
	a, _, _ := seedCatalog(t, pool)
	reader := NewReader(pool)
	ctx := context.Background()
	found, err := reader.ByCNPJ(ctx, "04218406000104")
	if err != nil || found.ID != a {
		t.Fatalf("resolve: %+v %v", found, err)
	}
	var before, after int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM directory_stations").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ByCNPJ(ctx, "12345678000195"); err != application.ErrUnknownStation {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE directory_identifiers SET valid_to = now() WHERE normalized_value = $1", "04218406000104"); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ByCNPJ(ctx, "04218406000104"); err != application.ErrUnknownStation {
		t.Fatalf("retired: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM directory_stations").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("read created a station")
	}
}
