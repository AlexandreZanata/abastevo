//go:build integration

package read

// RST-16 spatial reads and precise-location eligibility cost on real
// PostGIS. Discovery freezes radii 150m/1km/5km/15km (product cap;
// 20km runs raw-SQL diagnostic only, beyond product bounds) over
// dense/sparse/empty/border sites against an independent haversine
// oracle. Eligibility benchmarks the authoritative capture path
// (CheckPhotoCaptureLocation with the production Locate wiring:
// reviewed station plus PostGIS fix distance) separately from
// discovery, preserving fix-integrity preconditions. Faster KNN
// never certifies accuracy and grants no trust: verdicts, not
// latencies, decide eligibility.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
	communityapp "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/community/application"
	directoryadapters "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/application"
	directorydomain "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/domain"
)

// Anchor station: exact known WGS84 point, reviewed, dense city.
const (
	spatialAnchorID  = "bbbbbbbb-1111-4111-8111-111111111111"
	spatialAnchorLat = -23.550000
	spatialAnchorLon = -46.633000
	spatialDenseCity = "3550000"
)

// haversineM is a coarse spherical distance, kept only as a
// cross-check helper; the oracle uses vincentyInverseM below.
func haversineM(latA, lonA, latB, lonB float64) float64 {
	const radius = 6371000.0
	toRad := math.Pi / 180
	latARad, latBRad := latA*toRad, latB*toRad
	half := math.Sin((latB-latA)*toRad/2)*math.Sin((latB-latA)*toRad/2) +
		math.Cos(latARad)*math.Cos(latBRad)*math.Sin((lonB-lonA)*toRad/2)*math.Sin((lonB-lonA)*toRad/2)
	return 2 * radius * math.Asin(math.Sqrt(half))
}

// vincentyEast returns the point nominal metres due east of (lat, lon)
// on WGS84 (Karney-grade agreement with PostGIS geography to <1 cm at
// these ranges; the oracle stays an independent implementation).
func vincentyEast(lat, lon, nominal float64) (float64, float64) {
	const a = 6378137.0
	const f = 1 / 298.257223563
	const b = (1 - f) * a
	phi1 := lat * math.Pi / 180
	alpha1 := math.Pi / 2
	tanU1 := (1 - f) * math.Tan(phi1)
	cosU1 := 1 / math.Sqrt(1+tanU1*tanU1)
	sinU1 := tanU1 * cosU1
	sigma1 := math.Atan2(tanU1, math.Cos(alpha1))
	sinAlpha := cosU1 * math.Sin(alpha1)
	cosSqAlpha := 1 - sinAlpha*sinAlpha
	uSq := cosSqAlpha * (a*a - b*b) / (b * b)
	A := 1 + uSq/16384*(4096+uSq*(-768+uSq*(320-175*uSq)))
	B := uSq / 1024 * (256 + uSq*(-128+uSq*(74-47*uSq)))
	sigma := nominal / (b * A)
	var sigmaPrev float64
	for i := 0; i < 32; i++ {
		twoSigmaM := 2*sigma1 + sigma
		deltaSigma := B * math.Sin(sigma) * (math.Cos(twoSigmaM) +
			B/4*(math.Cos(sigma)*(-1+2*math.Cos(twoSigmaM)*math.Cos(twoSigmaM))-
				B/6*math.Cos(twoSigmaM)*(-3+4*math.Sin(sigma)*math.Sin(sigma))*
					(-3+4*math.Cos(twoSigmaM)*math.Cos(twoSigmaM))))
		sigmaPrev = sigma
		sigma = nominal/(b*A) + deltaSigma
		if math.Abs(sigma-sigmaPrev) < 1e-12 {
			break
		}
	}
	tmp := sinU1*math.Sin(sigma) - cosU1*math.Cos(sigma)*math.Cos(alpha1)
	phi2 := math.Atan2(sinU1*math.Cos(sigma)+cosU1*math.Sin(sigma)*math.Cos(alpha1),
		(1-f)*math.Sqrt(sinAlpha*sinAlpha+tmp*tmp))
	lambda := math.Atan2(math.Sin(sigma)*math.Sin(alpha1), cosU1*math.Cos(sigma)-sinU1*math.Sin(sigma)*math.Cos(alpha1))
	C := f / 16 * cosSqAlpha * (4 + f*(4-3*cosSqAlpha))
	twoSigmaM := 2*sigma1 + sigma
	L := lambda - (1-C)*f*sinAlpha*(sigma+C*math.Sin(sigma)*(math.Cos(twoSigmaM)+C*math.Cos(sigma)*(-1+2*math.Cos(twoSigmaM)*math.Cos(twoSigmaM))))
	return phi2 * 180 / math.Pi, lon + L*180/math.Pi
}

// spatialSeed plants the census plus the boundary rig: the anchor, one
// unreviewed pointed station, one reviewed point-less station and five
// RJ border stations east of the dense cluster.
func spatialSeed(tb testing.TB, pool *pgxpool.Pool, n int) {
	tb.Helper()
	ctx := context.Background()
	seedCensus(tb, pool, n)
	insert := func(id, name, code, state, quality, point string) {
		tb.Helper()
		var uid pgtype.UUID
		if err := uid.Scan(id); err != nil {
			tb.Fatalf("uuid: %v", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO directory_stations
			(id, display_name, address, municipality_code, state, status, current_point, current_quality)
			VALUES ($1, $2, '{}', $3, $4, 'active', `+point+`, $5)`,
			uid, name, code, state, quality); err != nil {
			tb.Fatalf("rig insert: %v", err)
		}
	}
	anchor := fmt.Sprintf("SRID=4326;POINT(%.6f %.6f)", spatialAnchorLon, spatialAnchorLat)
	insert(spatialAnchorID, "[RST16-TEST] ANCHOR", spatialDenseCity, "SP", "reviewed", "'"+anchor+"'::geography")
	insert("bbbbbbbb-2222-4222-8222-222222222222", "[RST16-TEST] UNREVIEWED", spatialDenseCity, "SP", "unknown", "'"+anchor+"'::geography")
	insert("bbbbbbbb-3333-4333-8333-333333333333", "[RST16-TEST] POINTLESS", spatialDenseCity, "SP", "reviewed", "NULL")
	for i := 0; i < 5; i++ {
		point := fmt.Sprintf("SRID=4326;POINT(%.6f %.6f)", spatialAnchorLon+0.018+float64(i)*0.002, spatialAnchorLat+0.001*float64(i))
		insert(fmt.Sprintf("bbbbbbbb-4444-4222-8222-44444444444%d", i), fmt.Sprintf("[RST16-TEST] BORDER %d", i), "3304557", "RJ", "reviewed", "'"+point+"'::geography")
	}
}

// oraclePoints loads every pointed station for the haversine oracle.
func oraclePoints(tb testing.TB, pool *pgxpool.Pool) map[string][2]float64 {
	tb.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id, ST_Y(current_point::geometry), ST_X(current_point::geometry) FROM directory_stations WHERE current_point IS NOT NULL`)
	if err != nil {
		tb.Fatalf("points: %v", err)
	}
	defer rows.Close()
	out := map[string][2]float64{}
	for rows.Next() {
		var id pgtype.UUID
		var lat, lon float64
		if err := rows.Scan(&id, &lat, &lon); err != nil {
			tb.Fatalf("scan: %v", err)
		}
		var buf [16]byte
		copy(buf[:], id.Bytes[:])
		out[fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])] = [2]float64{lat, lon}
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("rows: %v", err)
	}
	return out
}

// vincentyInverseM is the independent ellipsoidal (WGS84) oracle,
// agreeing with PostGIS geography to centimetres; the implementation
// is separate from PostGIS internals (Karney), so agreement is real
// evidence, not shared code.
func vincentyInverseM(latA, lonA, latB, lonB float64) float64 {
	const a = 6378137.0
	const f = 1 / 298.257223563
	const b = (1 - f) * a
	phi1, phi2 := latA*math.Pi/180, latB*math.Pi/180
	L := (lonB - lonA) * math.Pi / 180
	tanU1 := (1 - f) * math.Tan(phi1)
	tanU2 := (1 - f) * math.Tan(phi2)
	cosU1 := 1 / math.Sqrt(1+tanU1*tanU1)
	sinU1 := tanU1 * cosU1
	cosU2 := 1 / math.Sqrt(1+tanU2*tanU2)
	sinU2 := tanU2 * cosU2
	lambda := L
	for i := 0; i < 64; i++ {
		sinSigma := math.Sqrt((cosU2*math.Sin(lambda))*(cosU2*math.Sin(lambda)) +
			(cosU1*sinU2-sinU1*cosU2*math.Cos(lambda))*(cosU1*sinU2-sinU1*cosU2*math.Cos(lambda)))
		if sinSigma == 0 {
			return 0
		}
		cosSigma := sinU1*sinU2 + cosU1*cosU2*math.Cos(lambda)
		sigma := math.Atan2(sinSigma, cosSigma)
		sinAlpha := cosU1 * cosU2 * math.Sin(lambda) / sinSigma
		cosSqAlpha := 1 - sinAlpha*sinAlpha
		cos2SigmaM := cosSigma - 2*sinU1*sinU2/cosSqAlpha
		if math.IsNaN(cos2SigmaM) {
			cos2SigmaM = 0
		}
		C := f / 16 * cosSqAlpha * (4 + f*(4-3*cosSqAlpha))
		lambdaPrev := lambda
		lambda = L + (1-C)*f*sinAlpha*(sigma+C*sinSigma*(cos2SigmaM+C*cosSigma*(-1+2*cos2SigmaM*cos2SigmaM)))
		if math.Abs(lambda-lambdaPrev) < 1e-12 {
			uSq := cosSqAlpha * (a*a - b*b) / (b * b)
			A := 1 + uSq/16384*(4096+uSq*(-768+uSq*(320-175*uSq)))
			B := uSq / 1024 * (256 + uSq*(-128+uSq*(74-47*uSq)))
			deltaSigma := B * sinSigma * (cos2SigmaM + B/4*(cosSigma*(-1+2*cos2SigmaM*cos2SigmaM)-
				B/6*cos2SigmaM*(-3+4*sinSigma*sinSigma)*(-3+4*cos2SigmaM*cos2SigmaM)))
			return b * A * (sigma - deltaSigma)
		}
	}
	return math.NaN()
}

// oracleSet is the expected Nearby id set from the independent oracle.
func oracleSet(points map[string][2]float64, lat, lon float64, radiusM int) map[string]bool {
	out := map[string]bool{}
	for id, point := range points {
		if vincentyInverseM(lat, lon, point[0], point[1]) <= float64(radiusM) {
			out[id] = true
		}
	}
	return out
}

// walkNearby collects the full Nearby id set across keyset pages.
func walkNearby(tb testing.TB, r *Reader, lat, lon float64, radiusM, limit int) map[string]bool {
	tb.Helper()
	ctx := context.Background()
	got := map[string]bool{}
	cursor := ""
	for {
		f, err := application.ValidateNearby(lat, lon, radiusM, limit, cursor)
		if err != nil {
			tb.Fatalf("filter: %v", err)
		}
		rows, next, err := r.Nearby(ctx, f)
		if err != nil {
			tb.Fatalf("nearby: %v", err)
		}
		for _, row := range rows {
			if got[row.ID] {
				tb.Fatalf("duplicate %s in nearby walk", row.ID)
			}
			got[row.ID] = true
		}
		if next == "" {
			return got
		}
		cursor = next
	}
}

type spatialSite struct {
	name       string
	lat, lon   float64
	expectSome bool
}

// TestSpatialOracleAgrees checks the full Nearby result set against the
// haversine oracle at every frozen radius and site, plus a raw-SQL
// 20km diagnostic beyond product bounds.
func TestSpatialOracleAgrees(t *testing.T) {
	pool, _ := trafficFreshDB(t)
	spatialSeed(t, pool, 100000)
	reader := NewReader(pool)
	points := oraclePoints(t, pool)
	sites := []spatialSite{
		{"dense", spatialAnchorLat, spatialAnchorLon, true},
		{"sparse", -15.0, -40.0, false},
		{"empty_ocean", -25.0, -20.0, false},
		{"border_mid", spatialAnchorLat + 0.0005, spatialAnchorLon + 0.009, true},
	}
	for _, site := range sites {
		for _, radius := range []int{150, 1000, 5000, 15000} {
			want := oracleSet(points, site.lat, site.lon, radius)
			got := walkNearby(t, reader, site.lat, site.lon, radius, 100)
			// The Vincenty oracle (ellipsoidal) and PostGIS geography
			// (Karney) must agree to centimetres; the 0.5m band only
			// absorbs float formatting in keyset cursors, so anything
			// outside it is a real oracle failure.
			var diff []string
			for id := range want {
				if !got[id] {
					diff = append(diff, id)
				}
			}
			for id := range got {
				if !want[id] {
					diff = append(diff, id)
				}
			}
			for _, id := range diff {
				var dist float64
				err := pool.QueryRow(context.Background(), `SELECT ST_Distance(current_point,
					ST_SetSRID(ST_MakePoint($2::float8, $3::float8), 4326)::geography)
					FROM directory_stations WHERE id = $1::uuid`, id, site.lon, site.lat).Scan(&dist)
				if err != nil {
					t.Fatal(err)
				}
				if math.Abs(dist-float64(radius)) > 0.5 {
					t.Fatalf("%s r=%d: id %s diverges %.2fm off boundary", site.name, radius, id, dist-float64(radius))
				}
			}
			if len(diff) > 0 {
				t.Logf("%s r=%d: %d boundary-band ids (<=0.5m)", site.name, radius, len(diff))
			}
			if site.expectSome && len(got) == 0 {
				t.Fatalf("%s r=%d: expected non-empty results", site.name, radius)
			}
		}
	}
	// Border lab mix: the mid site at 5km must span both states.
	border := oracleSet(points, spatialAnchorLat+0.0005, spatialAnchorLon+0.009, 5000)
	states := map[string]bool{}
	for id := range border {
		var state string
		if err := pool.QueryRow(context.Background(), `SELECT state FROM directory_stations WHERE id = $1::uuid`, id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		states[state] = true
	}
	if !states["SP"] || !states["RJ"] {
		t.Fatalf("border mix missing a side: %v", states)
	}
	// 20km exceeds the product cap (15000): raw-SQL diagnostic only.
	queries := directory.New(pool)
	raw, err := queries.NearbyStations(context.Background(), directory.NearbyStationsParams{
		Lon: spatialAnchorLon, Lat: spatialAnchorLat, RadiusM: 20000, LimitPlusOne: 100001,
	})
	if err != nil {
		t.Fatalf("raw 20km: %v", err)
	}
	want := oracleSet(points, spatialAnchorLat, spatialAnchorLon, 20000)
	if len(raw) != len(want) {
		t.Fatalf("raw 20km: got %d, oracle %d", len(raw), len(want))
	}
}

// locateReplica wires the production Locate semantics (reviewed station
// plus PostGIS fix distance); unknown or pointless stations resolve to
// SiteUnknown exactly like the composition root.
func locateReplica(pool *pgxpool.Pool, calls *atomic.Int64) func(context.Context, string, float64, float64) (float64, communityapp.StationSite, error) {
	repo := directoryadapters.NewRepository(pool)
	return func(ctx context.Context, stationID string, lat, lon float64) (float64, communityapp.StationSite, error) {
		calls.Add(1)
		st, err := repo.Station(ctx, stationID)
		if err != nil {
			if errors.Is(err, directorydomain.ErrUnknownStation) {
				return 0, communityapp.SiteUnknown, nil
			}
			return 0, "", err
		}
		if st.CurrentPointWKT == "" || st.CurrentQuality != directorydomain.QualityReviewed {
			return 0, communityapp.SiteUnknown, nil
		}
		distanceM, err := repo.FixDistance(ctx, stationID, lon, lat)
		if err != nil {
			if errors.Is(err, directorydomain.ErrUnknownStation) {
				return 0, communityapp.SiteUnknown, nil
			}
			return 0, "", err
		}
		return distanceM, communityapp.SitePrecise, nil
	}
}

func spatialFix(tb testing.TB, lat, lon float64, mutate func(*communityapp.LocationEvidence), now time.Time) *communityapp.LocationEvidence {
	tb.Helper()
	accuracy := 10.0
	captured := now.Add(-30 * time.Second)
	evidence := &communityapp.LocationEvidence{
		ClaimedVerdict: communityapp.FixVerified, PermissionGranted: true, HasFix: true,
		SourceInfoPresent: true, AccuracyMeters: &accuracy,
		CapturedAt: &captured, Latitude: &lat, Longitude: &lon,
	}
	if mutate != nil {
		mutate(evidence)
	}
	return evidence
}

// TestSpatialEligibilityBoundary pins 149.9/150/150.1m fixes from
// Vincenty offsets against real PostGIS distances and the
// authoritative gate, plus fix-integrity negatives that must refuse
// before any database call.
func TestSpatialEligibilityBoundary(t *testing.T) {
	pool, _ := trafficFreshDB(t)
	spatialSeed(t, pool, 2000)
	ctx := context.Background()
	now := time.Now()
	for _, tc := range []struct {
		nominal float64
		allowed bool
	}{
		{149.9, true}, {150.0, true}, {150.1, false}, {10.0, true}, {500.0, false},
	} {
		lat, lon := vincentyEast(spatialAnchorLat, spatialAnchorLon, tc.nominal)
		var calls atomic.Int64
		ports := communityapp.Ports{Locate: locateReplica(pool, &calls)}
		err := communityapp.CheckPhotoCaptureLocation(ctx, ports, spatialAnchorID, spatialFix(t, lat, lon, nil, now), now)
		if (err == nil) != tc.allowed {
			t.Fatalf("nominal %.1fm: allowed=%v err=%v", tc.nominal, tc.allowed, err)
		}
		repo := directoryadapters.NewRepository(pool)
		measured, err := repo.FixDistance(ctx, spatialAnchorID, lon, lat)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(measured-tc.nominal) > 0.05 {
			t.Fatalf("nominal %.1fm measured %.3fm: oracle diverged", tc.nominal, measured)
		}
	}
	// Fix-integrity negatives refuse at 10m without touching the database.
	negatives := map[string]func(*communityapp.LocationEvidence){
		"simulated":     func(l *communityapp.LocationEvidence) { l.Simulated = true },
		"accuracy_101":  func(l *communityapp.LocationEvidence) { *l.AccuracyMeters = 101 },
		"future_fix":    func(l *communityapp.LocationEvidence) { future := now.Add(time.Second); l.CapturedAt = &future },
		"manual":        func(l *communityapp.LocationEvidence) { l.Manual = true },
		"no_permission": func(l *communityapp.LocationEvidence) { l.PermissionGranted = false },
	}
	lat10, lon10 := vincentyEast(spatialAnchorLat, spatialAnchorLon, 10.0)
	for name, mutate := range negatives {
		var calls atomic.Int64
		ports := communityapp.Ports{Locate: locateReplica(pool, &calls)}
		err := communityapp.CheckPhotoCaptureLocation(ctx, ports, spatialAnchorID, spatialFix(t, lat10, lon10, mutate, now), now)
		if !errors.Is(err, communityapp.ErrPhotoCaptureIneligible) {
			t.Fatalf("%s accepted or wrong error: %v", name, err)
		}
		if calls.Load() != 0 {
			t.Fatalf("%s reached the database: preconditions must gate first", name)
		}
	}
	// Unknown, unreviewed and point-less stations stay ineligible.
	for name, id := range map[string]string{
		"unknown":    "00000000-0000-4000-8000-000000000000",
		"unreviewed": "bbbbbbbb-2222-4222-8222-222222222222",
		"pointless":  "bbbbbbbb-3333-4333-8333-333333333333",
	} {
		var calls atomic.Int64
		ports := communityapp.Ports{Locate: locateReplica(pool, &calls)}
		err := communityapp.CheckPhotoCaptureLocation(ctx, ports, id, spatialFix(t, lat10, lon10, nil, now), now)
		if !errors.Is(err, communityapp.ErrPhotoCaptureIneligible) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
}

// spatialExplain extracts the full scan path, examined candidates,
// returned rows, buffers and execution time from EXPLAIN ANALYZE
// JSON. Candidates are the largest examined set over scan nodes;
// results are the outermost actual rows.
func spatialExplain(plan string) (path string, candidates, results int64, buffers, execMS string) {
	var doc []struct {
		Plan struct {
			NodeType   string  `json:"Node Type"`
			ActualRows float64 `json:"Actual Rows"`
		} `json:"Plan"`
		ExecutionTime float64 `json:"Execution Time"`
		SharedHit     float64 `json:"Shared Hit Blocks"`
		SharedRead    float64 `json:"Shared Read Blocks"`
	}
	if err := json.Unmarshal([]byte(plan), &doc); err != nil || len(doc) == 0 {
		return "unparsed", -1, -1, "", ""
	}
	var walk func(raw json.RawMessage)
	var best float64
	var parts []string
	walk = func(raw json.RawMessage) {
		var node struct {
			NodeType    string            `json:"Node Type"`
			IndexName   string            `json:"Index Name"`
			ActualRows  float64           `json:"Actual Rows"`
			ActualLoops float64           `json:"Actual Loops"`
			Plans       []json.RawMessage `json:"Plans"`
		}
		if err := json.Unmarshal(raw, &node); err != nil {
			return
		}
		label := node.NodeType
		if node.IndexName != "" {
			label += "(" + node.IndexName + ")"
		}
		parts = append(parts, label)
		examined := node.ActualRows * node.ActualLoops
		if examined > best {
			best = examined
		}
		for _, child := range node.Plans {
			walk(child)
		}
	}
	// Re-decode the outer document into the recursive walker.
	var outer []struct {
		Plan             json.RawMessage `json:"Plan"`
		ExecutionTime    float64         `json:"Execution Time"`
		SharedHitBlocks  float64         `json:"Shared Hit Blocks"`
		SharedReadBlocks float64         `json:"Shared Read Blocks"`
	}
	if err := json.Unmarshal([]byte(plan), &outer); err != nil || len(outer) == 0 {
		return "unparsed", -1, -1, "", ""
	}
	walk(outer[0].Plan)
	top2 := doc[0]
	return strings.Join(parts, ">"),
		int64(best),
		int64(top2.Plan.ActualRows),
		fmt.Sprintf("hit=%.0f read=%.0f", outer[0].SharedHitBlocks, outer[0].SharedReadBlocks),
		fmt.Sprintf("%.2f", top2.ExecutionTime)
}

// BenchmarkSpatialDiscovery times first-page Nearby at every frozen
// radius and site plus the candidate/result/buffer diagnostic.
func BenchmarkSpatialDiscovery(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	spatialSeed(b, pool, 100000)
	reader := NewReader(pool)
	ctx := context.Background()
	queries := directory.New(pool)
	sites := []spatialSite{
		{"dense", spatialAnchorLat, spatialAnchorLon, true},
		{"sparse", -15.0, -40.0, false},
		{"empty_ocean", -25.0, -20.0, false},
		{"border_mid", spatialAnchorLat + 0.0005, spatialAnchorLon + 0.009, true},
	}
	for _, site := range sites {
		for _, radius := range []int{150, 1000, 5000, 15000} {
			site, radius := site, radius
			fn := func(ctx context.Context) error {
				f, err := application.ValidateNearby(site.lat, site.lon, radius, 20, "")
				if err != nil {
					return err
				}
				_, _, err = reader.Nearby(ctx, f)
				return err
			}
			trafficWarmup(ctx, 10, fn)
			lat, errs := trafficSample(ctx, 20, fn)
			if errs != 0 {
				b.Fatalf("%s r=%d errors: %d", site.name, radius, errs)
			}
			p50, p95, _ := percentiles(lat)
			var plan string
			err := pool.QueryRow(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
				SELECT id FROM directory_stations
				WHERE current_point IS NOT NULL
				AND ST_DWithin(current_point, ST_SetSRID(ST_MakePoint($1::float8, $2::float8), 4326)::geography, $3::int)
				ORDER BY ST_Distance(current_point, ST_SetSRID(ST_MakePoint($1::float8, $2::float8), 4326)::geography) ASC
				LIMIT 21`, site.lon, site.lat, radius).Scan(&plan)
			if err != nil {
				b.Fatalf("explain: %v", err)
			}
			node, candidates, results, buffers, execMS := spatialExplain(plan)
			// Raw layer agreement checks the in-process Reader measurement (no HTTP).
			raw, err := queries.NearbyStations(ctx, directory.NearbyStationsParams{
				Lon: site.lon, Lat: site.lat, RadiusM: int32(radius), LimitPlusOne: 21,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(p50, fmt.Sprintf("nearby_%s_%d_p50_ms", site.name, radius))
			b.ReportMetric(p95, fmt.Sprintf("nearby_%s_%d_p95_ms", site.name, radius))
			b.Logf("nearby %s r=%dm first20 p50=%.2fms p95=%.2fms plan=%s candidates=%d results=%d raw20=%d %s exec=%sms",
				site.name, radius, p50, p95, node, candidates, results, len(raw), buffers, execMS)
		}
	}
}

// BenchmarkSpatialEligibility times the raw PostGIS fix distance
// against the full authoritative gate at an eligible 100m fix.
func BenchmarkSpatialEligibility(b *testing.B) {
	pool, _ := trafficFreshDB(b)
	spatialSeed(b, pool, 100000)
	ctx := context.Background()
	repo := directoryadapters.NewRepository(pool)
	now := time.Now()
	lat, lon := vincentyEast(spatialAnchorLat, spatialAnchorLon, 100.0)
	rawFn := func(ctx context.Context) error {
		_, err := repo.FixDistance(ctx, spatialAnchorID, lon, lat)
		return err
	}
	trafficWarmup(ctx, 10, rawFn)
	rawLat, rawErrs := trafficSample(ctx, 30, rawFn)
	if rawErrs != 0 {
		b.Fatalf("raw eligibility errors: %d", rawErrs)
	}
	gateFn := func(ctx context.Context) error {
		var calls atomic.Int64
		ports := communityapp.Ports{Locate: locateReplica(pool, &calls)}
		return communityapp.CheckPhotoCaptureLocation(ctx, ports, spatialAnchorID, spatialFix(b, lat, lon, nil, now), now)
	}
	trafficWarmup(ctx, 10, gateFn)
	gateLat, gateErrs := trafficSample(ctx, 30, gateFn)
	if gateErrs != 0 {
		b.Fatalf("gate eligibility errors: %d", gateErrs)
	}
	rawP50, rawP95, _ := percentiles(rawLat)
	gateP50, gateP95, _ := percentiles(gateLat)
	b.ReportMetric(rawP95, "eligibility_raw_p95_ms")
	b.ReportMetric(gateP95, "eligibility_gate_p95_ms")
	b.Logf("eligibility 100m: raw p50=%.2fms p95=%.2fms gate p50=%.2fms p95=%.2fms overhead=%+.2fms",
		rawP50, rawP95, gateP50, gateP95, gateP50-rawP50)
}
