//go:build integration

package adapters

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dbprofile "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/stationprofile"
	directoryhttp "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/http"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/read"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	profile "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/stationprofile/adapters"
	"github.com/go-chi/chi/v5"
)

func TestPreparedPublicationThroughHTTPAndProfiles(t *testing.T) {
	pool, store, canon := freshRegistryDB(t)
	ctx := context.Background()
	checksum := sha256.Sum256([]byte("owned-synthetic-prepared-publication"))
	hash := hex.EncodeToString(checksum[:])
	assertion := registry.BatchAssertion{
		SchemaVersion: registry.SchemaAssertionV2, Source: "registry-13col", SourceKey: "04218406000104", Checksum: hash,
		BusinessNameNormalized: "[RST-H-TEST] PUBLICATION", AddressRaw: "SYNTHETIC TEST STREET 100",
		AddressNormalized: map[string]string{"street": "SYNTHETIC TEST STREET", "number": "100"},
		MunicipalityIBGE:  "3550308", UF: "SP", AuthState: "unknown", Eligibility: "pending", LocationQuality: "unknown",
	}
	raw, err := json.Marshal(assertion)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	manifest := registry.BatchManifest{FormatVersion: registry.FormatBatch, RunID: "55555555-2222-4333-8444-555555555555", ParserVersion: "station-prep-v0.2.0", PolicyVersion: "station-policy-v1",
		Inputs: []registry.BatchInput{{Key: "registry-13col", Reference: "synthetic-publication.csv", Edition: "synthetic-only", SHA256: hash, Rows: 1}}, Counts: map[string]registry.BatchCounts{"registry-13col": {Input: 1, Accepted: 1}}}
	manifest.Completeness.EOFValidated = true
	manifest.Completeness.ExpectedManifest = true
	streams := map[string][]byte{registry.PrepAssertionsFile: raw, registry.PrepCandidatesFile: nil, registry.PrepQuarantineFile: nil}
	for _, name := range []string{registry.PrepAssertionsFile, registry.PrepCandidatesFile, registry.PrepQuarantineFile} {
		b := streams[name]
		sum := sha256.Sum256(b)
		manifest.Outputs = append(manifest.Outputs, registry.BatchOutput{Path: name, SHA256: hex.EncodeToString(sum[:]), Bytes: uint64(len(b)), Rows: bytes.Count(b, []byte("\n"))})
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	opener := func(name string) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(streams[name])), nil }
	profiles := profile.Store{Q: dbprofile.New(pool)}
	ports := registry.PreparedPublicationPorts{Canonical: canon, Locality: canon.SetInitialLocality, EnsureProfile: profiles.EnsureUnclaimed}
	if _, err := registry.PublishPreparedRegistry(ctx, store, ports, encoded); err == nil {
		t.Fatal("unloaded input published")
	}
	if _, err := registry.LoadPreparedBatch(ctx, pool, encoded, opener); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		report, err := registry.PublishPreparedRegistry(ctx, store, ports, encoded)
		if err != nil || report.Reconciled != 1 {
			t.Fatalf("publication %+v %v", report, err)
		}
	}
	reader := read.NewReader(pool)
	router := chi.NewRouter()
	directoryhttp.Handler{Stations: reader, Secrets: []byte("owned synthetic cursor secret")}.RegisterRoutes(router)
	profile.Handler{Store: profiles, Read: func(ctx context.Context, id string) (string, string, *float64, *float64, error) {
		station, err := reader.Detail(ctx, id)
		return station.DisplayName, station.LocationQuality, nil, nil, err
	}}.RegisterRoutes(router)
	search := httptest.NewRecorder()
	router.ServeHTTP(search, httptest.NewRequest(http.MethodGet, "/v1/stations?municipality_code=3550308&state=SP&limit=20", nil))
	if search.Code != 200 || !strings.Contains(search.Body.String(), "[RST-H-TEST] PUBLICATION") {
		t.Fatalf("city HTTP %d %s", search.Code, search.Body.String())
	}
	var stationID string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM directory_stations").Scan(&stationID); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/stations/"+stationID+"/profile", nil))
	if response.Code != 200 {
		t.Fatalf("profile HTTP %d %s", response.Code, response.Body.String())
	}
	var doc map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc["has_badge"] != false || doc["location_quality"] != "unknown" {
		t.Fatalf("unearned trust %+v", doc)
	}
	if err := canon.SetInitialLocality(ctx, stationID, "4106902", "PR"); err == nil {
		t.Fatal("conflicting locality overwrote source evidence")
	}
	// A source cannot rebind a completed manifest to publish altered facts.
	manifest.Inputs[0].Edition = "changed"
	changed, _ := json.Marshal(manifest)
	if _, err := registry.PublishPreparedRegistry(ctx, store, ports, changed); err == nil {
		t.Fatal("unbound manifest published")
	}
}
