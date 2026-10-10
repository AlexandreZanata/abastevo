//go:build integration

package adapters

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	dbprofile "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/stationprofile"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/read"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	profile "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/stationprofile/adapters"
	"io"
	"strings"
	"testing"
)

func refreshBatch(t *testing.T, id, name string) ([]byte, registry.OpenPreparedStream) {
	t.Helper()
	sum := sha256.Sum256([]byte(id + name))
	hash := hex.EncodeToString(sum[:])
	a := registry.BatchAssertion{SchemaVersion: registry.SchemaAssertionV2, Source: "registry-13col", SourceKey: "04218406000104", Checksum: hash, BusinessNameNormalized: name, AddressRaw: "SYNTHETIC STREET 100", AddressNormalized: map[string]string{"street": "SYNTHETIC STREET " + name, "number": "100"}, MunicipalityIBGE: "3550308", UF: "SP", AuthState: "unknown", Eligibility: "pending", LocationQuality: "unknown"}
	raw, _ := json.Marshal(a)
	raw = append(raw, '\n')
	streams := map[string][]byte{registry.PrepAssertionsFile: raw, registry.PrepCandidatesFile: nil, registry.PrepQuarantineFile: nil}
	m := registry.BatchManifest{FormatVersion: registry.FormatBatch, RunID: id, ParserVersion: "station-prep-v0.3.0", PolicyVersion: "station-policy-v2", Inputs: []registry.BatchInput{{Key: "registry-13col", Reference: "synthetic-refresh.csv", Edition: id, SHA256: hash, Rows: 1}}, Counts: map[string]registry.BatchCounts{"registry-13col": {Input: 1, Accepted: 1}}}
	m.Completeness.EOFValidated = true
	m.Completeness.ExpectedManifest = true
	for _, n := range []string{registry.PrepAssertionsFile, registry.PrepCandidatesFile, registry.PrepQuarantineFile} {
		b := streams[n]
		s := sha256.Sum256(b)
		m.Outputs = append(m.Outputs, registry.BatchOutput{Path: n, SHA256: hex.EncodeToString(s[:]), Bytes: uint64(len(b)), Rows: bytes.Count(b, []byte("\n"))})
	}
	manifest, _ := json.Marshal(m)
	return manifest, func(n string) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(streams[n])), nil }
}
func TestPreparedDailyRefreshUpdatesOwnedFactsSkipsUnchangedAndPreservesConflicts(t *testing.T) {
	pool, store, canon := freshRegistryDB(t)
	ctx := context.Background()
	profiles := profile.Store{Q: dbprofile.New(pool)}
	calls := 0
	ports := registry.PreparedPublicationPorts{Canonical: canon, Locality: canon.SetInitialLocality, RefreshFacts: canon.RefreshPreparedFacts, EnsureProfile: func(ctx context.Context, id string) error { calls++; return profiles.EnsureUnclaimed(ctx, id) }}
	first, firstOpen := refreshBatch(t, "66666666-2222-4333-8444-555555555555", "[SYNC-TEST] FIRST")
	if _, e := registry.LoadPreparedBatch(ctx, pool, first, firstOpen); e != nil {
		t.Fatal(e)
	}
	if _, e := registry.VerifyPreparedPublication(ctx, store, first); e == nil {
		t.Fatal("unpublished verified")
	}
	for range 2 {
		if _, e := registry.PublishPreparedRegistry(ctx, store, ports, first); e != nil {
			t.Fatal(e)
		}
	}
	if calls != 1 {
		t.Fatalf("unchanged republished %d times", calls)
	}
	if _, e := registry.VerifyPreparedPublication(ctx, store, first); e != nil {
		t.Fatal(e)
	}
	var id string
	_ = pool.QueryRow(ctx, "SELECT id::text FROM directory_stations").Scan(&id)
	changed, open := refreshBatch(t, "77777777-2222-4333-8444-555555555555", "[SYNC-TEST] CHANGED")
	if _, e := registry.LoadPreparedBatch(ctx, pool, changed, open); e != nil {
		t.Fatal(e)
	}
	if _, e := registry.PublishPreparedRegistry(ctx, store, ports, changed); e != nil {
		t.Fatal(e)
	}
	got, e := read.NewReader(pool).Detail(ctx, id)
	if e != nil || got.DisplayName != "[SYNC-TEST] CHANGED" {
		t.Fatalf("updated read %+v %v", got, e)
	}
	// A newly fetched edition can legitimately return to earlier facts. Its
	// membership reuses the old assertion ID but must refresh the projection.
	var revert registry.BatchManifest
	_ = json.Unmarshal(first, &revert)
	revert.RunID = "99999999-2222-4333-8444-555555555555"
	revert.Inputs[0].Edition = "synthetic-reversion"
	reverted, _ := json.Marshal(revert)
	if _, err := registry.LoadPreparedBatch(ctx, pool, reverted, firstOpen); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.PublishPreparedRegistry(ctx, store, ports, reverted); err != nil {
		t.Fatal(err)
	}
	got, e = read.NewReader(pool).Detail(ctx, id)
	if e != nil || got.DisplayName != "[SYNC-TEST] FIRST" {
		t.Fatalf("reverted read %+v %v", got, e)
	}
	if _, e = pool.Exec(ctx, "UPDATE directory_stations SET display_name='[SYNC-TEST] CURATED' WHERE id=$1::uuid", id); e != nil {
		t.Fatal(e)
	}
	conflict, open := refreshBatch(t, "88888888-2222-4333-8444-555555555555", "[SYNC-TEST] THIRD")
	if _, e = registry.LoadPreparedBatch(ctx, pool, conflict, open); e != nil {
		t.Fatal(e)
	}
	if _, e = registry.PublishPreparedRegistry(ctx, store, ports, conflict); e == nil {
		t.Fatal("curated conflict overwritten")
	}
	got, e = read.NewReader(pool).Detail(ctx, id)
	if e != nil || !strings.Contains(got.DisplayName, "CURATED") {
		t.Fatalf("curated read %+v %v", got, e)
	}
	var count int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM directory_stations").Scan(&count)
	if count != 1 {
		t.Fatal("identity duplicated")
	}
}
