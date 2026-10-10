//go:build integration

package adapters

import (
	"context"
	"errors"
	dbprofile "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/stationprofile"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/read"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	syncer "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registrysync"
	profile "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/stationprofile/adapters"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type syncTransport func(*http.Request) (*http.Response, error)

func (f syncTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestDailySyncActualRustLoaderRecoveryChangedReadAndOverlap(t *testing.T) {
	preparer := os.Getenv("ANPFUEL_TEST_PREPARER_PATH")
	if preparer == "" {
		t.Fatal("ANPFUEL_TEST_PREPARER_PATH required for selected full pipeline")
	}
	pool, store, canon := freshRegistryDB(t)
	ctx := context.Background()
	profiles := profile.Store{Q: dbprofile.New(pool)}
	ports := registry.PreparedPublicationPorts{Canonical: canon, Locality: canon.SetInitialLocality, RefreshFacts: canon.RefreshPreparedFacts, EnsureProfile: profiles.EnsureUnclaimed}
	fetches, prepares, verifies := 0, 0, 0
	name := "[SYNC-TEST] PIPELINE FIRST"
	interrupted := true
	r := syncer.Runner{Root: t.TempDir(), Now: time.Now, Client: &http.Client{Transport: syncTransport(func(req *http.Request) (*http.Response, error) {
		fetches++
		body := "CODIGOISIMP;AUTORIZACAO;DATAPUBLICACAO;RAZAOSOCIAL;CNPJ;ENDERECO;COMPLEMENTO;BAIRRO;CEP;UF;MUNICIPIO;BANDEIRA;DATAVINCULACAO\nSYNTHETIC-1;SYNTHETIC;01/01/2024;" + name + ";04218406000104;TEST STREET 100;;TEST DISTRICT;01000000;SP;TEST CITY;BRANCA;01/01/2023\n"
		if req.URL.Host == "servicodados.ibge.gov.br" {
			body = `[{"id":3550308,"nome":"TEST CITY","microrregiao":{"mesorregiao":{"UF":{"sigla":"SP"}}}}]`
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	r.Prepare = func(ctx context.Context, dir string, s syncer.Snapshot) error {
		prepares++
		cmd := exec.CommandContext(ctx, preparer, filepath.Join(dir, "registry.csv"), filepath.Join(dir, "aliases.json"), filepath.Join(dir, "prepared"), syncer.ANPURL, "synthetic-only", s.RunID, s.FetchedAt)
		cmd.Env = []string{"LANG=C.UTF-8"}
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("synthetic preparer: %s", raw)
		}
		return err
	}
	apply := func(ctx context.Context, dir string) error {
		manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			return err
		}
		if _, err = registry.LoadPreparedBatch(ctx, pool, manifest, func(n string) (io.ReadCloser, error) { return os.Open(filepath.Join(dir, n)) }); err != nil {
			return err
		}
		if interrupted {
			interrupted = false
			return errors.New("synthetic crash after load before publication")
		}
		_, err = registry.PublishPreparedRegistry(ctx, store, ports, manifest)
		return err
	}
	r.Apply = apply
	r.Verify = func(ctx context.Context, dir string) error {
		verifies++
		m, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
		if err != nil {
			return err
		}
		_, err = registry.VerifyPreparedPublication(ctx, store, m)
		return err
	}
	if err := r.Run(ctx); err == nil {
		t.Fatal("interruption hidden")
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if fetches != 2 || prepares != 1 {
		t.Fatalf("resume fetched/prepared again %d/%d", fetches, prepares)
	}
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if verifies != 1 || prepares != 1 {
		t.Fatal("unchanged did not verify-only")
	}
	var id string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM directory_stations").Scan(&id); err != nil {
		t.Fatal(err)
	}
	name = "[SYNC-TEST] PIPELINE CHANGED"
	blocked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	r.Apply = func(ctx context.Context, dir string) error {
		once.Do(func() { close(blocked) })
		<-release
		return apply(ctx, dir)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-blocked
	second := r
	if err := second.Run(ctx); err == nil {
		t.Fatal("concurrent worker entered")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err := read.NewReader(pool).Detail(ctx, id)
	if err != nil || got.DisplayName != name || got.LocationQuality != "unknown" {
		t.Fatalf("changed canonical read %+v %v", got, err)
	}
	var stations, assertions int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM directory_stations),(SELECT count(*) FROM registry_assertions)").Scan(&stations, &assertions); err != nil || stations != 1 || assertions != 2 {
		t.Fatalf("accounting %d/%d %v", stations, assertions, err)
	}
}
