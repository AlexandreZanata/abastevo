package registrysync

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixture(t *testing.T) (*Runner, *int, *int, *int) {
	t.Helper()
	fetches, applied, verified := 0, 0, 0
	r := &Runner{Root: t.TempDir(), Now: func() time.Time { return time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC) },
		Client: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
			fetches++
			body := "source snapshot"
			if req.URL.Host == "servicodados.ibge.gov.br" {
				body = `[{"id":3550308,"nome":"TEST CITY","microrregiao":{"mesorregiao":{"UF":{"sigla":"SP"}}}}]`
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})},
		Prepare: func(_ context.Context, dir string, s Snapshot) error {
			if err := os.Mkdir(filepath.Join(dir, "prepared"), 0700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dir, "prepared", "manifest.json"), []byte(`{"synthetic":true}`), 0600)
		},
		Apply: func(context.Context, string) error { applied++; return nil }, Verify: func(context.Context, string) error { verified++; return nil }}
	return r, &fetches, &applied, &verified
}
func TestUnchangedVerifiesWithoutPreparingOrWritingAnotherBatch(t *testing.T) {
	r, fetches, applied, verified := fixture(t)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Prepare = func(context.Context, string, Snapshot) error { t.Fatal("unchanged prepared again"); return nil }
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if *fetches != 4 || *applied != 1 || *verified != 1 {
		t.Fatalf("fetch=%d apply=%d verify=%d", *fetches, *applied, *verified)
	}
}
func TestInterruptedPublicationResumesWithoutRefetchAndPreservesLastSuccess(t *testing.T) {
	r, fetches, _, _ := fixture(t)
	r.Apply = func(context.Context, string) error { return errors.New("synthetic interruption") }
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("failure hidden")
	}
	before := *fetches
	r.Apply = func(context.Context, string) error { return nil }
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if *fetches != before {
		t.Fatal("pending refetched")
	}
	s, err := readState(r.Root)
	if err != nil || s.Pending != nil || s.Success == nil {
		t.Fatalf("state %+v %v", s, err)
	}
}
func TestCorruptPendingAndConcurrentWorkerFailBeforeMutation(t *testing.T) {
	r, fetches, _, _ := fixture(t)
	unlock, err := lock(r.Root)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("overlap allowed")
	}
	unlock()
	if *fetches != 0 {
		t.Fatal("overlap fetched")
	}
	r.Apply = func(context.Context, string) error { return errors.New("interrupt") }
	_ = r.Run(context.Background())
	s, _ := readState(r.Root)
	dir := filepath.Join(r.Root, s.Pending.Key)
	if err := os.WriteFile(filepath.Join(dir, "registry.csv"), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	before := *fetches
	r.Apply = func(context.Context, string) error { t.Fatal("corrupt source published"); return nil }
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("tampering accepted")
	}
	if *fetches != before {
		t.Fatal("tamper refetched")
	}
}
func TestOversizeAndMissingPublicationNeverAdvanceSuccess(t *testing.T) {
	r, _, _, _ := fixture(t)
	r.Client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(io.LimitReader(zeroReader{}, (12<<20)+1)), Header: make(http.Header)}, nil
	})}
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("oversize allowed")
	}
	r, _, _, _ = fixture(t)
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Verify = func(context.Context, string) error { return errors.New("missing DB evidence") }
	if err := r.Run(context.Background()); err == nil {
		t.Fatal("missing evidence hidden")
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestChangedEditionsKeepTwoOwnedSnapshotsAndPreserveNeighbors(t *testing.T) {
	r, _, _, _ := fixture(t)
	client := r.Client.Transport
	edition := 0
	r.Client = &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		response, err := client.RoundTrip(req)
		if req.URL.Host == "www.gov.br" {
			response.Body = io.NopCloser(strings.NewReader(strings.Repeat("changed", edition+1)))
		}
		return response, err
	})}
	neighbor := filepath.Join(r.Root, "manual-neighbor")
	if err := os.Mkdir(neighbor, 0700); err != nil {
		t.Fatal(err)
	}
	for edition = 0; edition < 4; edition++ {
		if err := r.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(r.Root)
	count := 0
	for _, entry := range entries {
		if validHash(entry.Name()) {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("retained snapshot count %d", count)
	}
	if _, err := os.Stat(neighbor); err != nil {
		t.Fatal("neighbor deleted")
	}
}
func TestRenameCrashReusesFrozenIdentityAndPrepareOutput(t *testing.T) {
	r, _, _, _ := fixture(t)
	r.Apply = func(context.Context, string) error { return errors.New("interrupt") }
	if r.Run(context.Background()) == nil {
		t.Fatal("interruption hidden")
	}
	before, _ := readState(r.Root)
	if err := saveState(r.Root, state{Version: 1}); err != nil {
		t.Fatal(err)
	}
	r.Prepare = func(context.Context, string, Snapshot) error { t.Fatal("frozen output rewritten"); return nil }
	r.Apply = func(context.Context, string) error { return nil }
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := readState(r.Root)
	if after.Success.RunID != before.Pending.RunID || after.Success.FetchedAt != before.Pending.FetchedAt {
		t.Fatal("frozen identity changed")
	}
}
func TestInvalidIBGEAndCancellationPreserveEmptyState(t *testing.T) {
	for _, raw := range []string{`[]`, `[{"id":3550308,"nome":"X"}]`, `[{"id":3550308,"nome":"X","microrregiao":{"mesorregiao":{"UF":{"sigla":"SP"}}}}] trailing`} {
		r, _, _, _ := fixture(t)
		base := r.Client.Transport
		r.Client = &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
			response, err := base.RoundTrip(req)
			if req.URL.Host == "servicodados.ibge.gov.br" {
				response.Body = io.NopCloser(strings.NewReader(raw))
			}
			return response, err
		})}
		if r.Run(context.Background()) == nil {
			t.Fatalf("invalid IBGE allowed %q", raw)
		}
		s, _ := readState(r.Root)
		if s.Success != nil || s.Pending != nil {
			t.Fatal("invalid input advanced state")
		}
	}
	r, fetches, _, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r.Run(ctx) == nil || *fetches != 0 {
		t.Fatal("cancelled work started")
	}
}

func TestRedirectAndDecodedGzipBoundsAreRefused(t *testing.T) {
	r, _, _, _ := fixture(t)
	calls := 0
	r.Client = &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.URL.Host != "www.gov.br" {
			t.Fatal("foreign host contacted")
		}
		return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"https://example.invalid/untrusted"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if r.Run(context.Background()) == nil || calls != 1 {
		t.Fatal("foreign redirect accepted")
	}
	r, _, _, _ = fixture(t)
	var compressed bytes.Buffer
	zip := gzip.NewWriter(&compressed)
	_, _ = io.Copy(zip, io.LimitReader(zeroReader{}, SourceCap+1))
	_ = zip.Close()
	r.Client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Encoding": []string{"gzip"}}, Body: io.NopCloser(bytes.NewReader(compressed.Bytes()))}, nil
	})}
	if r.Run(context.Background()) == nil {
		t.Fatal("decompressed bytes exceeded bound")
	}
}
