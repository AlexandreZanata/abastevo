// station-sync is a short-lived, daily official registry refresh. Kubernetes
// owns cadence/restarts; the existing preparer and loader own their contracts.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	syncer "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registrysync"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func event(code string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"] = code
	fields["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	_ = json.NewEncoder(os.Stdout).Encode(fields)
}
func main() {
	if err := run(); err != nil {
		event("refresh_failed", map[string]any{"reason": err.Error()})
		os.Exit(1)
	}
}
func run() error {
	root := flag.String("root", "/state", "private persistent state directory")
	preparer := flag.String("prepare", "/tools/prepare_registry", "trusted Rust executable")
	loader := flag.String("loader", "/tools/station-load", "trusted Go loader executable")
	timeout := flag.Duration("timeout", 18*time.Minute, "overall bounded refresh duration")
	flag.Parse()
	if *timeout <= 0 || *timeout > 20*time.Minute || os.Getenv("ANPFUEL_DATABASE_URL") == "" || !filepath.IsAbs(*root) || !filepath.IsAbs(*preparer) || !filepath.IsAbs(*loader) {
		return fmt.Errorf("invalid bounded refresh configuration")
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, *timeout)
	defer cancel()
	safetyClient := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("readiness redirect refused") }}
	if err := safety(ctx, *root, safetyClient); err != nil {
		return err
	}
	done := make(chan struct{})
	guardError := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := safety(ctx, *root, safetyClient); err != nil {
					guardError <- err
					cancel()
					return
				}
			}
		}
	}()
	r := syncer.Runner{Root: *root, Now: time.Now, Client: &http.Client{Timeout: 30 * time.Second}, Observe: func(code string) { event(code, nil) }}
	r.Prepare = func(ctx context.Context, dir string, s syncer.Snapshot) error {
		return child(ctx, "prepare", *preparer, []string{filepath.Join(dir, "registry.csv"), filepath.Join(dir, "aliases.json"), filepath.Join(dir, "prepared"), syncer.ANPURL, "content-sha256:" + s.SourceHash, s.RunID, s.FetchedAt}, false)
	}
	r.Apply = func(ctx context.Context, dir string) error {
		return child(ctx, "load_publish", *loader, []string{"--emit-dir", dir, "--publish", "--attempts", "3", "--timeout", "14m"}, true)
	}
	r.Verify = func(ctx context.Context, dir string) error {
		return child(ctx, "verify", *loader, []string{"--emit-dir", dir, "--verify", "--timeout", "1m"}, true)
	}
	started := time.Now()
	err := r.Run(ctx)
	cancel()
	<-done
	select {
	case failure := <-guardError:
		return failure
	default:
	}
	if err != nil {
		return err
	}
	event("refresh_complete", map[string]any{"seconds": time.Since(started).Seconds(), "container_memory_peak_bytes": memoryPeak()})
	return nil
}
func child(ctx context.Context, stage, binary string, args []string, db bool) error {
	started := time.Now()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = []string{"LANG=C.UTF-8", "TZ=UTC", "GOMAXPROCS=1", "GOMEMLIMIT=64MiB"}
	if db {
		cmd.Env = append(cmd.Env, "ANPFUEL_DATABASE_URL="+os.Getenv("ANPFUEL_DATABASE_URL"))
	}
	// Source/library errors can contain input fragments; logs expose only stages,
	// exit status and aggregate timing/resource counters, never child output.
	var output countsBuffer
	cmd.Stdout = &output
	cmd.Stderr = io.Discard
	err := cmd.Run()
	fields := map[string]any{"stage": stage, "seconds": time.Since(started).Seconds(), "ok": err == nil}
	if cmd.ProcessState != nil {
		usage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
		fields["child_max_rss_kib"] = usage.Maxrss
		fields["child_cpu_seconds"] = cmd.ProcessState.UserTime().Seconds() + cmd.ProcessState.SystemTime().Seconds()
	}
	fields["counts"] = output.counts()
	event("stage_complete", fields)
	if err != nil {
		return fmt.Errorf("refresh stage %s failed", stage)
	}
	return nil
}

// At most4KiB of known numeric CLI counters are retained. Other child output
// is discarded, including source fragments and persistence error strings.
type countsBuffer struct{ bytes.Buffer }

func (b *countsBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if b.Len() < 4096 {
		keep := 4096 - b.Len()
		if len(p) > keep {
			p = p[:keep]
		}
		_, _ = b.Buffer.Write(p)
	}
	return n, nil
}
func (b *countsBuffer) counts() map[string]uint64 {
	result := map[string]uint64{}
	allowed := map[string]bool{"input": true, "accepted": true, "duplicates": true, "quarantined": true, "rejected": true, "registry_assertions": true, "preserved_curated": true}
	for _, token := range strings.Fields(b.String()) {
		key, value, ok := strings.Cut(token, "=")
		if ok && allowed[key] {
			if count, err := strconv.ParseUint(value, 10, 64); err == nil {
				result[key] = count
			}
		}
	}
	return result
}
