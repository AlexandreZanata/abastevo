// VPS-only bounded preparation timing driver. No network or database access.
// Each child runs serially in a resource-capped Abastevo Job. Raw source bytes
// remain read-only; only this process's temporary directories are removed.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func run() error {
	if len(os.Args) != 9 {
		return fmt.Errorf("usage: driver PREPARER CSV ALIASES SOURCE EDITION UUID TIMESTAMP EXPECTED_MANIFEST")
	}
	reference, err := os.ReadFile(os.Args[8])
	if err != nil {
		return err
	}
	var manifest struct {
		Outputs []struct{ Path, SHA256 string }
	}
	if err := json.Unmarshal(reference, &manifest); err != nil {
		return err
	}
	if len(manifest.Outputs) != 3 {
		return fmt.Errorf("expected exactly three frozen output hashes")
	}
	for trial := 0; trial <= 5; trial++ {
		// Trial zero warms executable/input page caches; do not pool with trials.
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		dir, err := os.MkdirTemp("/work", "owned-prepare-")
		if err != nil {
			cancel()
			return err
		}
		out := filepath.Join(dir, "published")
		started := time.Now()
		cmd := exec.CommandContext(ctx, os.Args[1], os.Args[2], os.Args[3], out, os.Args[4], os.Args[5], os.Args[6], os.Args[7])
		output, err := cmd.CombinedOutput()
		elapsed := time.Since(started).Seconds()
		cancel()
		if err != nil {
			os.RemoveAll(dir)
			return fmt.Errorf("child trial %d failed: %w", trial, err)
		}
		for _, item := range manifest.Outputs {
			if filepath.Base(item.Path) != item.Path {
				return fmt.Errorf("unsafe reference path")
			}
			file, err := os.Open(filepath.Join(out, item.Path))
			if err != nil {
				return err
			}
			digest := sha256.New()
			_, err = io.Copy(digest, file)
			file.Close()
			if err != nil {
				return err
			}
			if hex.EncodeToString(digest.Sum(nil)) != item.SHA256 {
				return fmt.Errorf("trial %d output checksum differs: %s", trial, item.Path)
			}
		}
		usage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
		row := map[string]any{"trial": trial, "warmup": trial == 0, "wall_seconds": elapsed, "user_seconds": cmd.ProcessState.UserTime().Seconds(), "system_seconds": cmd.ProcessState.SystemTime().Seconds(), "max_rss_kib": usage.Maxrss, "output_hashes_match": true, "accounting": string(output)}
		bytes, _ := json.Marshal(row)
		fmt.Println(string(bytes))
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "vps preparation:", err)
		os.Exit(1)
	}
}
