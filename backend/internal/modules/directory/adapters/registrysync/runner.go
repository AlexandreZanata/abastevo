// Package registrysync coordinates one bounded official edition using explicit
// prepare/load/verify ports. Its durable state belongs only to this worker.
package registrysync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

const Pipeline = "station-prep-v0.3.0-policy-v2-sync-v1"

type Runner struct {
	Root    string
	Client  *http.Client
	Now     func() time.Time
	Prepare func(context.Context, string, Snapshot) error
	Apply   func(context.Context, string) error
	Verify  func(context.Context, string) error
	Observe func(string)
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Client == nil || r.Now == nil || r.Prepare == nil || r.Apply == nil || r.Verify == nil {
		return fmt.Errorf("refresh ports missing")
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	release, e := lock(r.Root)
	if e != nil {
		return e
	}
	defer release()
	s, e := readState(r.Root)
	if e != nil {
		return e
	}
	if e = r.reap(&s); e != nil {
		return e
	}
	if e = cleanTemporary(r.Root, `^\.download-[0-9]+$`); e != nil {
		return e
	}
	if e = r.recoverOrphan(&s); e != nil {
		return e
	}
	if s.Pending != nil {
		return r.complete(ctx, &s)
	}
	temp, e := os.MkdirTemp(r.Root, ".download-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(temp)
	if e = r.download(ctx, ANPURL, filepath.Join(temp, "registry.csv"), SourceCap); e != nil {
		return e
	}
	if e = r.download(ctx, IBGEURL, filepath.Join(temp, "ibge.json"), ReferenceCap); e != nil {
		return e
	}
	if e = aliases(temp); e != nil {
		return e
	}
	source, e := fileHash(filepath.Join(temp, "registry.csv"), SourceCap)
	if e != nil {
		return e
	}
	reference, e := fileHash(filepath.Join(temp, "aliases.json"), AliasCap)
	if e != nil {
		return e
	}
	hash := sha256.Sum256([]byte(Pipeline + "\n" + source + "\n" + reference))
	fingerprint := hex.EncodeToString(hash[:])
	if s.Success != nil && s.Success.Fingerprint == fingerprint {
		dir, e := snapshotDir(r.Root, *s.Success)
		if e != nil {
			return e
		}
		if e = r.validateFiles(dir, *s.Success); e != nil {
			return e
		}
		if e = r.Verify(ctx, filepath.Join(dir, "prepared")); e != nil {
			return e
		}
		s.CheckedAt = r.Now().UTC().Format(time.RFC3339Nano)
		if e = saveState(r.Root, s); e != nil {
			return e
		}
		r.event("unchanged_verified")
		return nil
	}
	// Each detected edition has its own immutable fetch timestamp. Reverting
	// to formerly seen source bytes must not rebind an earlier manifest UUID.
	stamp := r.Now().UTC().Format(time.RFC3339Nano)
	identity := sha256.Sum256([]byte(fingerprint + "\n" + stamp))
	key := hex.EncodeToString(identity[:])
	identity[6] = (identity[6] & 15) | 0x50
	identity[8] = (identity[8] & 63) | 0x80
	h := hex.EncodeToString(identity[:16])
	id := h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
	snap := Snapshot{Key: key, Fingerprint: fingerprint, SourceHash: source, AliasHash: reference, RunID: id, FetchedAt: stamp}
	dir := filepath.Join(r.Root, key)
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		return fmt.Errorf("snapshot identity collision")
	}
	b, _ := json.Marshal(snap)
	if e = atomicFile(temp, "snapshot.json", b); e != nil {
		return e
	}
	if e = os.Rename(temp, dir); e != nil {
		return e
	}
	if e = syncDir(r.Root); e != nil {
		return e
	}
	s.Pending = &snap
	if e = saveState(r.Root, s); e != nil {
		return e
	}
	return r.complete(ctx, &s)
}
func snapshotDir(root string, s Snapshot) (string, error) {
	if !validHash(s.Key) {
		return "", fmt.Errorf("invalid snapshot path")
	}
	dir := filepath.Join(root, s.Key)
	info, e := os.Lstat(dir)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("missing or unsafe frozen snapshot")
	}
	return dir, nil
}
func (r *Runner) validateFiles(dir string, s Snapshot) error {
	for _, v := range []struct {
		name, hash string
		cap        int64
	}{{"registry.csv", s.SourceHash, SourceCap}, {"aliases.json", s.AliasHash, AliasCap}} {
		h, e := fileHash(filepath.Join(dir, v.name), v.cap)
		if e != nil || h != v.hash {
			return fmt.Errorf("frozen source checksum mismatch")
		}
	}
	if s.ManifestHash != "" {
		info, err := os.Lstat(filepath.Join(dir, "prepared"))
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe prepared snapshot")
		}
		h, e := fileHash(filepath.Join(dir, "prepared", "manifest.json"), 1<<20)
		if e != nil || h != s.ManifestHash {
			return fmt.Errorf("frozen manifest checksum mismatch")
		}
	}
	return nil
}
func (r *Runner) complete(ctx context.Context, s *state) error {
	dir, e := snapshotDir(r.Root, *s.Pending)
	if e != nil {
		return e
	}
	if e = r.validateFiles(dir, *s.Pending); e != nil {
		return e
	}
	if e = cleanTemporary(dir, `^\.station-prep-[0-9]+-[0-9]+$`); e != nil {
		return e
	}
	prepared := filepath.Join(dir, "prepared")
	if info, err := os.Lstat(prepared); err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return fmt.Errorf("unsafe prepared directory")
	}
	if s.Pending.ManifestHash == "" {
		if _, e = os.Lstat(prepared); os.IsNotExist(e) {
			if e = r.Prepare(ctx, dir, *s.Pending); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
		h, e := fileHash(filepath.Join(prepared, "manifest.json"), 1<<20)
		if e != nil {
			return e
		}
		s.Pending.ManifestHash = h
		if e = saveState(r.Root, *s); e != nil {
			return e
		}
	}
	if e = r.Apply(ctx, prepared); e != nil {
		return e
	}
	// No other snapshot or persistent database data is removed on success.
	s.Retired = s.Previous
	s.Previous = s.Success
	s.Success = s.Pending
	s.Pending = nil
	s.CheckedAt = s.Success.FetchedAt
	if e = saveState(r.Root, *s); e != nil {
		return e
	}
	if e = r.reap(s); e != nil {
		return e
	}
	r.event("publication_complete")
	return nil
}
func (r *Runner) event(code string) {
	if r.Observe != nil {
		r.Observe(code)
	}
}

// Reaping is limited to a previously referenced private snapshot, never DB facts.
func (r *Runner) reap(s *state) error {
	if s.Retired == nil {
		return nil
	}
	for _, keep := range []*Snapshot{s.Success, s.Previous, s.Pending} {
		if keep != nil && keep.Key == s.Retired.Key {
			return fmt.Errorf("unsafe retired snapshot")
		}
	}
	dir, e := snapshotDir(r.Root, *s.Retired)
	if e != nil {
		if _, statErr := os.Lstat(filepath.Join(r.Root, s.Retired.Key)); os.IsNotExist(statErr) {
			s.Retired = nil
			return saveState(r.Root, *s)
		}
		return e
	}
	raw, e := smallFile(filepath.Join(dir, "snapshot.json"), 4096)
	if e != nil || len(raw) > 4096 {
		return fmt.Errorf("retired ownership missing")
	}
	var owned Snapshot
	if json.Unmarshal(raw, &owned) != nil || owned.Key != s.Retired.Key || owned.Fingerprint != s.Retired.Fingerprint || owned.SourceHash != s.Retired.SourceHash || owned.RunID != s.Retired.RunID {
		return fmt.Errorf("retired ownership mismatch")
	}
	if e = os.RemoveAll(dir); e != nil {
		return e
	}
	s.Retired = nil
	return saveState(r.Root, *s)
}
func cleanTemporary(root, pattern string) error {
	entries, e := os.ReadDir(root)
	if e != nil {
		return e
	}
	owned := regexp.MustCompile(pattern)
	for _, entry := range entries {
		if owned.MatchString(entry.Name()) && entry.IsDir() {
			if e = os.RemoveAll(filepath.Join(root, entry.Name())); e != nil {
				return e
			}
		}
	}
	return nil
}

func (r *Runner) recoverOrphan(s *state) error {
	entries, err := os.ReadDir(r.Root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !validHash(entry.Name()) {
			continue
		}
		known := false
		for _, keep := range []*Snapshot{s.Pending, s.Success, s.Previous} {
			if keep != nil && keep.Key == entry.Name() {
				known = true
			}
		}
		if known {
			continue
		}
		if s.Pending != nil {
			return fmt.Errorf("multiple pending snapshots require review")
		}
		raw, err := smallFile(filepath.Join(r.Root, entry.Name(), "snapshot.json"), 4096)
		if err != nil {
			return fmt.Errorf("unowned snapshot collision")
		}
		var snap Snapshot
		if json.Unmarshal(raw, &snap) != nil || snap.Key != entry.Name() || !validHash(snap.Fingerprint) || !validHash(snap.SourceHash) || !validHash(snap.AliasHash) {
			return fmt.Errorf("invalid orphan identity")
		}
		dir, err := snapshotDir(r.Root, snap)
		if err != nil {
			return err
		}
		if err = r.validateFiles(dir, snap); err != nil {
			return err
		}
		s.Pending = &snap
	}
	if s.Pending != nil {
		return saveState(r.Root, *s)
	}
	return nil
}
