package registrysync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Snapshot contains only provenance, never a source row or a credential.
type Snapshot struct {
	Key          string `json:"key"`
	Fingerprint  string `json:"fingerprint"`
	SourceHash   string `json:"source_hash"`
	AliasHash    string `json:"alias_hash"`
	RunID        string `json:"run_id"`
	FetchedAt    string `json:"fetched_at"`
	ManifestHash string `json:"manifest_hash,omitempty"`
}
type state struct {
	Version   int       `json:"version"`
	Pending   *Snapshot `json:"pending,omitempty"`
	Success   *Snapshot `json:"success,omitempty"`
	Previous  *Snapshot `json:"previous,omitempty"`
	Retired   *Snapshot `json:"retired,omitempty"`
	CheckedAt string    `json:"checked_at,omitempty"`
}

func validHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && s == hex.EncodeToString(b)
}
func readState(root string) (state, error) {
	s := state{Version: 1}
	path := filepath.Join(root, "state.json")
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return s, fmt.Errorf("unsafe sync state")
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
	if err != nil {
		return s, err
	}
	if len(raw) > 64<<10 {
		return s, fmt.Errorf("state exceeds bound")
	}
	if err = json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("invalid sync state")
	}
	if s.Version != 1 {
		return s, fmt.Errorf("unsupported sync state")
	}
	for _, v := range []*Snapshot{s.Pending, s.Success, s.Previous, s.Retired} {
		if v != nil {
			if _, err := time.Parse(time.RFC3339Nano, v.FetchedAt); err != nil || len(v.RunID) != 36 {
				return s, fmt.Errorf("invalid frozen timestamp/UUID")
			}
		}
		if v != nil && (!validHash(v.Key) || !validHash(v.Fingerprint) || !validHash(v.SourceHash) || !validHash(v.AliasHash) || (v.ManifestHash != "" && !validHash(v.ManifestHash))) {
			return s, fmt.Errorf("invalid snapshot identity")
		}
	}
	return s, nil
}
func saveState(root string, s state) error {
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	return atomicFile(root, "state.json", append(b, '\n'))
}
func atomicFile(root, name string, b []byte) error {
	f, e := os.CreateTemp(root, ".state-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), filepath.Join(root, name)); e != nil {
		return e
	}
	return syncDir(root)
}
func syncDir(root string) error {
	f, e := os.Open(root)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func fileHash(path string, cap int64) (string, error) {
	info, e := os.Lstat(path)
	if e != nil {
		return "", e
	}
	if !info.Mode().IsRegular() || info.Size() > cap {
		return "", fmt.Errorf("unsafe or oversized snapshot file")
	}
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, io.LimitReader(f, cap+1))
	if e != nil || n > cap {
		return "", fmt.Errorf("snapshot hash read failed")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func lock(root string) (func(), error) {
	info, e := os.Lstat(root)
	if e != nil {
		return nil, e
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("unsafe state directory")
	}
	f, e := os.OpenFile(filepath.Join(root, ".lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("another registry refresh owns the state")
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}

func smallFile(path string, cap int64) ([]byte, error) {
	if _, err := fileHash(path, cap); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, cap+1))
}
