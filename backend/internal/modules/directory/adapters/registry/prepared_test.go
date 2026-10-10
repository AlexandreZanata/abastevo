package registry

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestPreparedScannerRejectsOversizeAndMissingTerminator(t *testing.T) {
	for _, raw := range [][]byte{[]byte(strings.Repeat("x", preparedMaxLine+1)), []byte(`{"row":1}`), []byte("\n")} {
		closed := false
		opener := func(string) (io.ReadCloser, error) {
			return &closeProbe{Reader: bytes.NewReader(raw), closed: &closed}, nil
		}
		if err := scanPrepared(context.Background(), opener, PrepAssertionsFile, streamOutput(PrepAssertionsFile, raw), func([]byte) error { return nil }); err == nil {
			t.Fatal("invalid JSONL framing accepted")
		}
		if !closed {
			t.Fatal("failed scan leaked file")
		}
	}
}

type closeProbe struct {
	io.Reader
	closed *bool
}

func (r *closeProbe) Close() error { *r.closed = true; return nil }
