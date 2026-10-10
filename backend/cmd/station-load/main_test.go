package main

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRetryOnlyTransientPersistenceFailures(t *testing.T) {
	for _, code := range []string{"40001", "40P01", "08006", "57P01"} {
		if !retryable(&pgconn.PgError{Code: code}) {
			t.Fatalf("transient %s refused", code)
		}
	}
	if !retryable(io.ErrUnexpectedEOF) {
		t.Fatal("disconnected transaction must retry idempotently")
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, errors.New("manifest checksum mismatch"), &pgconn.PgError{Code: "23505"}, &pgconn.PgError{Code: "42501"}} {
		if retryable(err) {
			t.Fatalf("non-transient failure retried: %v", err)
		}
	}
}

func TestPreparedRetryIsBoundedAndCancellationStopsBackoff(t *testing.T) {
	calls := 0
	err := retryPreparedOperation(context.Background(), 2, func() error { calls++; return io.ErrUnexpectedEOF })
	if !errors.Is(err, io.ErrUnexpectedEOF) || calls != 2 {
		t.Fatalf("attempts=%d err=%v", calls, err)
	}
	calls = 0
	ctx, cancel := context.WithCancel(context.Background())
	err = retryPreparedOperation(ctx, 3, func() error { calls++; cancel(); return io.EOF })
	if calls != 1 || err == nil {
		t.Fatalf("cancelled attempts=%d err=%v", calls, err)
	}
	calls = 0
	expected := errors.New("checksum mismatch")
	err = retryPreparedOperation(context.Background(), 3, func() error { calls++; return expected })
	if calls != 1 || !errors.Is(err, expected) {
		t.Fatalf("integrity failure attempts=%d err=%v", calls, err)
	}
}

func TestMalformedPreparedManifestIsNotATransientDisconnect(t *testing.T) {
	_, err := registry.LoadPreparedBatch(context.Background(), nil, []byte(`{"format_version":`), nil)
	if err == nil || retryable(err) {
		t.Fatalf("integrity failure must never retry: %v", err)
	}
}
