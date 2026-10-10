// Command station-load atomically loads one prepared station batch (manifest
// plus assertions/candidates/quarantine streams from station-prep)
// into a directory database through the owned loader. It runs no
// migrations and changes no schema: the target database must already
// be migrated. Exit 0 only when every input report reads complete;
// any other outcome exits 1. --publish resolves registry identities and ensures
// unclaimed profiles after loading; coordinate candidates are never published.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	dbprofile "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/stationprofile"
	directoryadapters "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters"
	profileadapters "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/stationprofile/adapters"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "station-load:", err)
		os.Exit(1)
	}
}

func run() error {
	emitDir := flag.String("emit-dir", "", "prepare_registry output with manifest.json and three JSONL streams")
	dsn := flag.String("dsn", os.Getenv("ANPFUEL_DATABASE_URL"), "postgres DSN (or ANPFUEL_DATABASE_URL)")
	verify := flag.Bool("verify", false, "verify complete bound registry publication without writes")
	publish := flag.Bool("publish", false, "publish complete registry input to Directory and unclaimed profiles; never PMQC candidates")
	attempts := flag.Int("attempts", 3, "bounded retries for transient connection/transaction failures")
	timeout := flag.Duration("timeout", 10*time.Minute, "overall deadline")
	flag.Parse()
	if *emitDir == "" || *dsn == "" {
		return fmt.Errorf("usage: station-load --emit-dir DIR [--dsn URL] [--timeout DURATION]")
	}
	if *attempts < 1 || *attempts > 5 {
		return fmt.Errorf("attempts must be 1..5")
	}
	if *timeout <= 0 || *timeout > 30*time.Minute {
		return fmt.Errorf("timeout must be positive and at most 30 minutes")
	}
	file, err := os.Open(filepath.Join(*emitDir, "manifest.json"))
	if err != nil {
		return err
	}
	manifest, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	file.Close()
	if err != nil {
		return err
	}
	open := func(name string) (io.ReadCloser, error) { return os.Open(filepath.Join(*emitDir, name)) }
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		return fmt.Errorf("invalid database configuration")
	}
	poolConfig.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("pool: %w", err)
	}
	defer pool.Close()
	if *verify {
		if *publish {
			return fmt.Errorf("verify and publish are mutually exclusive")
		}
		report, err := registry.VerifyPreparedPublication(ctx, registry.NewPGStore(pool), manifest)
		if err != nil {
			return err
		}
		fmt.Printf("verification=complete registry_assertions=%d run=%s\n", report.Reconciled, report.RunID)
		return nil
	}
	var reports map[string]registry.Report
	err = retryPreparedOperation(ctx, *attempts, func() error {
		reports, err = registry.LoadPreparedBatch(ctx, pool, manifest, open)
		return err
	})
	if err != nil {
		return err
	}
	failed := false
	for key, report := range reports {
		fmt.Printf("input=%s state=%s accepted=%d duplicates=%d rejected=%d run=%s err=%s\n",
			key, report.State, report.Accepted, report.Duplicates, report.Rejected, report.RunID, report.ErrorCode)
		if report.State != "complete" {
			failed = true
		}
	}
	if failed {
		return fmt.Errorf("batch did not complete on every input")
	}
	if *publish {
		canon := directoryadapters.RegistryCanonicalizer{Repo: directoryadapters.NewRepository(pool)}
		profiles := profileadapters.Store{Q: dbprofile.New(pool)}
		var publication registry.ReconReport
		err = retryPreparedOperation(ctx, *attempts, func() error {
			publication, err = registry.PublishPreparedRegistry(ctx, registry.NewPGStore(pool), registry.PreparedPublicationPorts{
				Canonical: canon, Locality: canon.SetInitialLocality, EnsureProfile: profiles.EnsureUnclaimed, RefreshFacts: canon.RefreshPreparedFacts,
			}, manifest)
			return err
		})
		if err != nil {
			return err
		}
		fmt.Printf("publication=complete registry_assertions=%d run=%s source=station-prep coordinates=unreviewed\n", publication.Reconciled, publication.RunID)
	}
	return nil
}

func retryPreparedOperation(ctx context.Context, attempts int, operation func() error) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil || ctx.Err() != nil || attempt >= attempts || !retryable(err) {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Retrying reopens the same immutable manifest/streams. Transaction rollback or
// an ambiguous commit acknowledgement converges under the prepared-run lock.
func retryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01" || pgErr.Code == "57P01" || strings.HasPrefix(pgErr.Code, "08")
	}
	var connectErr *pgconn.ConnectError
	var transport net.Error
	return errors.As(err, &connectErr) || errors.As(err, &transport) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || pgconn.SafeToRetry(err)
}
