package registry

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"hash"
	"io"
	"strings"
	"time"

	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPreparedStream opens a fresh reader for one of the three fixed names.
type OpenPreparedStream func(string) (io.ReadCloser, error)

const preparedMaxManifest = 1 << 20
const preparedMaxStream = 100 << 20
const preparedMaxLine = 1 << 20
const preparedMaxRows = 500000

// LoadPreparedBatch loads bounded JSONL streams atomically. Exact per-attempt
// dedup lives in a temporary SQL table. Immutable assertion identity and each
// edition's membership are separate. Transaction advisory locking fences replay
// and recovery; a disconnected attempt cannot commit after its successor.
// No fixture, assertion or partial legacy run is deleted during recovery.
func LoadPreparedBatch(ctx context.Context, pool *pgxpool.Pool, raw []byte, open OpenPreparedStream) (map[string]Report, error) {
	if len(raw) > preparedMaxManifest {
		return nil, fmt.Errorf("registry: manifest exceeds 1 MiB")
	}
	manifest, err := validateBatchManifest(raw)
	if err != nil {
		return nil, err
	}
	if len(manifest.Inputs) > 16 || len(manifest.Outputs) != 3 {
		return nil, fmt.Errorf("registry: invalid prepared input/output bounds")
	}
	outputs := map[string]BatchOutput{}
	for _, out := range manifest.Outputs {
		if out.Path != PrepAssertionsFile && out.Path != PrepCandidatesFile && out.Path != PrepQuarantineFile {
			return nil, fmt.Errorf("registry: unexpected output name")
		}
		if _, exists := outputs[out.Path]; exists || out.Bytes > preparedMaxStream || out.Rows < 0 || out.Rows > preparedMaxRows || !validSHA256(out.SHA256) {
			return nil, fmt.Errorf("registry: invalid prepared output bounds/hash")
		}
		outputs[out.Path] = out
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, SourcePrep+":"+manifest.RunID); err != nil {
		return nil, err
	}
	// Server-side attempt state avoids a map growing with national row count.
	if _, err = tx.Exec(ctx, `CREATE TEMP TABLE prepared_attempt_seen (
 input_key text NOT NULL, source_key text NOT NULL, checksum text NOT NULL,
 supersedes text NOT NULL, PRIMARY KEY(input_key,source_key,checksum)) ON COMMIT DROP`); err != nil {
		return nil, err
	}
	store := &PGStore{Q: directory.New(tx)}
	reports := map[string]Report{}
	complete := map[string]bool{}
	for _, input := range manifest.Inputs {
		counts := manifest.Counts[input.Key]
		if counts.Input < 0 || counts.Input > preparedMaxRows || counts.Accepted < 0 || counts.Duplicates < 0 || counts.Quarantined < 0 {
			return nil, fmt.Errorf("registry: invalid prepared input bounds")
		}
		id, created, err := store.CreateRun(ctx, newUUID(), SourcePrep, "station-prep:"+manifest.RunID+":"+input.Key, input.SHA256)
		if err != nil {
			return nil, err
		}
		if !created {
			run, err := store.GetRun(ctx, SourcePrep, "station-prep:"+manifest.RunID+":"+input.Key)
			if err != nil {
				return nil, err
			}
			id = run.RunID
			if run.State == "quarantined" {
				return nil, fmt.Errorf("registry: quarantined input requires review")
			}
			complete[input.Key] = run.State == "complete"
		}
		uid, err := mustUUID(id)
		if err != nil {
			return nil, err
		}
		if _, err = store.Q.BindPreparedManifest(ctx, directory.BindPreparedManifestParams{ID: uid, Checksum: input.SHA256, ManifestSha256: digest}); err != nil {
			return nil, fmt.Errorf("registry: input %q manifest identity conflict: %w", input.Key, err)
		}
		if !complete[input.Key] {
			affected, err := store.Q.ResumePreparedRun(ctx, uid)
			if err != nil {
				return nil, err
			}
			if affected != 1 {
				return nil, fmt.Errorf("registry: run cannot resume")
			}
		}
		reports[input.Key] = Report{RunID: id, State: "running"}
	}
	stage := func(key string, a Assertion, supersedes string) error {
		report, ok := reports[key]
		if !ok {
			return fmt.Errorf("registry: row source matches no input")
		}
		tag, err := tx.Exec(ctx, `INSERT INTO prepared_attempt_seen(input_key,source_key,checksum,supersedes)
 VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, key, a.SourceKey, a.Checksum, supersedes)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var prior string
			if err := tx.QueryRow(ctx, `SELECT supersedes FROM prepared_attempt_seen WHERE input_key=$1 AND source_key=$2 AND checksum=$3`, key, a.SourceKey, a.Checksum).Scan(&prior); err != nil {
				return err
			}
			if prior != supersedes {
				return fmt.Errorf("registry: conflicting duplicate supersedes")
			}
			report.Duplicates++
		} else {
			report.Accepted++
			if !complete[key] {
				if _, err = store.StageAssertion(ctx, a.WithRun(report.RunID)); err != nil {
					return err
				}
			}
			uid, err := mustUUID(report.RunID)
			if err != nil {
				return err
			}
			params := directory.AttachPreparedAssertionParams{RunID: uid, SourceKey: a.SourceKey, Checksum: a.Checksum, SupersedesChecksum: supersedes}
			_, err = store.Q.AttachPreparedAssertion(ctx, params)
			if errors.Is(err, pgx.ErrNoRows) {
				_, err = store.Q.GetPreparedMembership(ctx, directory.GetPreparedMembershipParams{RunID: uid, SourceKey: a.SourceKey, Checksum: a.Checksum, SupersedesChecksum: supersedes})
			}
			if err != nil {
				return err
			}
		}
		reports[key] = report
		return nil
	}
	for _, name := range []string{PrepAssertionsFile, PrepCandidatesFile, PrepQuarantineFile} {
		out := outputs[name]
		err := scanPrepared(ctx, open, name, out, func(line []byte) error {
			switch name {
			case PrepAssertionsFile:
				var row BatchAssertion
				if err := decodeStrict(line, &row, "assertion row"); err != nil {
					return err
				}
				a, err := mapAssertionRow(row)
				if err != nil {
					return err
				}
				supersedes := ""
				if row.Supersedes != nil {
					supersedes = strings.ToLower(*row.Supersedes)
					if supersedes != "" && !validSHA256(supersedes) {
						return fmt.Errorf("registry: malformed supersedes")
					}
				}
				return stage(row.Source, a, supersedes)
			case PrepCandidatesFile:
				var row BatchCandidate
				if err := decodeStrict(line, &row, "candidate row"); err != nil {
					return err
				}
				a, err := mapCandidateRow(row)
				if err != nil {
					return err
				}
				return stage(row.Source, a, "")
			default:
				var row BatchQuarantine
				if err := decodeStrict(line, &row, "quarantine row"); err != nil {
					return err
				}
				report, ok := reports[row.Source]
				if !ok || row.SchemaVersion != SchemaQuarantine || row.RowLocator == "" || !validQuarantineReasons[row.Reason] {
					return fmt.Errorf("registry: invalid quarantine row")
				}
				report.Rejected++
				reports[row.Source] = report
				return nil
			}
		})
		if err != nil {
			return nil, err
		}
	}
	for _, input := range manifest.Inputs {
		report := reports[input.Key]
		want := manifest.Counts[input.Key]
		if report.Accepted != int64(want.Accepted) || report.Duplicates != int64(want.Duplicates) || report.Rejected != int64(want.Quarantined) {
			return nil, fmt.Errorf("registry: input %q count mismatch", input.Key)
		}
		if !complete[input.Key] {
			uid, _ := mustUUID(report.RunID)
			dangling, err := store.Q.PreparedDanglingSupersedes(ctx, uid)
			if err != nil {
				return nil, err
			}
			if dangling != 0 {
				return nil, fmt.Errorf("registry: dangling/conflicting supersedes")
			}
			if _, err := store.Q.LinkPreparedSupersedes(ctx, uid); err != nil {
				return nil, err
			}
			if err := store.FinishRun(ctx, report.RunID, "complete", report.Accepted, report.Duplicates, report.Rejected, ""); err != nil {
				return nil, err
			}
		}
		report.State = "complete"
		reports[input.Key] = report
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return reports, nil
}

type preparedMeter struct {
	hash     hash.Hash
	bytes    uint64
	newlines int
	last     byte
}

func (m *preparedMeter) Write(p []byte) (int, error) {
	m.bytes += uint64(len(p))
	for _, c := range p {
		if c == '\n' {
			m.newlines++
		}
		m.last = c
	}
	return m.hash.Write(p)
}

func scanPrepared(ctx context.Context, open OpenPreparedStream, name string, out BatchOutput, consume func([]byte) error) error {
	reader, err := open(name)
	if err != nil {
		return err
	}
	defer reader.Close()
	meter := &preparedMeter{hash: sha256.New()}
	scanner := bufio.NewScanner(io.TeeReader(io.LimitReader(reader, preparedMaxStream+1), meter))
	scanner.Buffer(make([]byte, 64<<10), preparedMaxLine)
	rows := 0
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			return fmt.Errorf("registry: blank JSONL line")
		}
		rows++
		if rows > out.Rows {
			return fmt.Errorf("registry: %s excess rows", name)
		}
		if err := consume(line); err != nil {
			return fmt.Errorf("registry: %s row %d: %w", name, rows, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("registry: %s scan: %w", name, err)
	}
	if meter.bytes != out.Bytes || meter.bytes > preparedMaxStream || meter.newlines != out.Rows || rows != out.Rows || (meter.bytes > 0 && meter.last != '\n') || hex.EncodeToString(meter.hash.Sum(nil)) != strings.ToLower(out.SHA256) {
		return fmt.Errorf("registry: %s size/rows/checksum mismatch", name)
	}
	return nil
}
