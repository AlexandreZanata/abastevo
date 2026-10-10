package adapters

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	community "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/community"
	domain "github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/community/domain"
)

// Store persists observations and decisions.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wires the owned generated queries to a pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	hexed := hex.EncodeToString(b[:])
	return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" +
		hexed[16:20] + "-" + hexed[20:32], nil
}

func mustUUID(text string) (pgtype.UUID, error) {
	raw, err := hex.DecodeString(stripDashes(text))
	if err != nil || len(raw) != 16 {
		return pgtype.UUID{}, errors.New("adapters: malformed UUID")
	}
	var id pgtype.UUID
	copy(id.Bytes[:], raw)
	id.Valid = true
	return id, nil
}

func stripDashes(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '-' {
			out = append(out, s[i])
		}
	}
	return string(out)
}

func uuidString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	hexed := hex.EncodeToString(id.Bytes[:])
	return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" +
		hexed[16:20] + "-" + hexed[20:32]
}

func pgTime(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

// uuidOrNull maps an optional UUID reference, failing loudly on malformed
// input instead of storing a wrong link.
func uuidOrNull(text string) (pgtype.UUID, error) {
	if strings.TrimSpace(text) == "" {
		return pgtype.UUID{}, nil
	}
	return mustUUID(text)
}

// textOrNull maps an optional band: empty means absent (NULL), never a
// stored empty string that later reads as a band.
func textOrNull(text string) pgtype.Text {
	if strings.TrimSpace(text) == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: text, Valid: true}
}

func textValue(v pgtype.Text) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

// Submit persists one observation with its validation job atomically. A
// retry with the same key returns the existing row; a different payload
// under the same key conflicts. The enqueue closure receives the open
// transaction so callers wire the platform queue without this package
// importing it.
func (s *Store) Submit(ctx context.Context, obs domain.Observation, enqueue func(ctx context.Context, tx pgx.Tx, kind string, payload []byte, dedupe string) error) (string, bool, error) {
	uid, err := mustUUID(obs.ID)
	if err != nil {
		return "", false, err
	}
	stationUUID, err := mustUUID(obs.StationID)
	if err != nil {
		return "", false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tq := community.New(tx)
	evidenceUUID, err := uuidOrNull(obs.EvidenceID)
	if err != nil {
		return "", false, err
	}
	supersedesUUID, err := uuidOrNull(obs.SupersedesID)
	if err != nil {
		return "", false, err
	}
	captureUUID, err := uuidOrNull(obs.PhotoCaptureID)
	if err != nil {
		return "", false, err
	}
	inserted, err := tq.InsertObservation(ctx, community.InsertObservationParams{
		ID:                 uid,
		ContributorRef:     obs.ContributorRef,
		ClientSubmissionID: obs.ClientSubmissionID,
		StationID:          stationUUID,
		FuelProduct:        obs.Product,
		Unit:               obs.Unit,
		AmountMilliBrl:     obs.AmountMilli,
		RawPriceText:       obs.RawText,
		ConditionKind:      obs.ConditionKind,
		QualifierKey:       obs.QualifierKey,
		EvidenceID:         evidenceUUID,
		PhotoCaptureID:     captureUUID,
		ReceivedAt:         pgTime(obs.ReceivedAt),
		ClaimedCapturedAt:  pgTime(obs.ClaimedCapturedAt),
		SupersedesID:       supersedesUUID,
		PolicyVersion:      obs.PolicyVersion,
		LocationVerdict:    textOrNull(obs.LocationVerdict),
		LocationProximity:  textOrNull(obs.LocationProximity),
		LocationReason:     textOrNull(obs.LocationReason),
	})
	_ = inserted
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Natural-key conflict: someone holds this submission.
			_ = tx.Rollback(ctx)
			return s.resolveConflict(ctx, obs)
		}
		var conflict *pgconn.PgError
		if errors.As(err, &conflict) && conflict.Code == "23505" {
			// The photo-subset index (000046) can surface as 23505 for an
			// identical concurrent retry that also matches the natural
			// key. Converge identical replays; reject divergent payloads
			// (including a reused photo product under a new key).
			_ = tx.Rollback(ctx)
			if id, existed, rerr := s.resolveConflict(ctx, obs); rerr == nil {
				return id, existed, nil
			} else if rerr != nil && !errors.Is(rerr, domain.ErrConflict) && !errors.Is(rerr, pgx.ErrNoRows) {
				return "", false, rerr
			}
			return "", false, domain.ErrConflict
		}
		return "", false, err
	}
	payload, _ := json.Marshal(map[string]any{"version": 1, "observation_id": obs.ID})
	if err := enqueue(ctx, tx, "validate-observation", payload, "validate:"+obs.ID); err != nil {
		return "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}
	return obs.ID, false, nil
}

// resolveConflict compares a retried submission against the stored fact:
// identical replays converge, divergent payloads conflict per B-BR-005.
func (s *Store) resolveConflict(ctx context.Context, obs domain.Observation) (string, bool, error) {
	row, err := community.New(s.pool).GetObservationByNaturalKey(ctx, community.GetObservationByNaturalKeyParams{
		ContributorRef:     obs.ContributorRef,
		ClientSubmissionID: obs.ClientSubmissionID,
	})
	if err != nil {
		return "", false, err
	}
	if uuidString(row.StationID) != obs.StationID ||
		row.FuelProduct != obs.Product ||
		row.Unit != obs.Unit ||
		row.AmountMilliBrl != obs.AmountMilli ||
		row.ConditionKind != obs.ConditionKind ||
		row.QualifierKey != obs.QualifierKey ||
		uuidString(row.EvidenceID) != obs.EvidenceID ||
		uuidString(row.PhotoCaptureID) != obs.PhotoCaptureID ||
		(obs.PhotoCaptureID != "" && !row.ClaimedCapturedAt.Time.Equal(obs.ClaimedCapturedAt.Truncate(time.Microsecond))) ||
		uuidString(row.SupersedesID) != obs.SupersedesID ||
		textValue(row.LocationVerdict) != obs.LocationVerdict ||
		textValue(row.LocationProximity) != obs.LocationProximity ||
		textValue(row.LocationReason) != obs.LocationReason {
		return "", false, domain.ErrConflict
	}
	return uuidString(row.ID), true, nil
}

// RecordDecision appends one decision after enforcing the machine at the
// boundary: the FromState must match the derived current state, the
// transition must be known, and the sequence must continue exactly.
func (s *Store) RecordDecision(ctx context.Context, d domain.Decision) error {
	if d.EventName() == "" {
		return domain.ErrBadTransition
	}
	obsUUID, err := mustUUID(d.ObservationID)
	if err != nil {
		return err
	}
	q := community.New(s.pool)
	rows, err := q.ListDecisions(ctx, obsUUID)
	if err != nil {
		return err
	}
	current := domain.StateReceived
	for _, row := range rows {
		current = row.ToState
	}
	if d.FromState != current {
		return domain.ErrBadTransition
	}
	if d.Sequence != int64(len(rows)+1) {
		return domain.ErrBadTransition
	}
	idText, err := newUUIDv4()
	if err != nil {
		return err
	}
	id, err := mustUUID(idText)
	if err != nil {
		return err
	}
	_, err = q.AppendDecision(ctx, community.AppendDecisionParams{
		ID:            id,
		ObservationID: obsUUID,
		Sequence:      d.Sequence,
		ToState:       d.ToState,
		ReasonCodes:   append([]string{}, d.ReasonCodes...),
		PolicyVersion: d.PolicyVersion,
		OccurredAt:    pgTime(d.OccurredAt),
		ActorRef:      d.Actor,
	})
	return err
}

// RecordDecisionWithJob appends one decision and enqueues downstream work
// atomically: the VALIDATED decision and its consensus intent commit
// together or not at all, so no validated fact waits without downstream
// work and no consensus job points at an unrecorded transition. Guards
// mirror RecordDecision inside the transaction so concurrent validators
// converge instead of forking history; the enqueue closure receives the
// open transaction, keeping this package decoupled from the jobs table.
func (s *Store) RecordDecisionWithJob(ctx context.Context, d domain.Decision, kind string, payload []byte, dedupe string, enqueue func(ctx context.Context, tx pgx.Tx, kind string, payload []byte, dedupe string) error) error {
	if d.EventName() == "" {
		return domain.ErrBadTransition
	}
	obsUUID, err := mustUUID(d.ObservationID)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tq := community.New(tx)
	rows, err := tq.ListDecisions(ctx, obsUUID)
	if err != nil {
		return err
	}
	current := domain.StateReceived
	for _, row := range rows {
		current = row.ToState
	}
	if d.FromState != current {
		return domain.ErrBadTransition
	}
	if d.Sequence != int64(len(rows)+1) {
		return domain.ErrBadTransition
	}
	idText, err := newUUIDv4()
	if err != nil {
		return err
	}
	id, err := mustUUID(idText)
	if err != nil {
		return err
	}
	if _, err := tq.AppendDecision(ctx, community.AppendDecisionParams{
		ID:            id,
		ObservationID: obsUUID,
		Sequence:      d.Sequence,
		ToState:       d.ToState,
		ReasonCodes:   append([]string{}, d.ReasonCodes...),
		PolicyVersion: d.PolicyVersion,
		OccurredAt:    pgTime(d.OccurredAt),
		ActorRef:      d.Actor,
	}); err != nil {
		return err
	}
	if kind != "" {
		if enqueue == nil {
			return errors.New("adapters: consensus enqueue required for validated transition")
		}
		if err := enqueue(ctx, tx, kind, payload, dedupe); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Observation loads one fact by ID.
func (s *Store) Observation(ctx context.Context, id string) (domain.Observation, error) {
	uid, err := mustUUID(id)
	if err != nil {
		return domain.Observation{}, err
	}
	row, err := community.New(s.pool).GetObservation(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Observation{}, errors.New("adapters: unknown observation")
		}
		return domain.Observation{}, err
	}
	return mapObservation(row), nil
}

// Decisions lists an observation's decision history in sequence order.
func (s *Store) Decisions(ctx context.Context, id string) ([]domain.Decision, error) {
	uid, err := mustUUID(id)
	if err != nil {
		return nil, err
	}
	rows, err := community.New(s.pool).ListDecisions(ctx, uid)
	if err != nil {
		return nil, err
	}
	out := make([]domain.Decision, 0, len(rows))
	for _, row := range rows {
		out = append(out, domain.Decision{
			ObservationID: id,
			Sequence:      row.Sequence,
			ToState:       row.ToState,
			ReasonCodes:   row.ReasonCodes,
			PolicyVersion: row.PolicyVersion,
			OccurredAt:    row.OccurredAt.Time,
			Actor:         row.ActorRef,
		})
	}
	return out, nil
}

func mapObservation(row community.CommunityObservation) domain.Observation {
	var claimed time.Time
	if row.ClaimedCapturedAt.Valid {
		claimed = row.ClaimedCapturedAt.Time
	}
	return domain.Observation{
		ID: row.ID.String(), ContributorRef: row.ContributorRef,
		ClientSubmissionID: row.ClientSubmissionID,
		StationID:          uuidString(row.StationID),
		Product:            row.FuelProduct, Unit: row.Unit,
		AmountMilli: row.AmountMilliBrl, RawText: row.RawPriceText,
		ConditionKind: row.ConditionKind, QualifierKey: row.QualifierKey,
		EvidenceID:        uuidString(row.EvidenceID),
		PhotoCaptureID:    uuidString(row.PhotoCaptureID),
		ReceivedAt:        row.ReceivedAt.Time,
		ClaimedCapturedAt: claimed,
		SupersedesID:      uuidString(row.SupersedesID),
		PolicyVersion:     row.PolicyVersion,
		Freshness:         domain.CaptureFreshness(claimed, row.ReceivedAt.Time),
		LocationVerdict:   textValue(row.LocationVerdict),
		LocationProximity: textValue(row.LocationProximity),
		LocationReason:    textValue(row.LocationReason),
	}
}

// LastObservationSite reports the contributor's most recent observation
// site for teleport review. Absence (no prior fact) is not an error:
// first observations never teleport.
func (s *Store) LastObservationSite(ctx context.Context, contributorRef string) (string, time.Time, bool, error) {
	row, err := community.New(s.pool).LastObservationSite(ctx, contributorRef)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, err
	}
	return uuidString(row.StationID), row.ReceivedAt.Time, true, nil
}

// ListByContributor returns one contributor's facts newest-first with
// keyset pagination over (received_at, id).
func (s *Store) ListByContributor(ctx context.Context, ref string, limit int, after time.Time, afterID string, hasCursor bool) ([]domain.Observation, error) {
	rows, err := community.New(s.pool).ListByContributor(ctx, community.ListByContributorParams{
		ContributorRef: ref,
		HasCursor:      hasCursor,
		AfterTime:      pgTime(after),
		AfterID:        afterID,
		LimitPlusOne:   int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]domain.Observation, 0, len(rows))
	for _, row := range rows {
		out = append(out, mapObservation(row))
	}
	return out, nil
}

// OldestObservation reports the oldest stored fact for the retention
// report (P07-T05): the 24-month observation horizon stays visible
// without touching history. Zero time with no error means no rows.
func (s *Store) OldestObservation(ctx context.Context) (time.Time, error) {
	ts, err := community.New(s.pool).OldestObservation(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	return ts.Time, nil
}
