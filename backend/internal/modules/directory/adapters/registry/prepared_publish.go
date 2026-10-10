package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"

	directory "github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
)

// ErrCuratedFacts is an explicit preserved projection, not a persistence failure.
var ErrCuratedFacts = errors.New("registry: curated source-fact projection preserved")

// PreparedPublicationPorts keeps Directory and StationProfile ownership explicit.
// Composition roots supply these ports; registry never imports either adapter.
type PreparedPublicationPorts struct {
	Canonical     Canonicalizer
	Locality      func(context.Context, string, string, string) error
	EnsureProfile func(context.Context, string) error
	RefreshFacts  func(context.Context, string, string, map[string]string) error
}

// PublishPreparedRegistry publishes only the bound complete registry input, in
// pages of at most 100. PMQC input/candidates are never publication inputs.
// Restart reuses canonical IDs and profiles. No authorization/badge is inferred.
func PublishPreparedRegistry(ctx context.Context, store *PGStore, ports PreparedPublicationPorts, raw []byte) (ReconReport, error) {
	if ports.Canonical == nil || ports.Locality == nil || ports.EnsureProfile == nil {
		return ReconReport{}, fmt.Errorf("registry: publication ports missing")
	}
	run, uid, err := preparedPublicationRun(ctx, store, raw)
	if err != nil {
		return ReconReport{}, err
	}
	report := ReconReport{RunID: run.RunID}
	after, _ := mustUUID("00000000-0000-0000-0000-000000000000")
	for {
		page, err := store.Q.ListPreparedRegistryPage(ctx, directory.ListPreparedRegistryPageParams{RunID: uid, AfterID: after})
		if err != nil {
			return report, err
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			address := map[string]string{}
			if err := json.Unmarshal(row.Address, &address); err != nil {
				return report, err
			}
			stationID, err := ports.Canonical.ResolveStation(ctx, row.SourceKey, row.DisplayName, address)
			if err != nil {
				return report, err
			}
			if ports.RefreshFacts != nil {
				if err := ports.RefreshFacts(ctx, stationID, row.DisplayName, address); err != nil && !errors.Is(err, ErrCuratedFacts) {
					return report, err
				}
			}
			if err := ports.Locality(ctx, stationID, row.MunicipalityCode.String, row.State.String); err != nil {
				return report, err
			}
			if err := ports.EnsureProfile(ctx, stationID); err != nil {
				return report, err
			}
			if err := store.SetAssertionStation(ctx, uuidString(row.ID), stationID); err != nil {
				return report, err
			}
			report.Reconciled++
			after = row.ID
		}
	}
	return VerifyPreparedPublication(ctx, store, raw)
}

// VerifyPreparedPublication checks the immutable manifest binding and that every
// eligible assertion has completed the identity/profile publication sequence.
// It performs no writes and never replaces missing evidence with local state.
func VerifyPreparedPublication(ctx context.Context, store *PGStore, raw []byte) (ReconReport, error) {
	run, uid, err := preparedPublicationRun(ctx, store, raw)
	if err != nil {
		return ReconReport{}, err
	}
	missing, err := store.Q.CountUnpublishedPreparedRows(ctx, uid)
	if err != nil {
		return ReconReport{}, err
	}
	if missing != 0 {
		return ReconReport{}, fmt.Errorf("registry: unpublished prepared rows: %d", missing)
	}
	count, err := store.Q.CountPreparedPublicationRows(ctx, uid)
	if err != nil {
		return ReconReport{}, err
	}
	preserved, err := store.Q.CountPreparedCuratedFacts(ctx, uid)
	if err != nil {
		return ReconReport{}, err
	}
	return ReconReport{RunID: run.RunID, Reconciled: count, Skipped: preserved}, nil
}

func preparedPublicationRun(ctx context.Context, store *PGStore, raw []byte) (Report, pgtype.UUID, error) {
	if len(raw) > preparedMaxManifest {
		return Report{}, pgtype.UUID{}, fmt.Errorf("registry: oversized publication manifest")
	}
	manifest, err := validateBatchManifest(raw)
	if err != nil {
		return Report{}, pgtype.UUID{}, err
	}
	found := false
	for _, input := range manifest.Inputs {
		if input.Key == "registry-13col" {
			found = true
		}
	}
	if !found {
		return Report{}, pgtype.UUID{}, fmt.Errorf("registry: no registry input to publish")
	}
	run, err := store.GetRun(ctx, SourcePrep, "station-prep:"+manifest.RunID+":registry-13col")
	if err != nil {
		return Report{}, pgtype.UUID{}, err
	}
	if run.State != "complete" {
		return Report{}, pgtype.UUID{}, fmt.Errorf("registry: publication requires complete input")
	}
	uid, err := mustUUID(run.RunID)
	if err != nil {
		return Report{}, pgtype.UUID{}, err
	}
	binding, err := store.Q.GetPreparedRunBinding(ctx, uid)
	if err != nil {
		return Report{}, pgtype.UUID{}, err
	}
	sum := sha256.Sum256(raw)
	if binding != hex.EncodeToString(sum[:]) {
		return Report{}, pgtype.UUID{}, fmt.Errorf("registry: publication manifest binding mismatch")
	}
	return run, uid, nil
}
