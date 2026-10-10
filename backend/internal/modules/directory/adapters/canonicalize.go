package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/AlexandreZanata/brazil-fuel-prices/backend/db/queries/directory"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/adapters/registry"
	"github.com/AlexandreZanata/brazil-fuel-prices/backend/internal/modules/directory/domain"
)

// RegistryCanonicalizer implements registry.Canonicalizer over the
// canonical repository: stable UUIDs via ResolveCNPJ (concurrent
// callers converge), reviewed points through the location-revision
// chain, explicit status projections. It never merges distinct CNPJs
// and never invents prices.
type RegistryCanonicalizer struct {
	Repo *Repository
}

var _ registry.Canonicalizer = RegistryCanonicalizer{}

func (c RegistryCanonicalizer) ResolveStation(ctx context.Context, cnpj, display string, address map[string]string) (string, error) {
	station, err := c.Repo.ResolveCNPJ(ctx, cnpj, display, address)
	if err != nil {
		return "", err
	}
	return station.ID, nil
}

func (c RegistryCanonicalizer) RecordReviewedPoint(ctx context.Context, stationID string, lat, lon float64, sourceRef string) error {
	rev, err := c.Repo.RecordLocation(ctx, domain.LocationRevision{
		StationID:       stationID,
		PointWKT:        fmt.Sprintf("POINT(%f %f)", lon, lat),
		Quality:         "reviewed",
		Provider:        "registry-api",
		SourceReference: sourceRef,
	})
	if err != nil {
		return err
	}
	_, err = c.Repo.ProjectLocation(ctx, stationID, rev.ID)
	return err
}

func (c RegistryCanonicalizer) SetStatus(ctx context.Context, stationID, status string) error {
	stationUUID, err := mustUUID(stationID)
	if err != nil {
		return err
	}
	_, err = directory.New(c.Repo.pool).UpdateStationStatus(ctx, directory.UpdateStationStatusParams{
		ID:     stationUUID,
		Status: status,
	})
	return err
}

// SetInitialLocality fills source-backed locality without overwriting conflicting
// canonical evidence. A conflict is visible to the importer for operator review.
func (c RegistryCanonicalizer) SetInitialLocality(ctx context.Context, stationID, municipality, state string) error {
	uid, err := mustUUID(stationID)
	if err != nil {
		return err
	}
	_, err = directory.New(c.Repo.pool).SetInitialRegistryLocality(ctx, directory.SetInitialRegistryLocalityParams{
		ID: uid, MunicipalityCode: pgtype.Text{String: municipality, Valid: true}, State: pgtype.Text{String: state, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("registry: locality projection conflict/unavailable: %w", err)
	}
	return nil
}

// RefreshPreparedFacts preserves curated projections: only values supported by
// an earlier bound complete prepared assertion may change. Reviewed location,
// status, locality and profile/community data are outside this update.
func (c RegistryCanonicalizer) RefreshPreparedFacts(ctx context.Context, stationID, display string, address map[string]string) error {
	uid, err := mustUUID(stationID)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(address)
	if err != nil {
		return err
	}
	_, err = directory.New(c.Repo.pool).RefreshPreparedStationFacts(ctx, directory.RefreshPreparedStationFactsParams{ID: uid, DisplayName: display, Address: raw})
	if err != nil {
		return fmt.Errorf("registry: source facts conflict/unavailable: %w", err)
	}
	return nil
}
