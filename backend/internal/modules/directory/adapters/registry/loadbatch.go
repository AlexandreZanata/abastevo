package registry

// Batch loader for Rust-prepared station batches (RST-05). It validates a
// versioned manifest plus its three JSONL streams, then stages assertions
// through the existing Store ownership: per-input runs, idempotent replay
// keys, complete-run gating. Staging never publishes: only complete runs
// reconcile, and failed runs stay invisible to publishers. PMQC candidates
// stage with their original CRS and unknown quality; only reviewed quality
// ever projects through the location chain, so no loader output can pass
// the 150m gate on its own.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// SourcePrep is the frozen source key for Rust-prepared batches. It is a
// distinct source from the CSV/API snapshots: provenance stays honest and
// official-lookup reads keep their current scope until a tested
// publication change says otherwise.
const SourcePrep = "station-prep"

// Prep stream file names emitted by station-prep.
const (
	PrepAssertionsFile = "assertions.jsonl"
	PrepCandidatesFile = "candidates.jsonl"
	PrepQuarantineFile = "quarantine.jsonl"
)

// Row schema versions frozen by the RST-01 contract.
const (
	SchemaAssertion   = "station-assertion-v1"
	SchemaAssertionV2 = "station-assertion-v2"
	SchemaCandidate   = "station-coordinate-candidate-v1"
	SchemaQuarantine  = "station-quarantine-v1"
	FormatBatch       = "station-batch-v1"
)

// BatchManifest mirrors station-batch-v1 (loader subset). StartedAt
// and EndedAt are recorded run provenance (RST-13 emitters always set
// them); they stay optional so pre-existing manifests keep loading,
// and no staging decision reads them.
type BatchManifest struct {
	FormatVersion         string `json:"format_version"`
	RunID                 string `json:"run_id"`
	ParserVersion         string `json:"parser_version"`
	PolicyVersion         string `json:"policy_version"`
	StartedAt             string `json:"started_at"`
	EndedAt               string `json:"ended_at"`
	MunicipalityReference struct {
		Reference     string `json:"reference"`
		ReferenceHash string `json:"reference_hash"`
	} `json:"municipality_reference"`
	Inputs       []BatchInput           `json:"inputs"`
	Counts       map[string]BatchCounts `json:"counts"`
	Completeness struct {
		EOFValidated     bool `json:"eof_validated"`
		ExpectedManifest bool `json:"expected_manifest"`
	} `json:"completeness"`
	Outputs []BatchOutput `json:"outputs"`
}

// BatchInput is one fingerprinted manifest input.
type BatchInput struct {
	Key        string `json:"key"`
	Reference  string `json:"reference"`
	Edition    string `json:"edition"`
	EditionSeq uint64 `json:"edition_seq"`
	SHA256     string `json:"sha256"`
	Bytes      uint64 `json:"bytes"`
	Rows       int    `json:"rows"`
	Encoding   string `json:"encoding"`
}

// BatchCounts is the per-input row accounting.
type BatchCounts struct {
	Input       int `json:"input"`
	Accepted    int `json:"accepted"`
	Duplicates  int `json:"duplicates"`
	Quarantined int `json:"quarantined"`
}

// BatchOutput is one hashed manifest output.
type BatchOutput struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  uint64 `json:"bytes"`
	Rows   int    `json:"rows"`
}

// BatchStreams carries the three JSONL streams described by the manifest.
type BatchStreams struct {
	Assertions []byte
	Candidates []byte
	Quarantine []byte
}

// BatchAssertion mirrors station-assertion-v1 (loader subset).
type BatchAssertion struct {
	SchemaVersion          string            `json:"schema_version"`
	Source                 string            `json:"source"`
	SourceKey              string            `json:"source_key"`
	Checksum               string            `json:"checksum"`
	SimpRef                string            `json:"simp_ref"`
	AuthorizationRef       string            `json:"authorization_ref"`
	BusinessNameRaw        string            `json:"business_name_raw"`
	BusinessNameNormalized string            `json:"business_name_normalized"`
	AddressRaw             string            `json:"address_raw"`
	AddressNormalized      map[string]string `json:"address_normalized"`
	UF                     string            `json:"uf"`
	MunicipalityNameRaw    string            `json:"municipality_name_raw"`
	MunicipalityIBGE       string            `json:"municipality_ibge"`
	BrandRaw               string            `json:"brand_raw"`
	PublishedAt            string            `json:"published_at"`
	EffectiveAt            string            `json:"effective_at"`
	BrandLinkedAt          string            `json:"brand_linked_at,omitempty"`
	AuthState              string            `json:"auth_state"`
	Eligibility            string            `json:"eligibility"`
	AuthEvidence           string            `json:"auth_evidence"`
	LocationQuality        string            `json:"location_quality"`
	Latitude               *float64          `json:"latitude"`
	Longitude              *float64          `json:"longitude"`
	CRS                    string            `json:"crs"`
	Supersedes             *string           `json:"supersedes"`
}

// BatchCandidate mirrors station-coordinate-candidate-v1.
type BatchCandidate struct {
	SchemaVersion string   `json:"schema_version"`
	Source        string   `json:"source"`
	SourceKey     string   `json:"source_key"`
	SampleRef     string   `json:"sample_ref"`
	ObservedAt    string   `json:"observed_at"`
	Latitude      *float64 `json:"latitude"`
	Longitude     *float64 `json:"longitude"`
	OriginalCRS   string   `json:"original_crs"`
	AccuracyM     *float64 `json:"accuracy_m"`
	EvidenceRef   string   `json:"evidence_ref"`
	ReviewState   string   `json:"review_state"`
	StalenessNote string   `json:"staleness_note"`
}

// BatchQuarantine mirrors station-quarantine-v1.
type BatchQuarantine struct {
	SchemaVersion string  `json:"schema_version"`
	Source        string  `json:"source"`
	RowLocator    string  `json:"row_locator"`
	Reason        string  `json:"reason"`
	Detail        string  `json:"detail"`
	SourceKey     *string `json:"source_key"`
}

// Loader row states accepted from prepared batches. Assertion streams
// must stay unknown quality without coordinates: Rust must never smuggle
// a reviewed point past the location review gate. Candidate streams must
// stay pending review for the same reason.
var (
	validAuthStates = map[string]bool{
		"unknown": true, "authorized": true, "suspended": true, "revoked": true,
	}
	validEligibility = map[string]bool{
		"ineligible": true, "pending": true, "eligible": true, "withdrawn": true,
	}
	validQuarantineReasons = map[string]bool{
		"invalid_cnpj": true, "unknown_city": true, "ambiguous_municipality": true,
		"invalid_point": true, "suspect_swap": true, "out_of_brazil": true,
		"date_reversal": true, "header_mismatch": true, "record_too_large": true,
		"older_conflicting_evidence": true, "invalid_field": true,
	}
)

// validatedBatch is a fully checked batch ready for staging: no further
// validation happens during the write phase, so a mid-load failure can
// only come from the store, never from malformed input.
type validatedBatch struct {
	manifest   *BatchManifest
	assertions map[string][]Assertion
	candidates map[string][]Assertion
	quarantine map[string]int
	supersedes map[string]map[string]string
}

func decodeStrict(raw []byte, target any, what string) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("registry: invalid %s: %w", what, err)
	}
	return nil
}

func splitJSONL(raw []byte, what string) ([]json.RawMessage, error) {
	trimmed := bytes.TrimRight(raw, "\n")
	if len(trimmed) == 0 {
		return nil, nil
	}
	lines := bytes.Split(trimmed, []byte("\n"))
	out := make([]json.RawMessage, 0, len(lines))
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			return nil, fmt.Errorf("registry: %s line %d is blank", what, i+1)
		}
		out = append(out, append(json.RawMessage(nil), line...))
	}
	return out, nil
}

func validSHA256(raw string) bool {
	decoded, err := hex.DecodeString(raw)
	return err == nil && len(decoded) == 32
}

func parseEffectiveDate(raw, what string) (pgtype.Date, error) {
	if raw == "" {
		return pgtype.Date{}, nil
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return pgtype.Date{}, fmt.Errorf("registry: invalid %s %q", what, raw)
	}
	return pgtype.Date{Time: parsed, Valid: true}, nil
}

// ValidateBatch checks a manifest plus its three streams without touching
// the store: versions, hashes, sizes, row counts, schemas, identifiers
// and per-input accounting. Anything invalid fails before any run exists.
func ValidateBatch(manifestJSON []byte, streams BatchStreams) (*BatchManifest, error) {
	_, manifest, err := validateBatch(manifestJSON, streams)
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func validateBatchManifest(manifestJSON []byte) (BatchManifest, error) {
	var manifest BatchManifest
	if err := decodeStrict(manifestJSON, &manifest, "batch manifest"); err != nil {
		return manifest, fmt.Errorf("%v", err)
	}
	if manifest.FormatVersion != FormatBatch {
		return manifest, fmt.Errorf("registry: unsupported batch format %q", manifest.FormatVersion)
	}
	if _, err := mustUUID(manifest.RunID); err != nil {
		return manifest, fmt.Errorf("registry: malformed batch run id")
	}
	if manifest.ParserVersion == "" || manifest.PolicyVersion == "" {
		return manifest, fmt.Errorf("registry: batch parser/policy version must be recorded")
	}
	if !manifest.Completeness.EOFValidated || !manifest.Completeness.ExpectedManifest {
		return manifest, fmt.Errorf("registry: batch completeness not attested")
	}
	if len(manifest.Inputs) == 0 {
		return manifest, fmt.Errorf("registry: batch carries no inputs")
	}
	seenInputs := map[string]bool{}
	for _, input := range manifest.Inputs {
		if input.Key == "" || input.Edition == "" {
			return manifest, fmt.Errorf("registry: batch input without key/edition")
		}
		if seenInputs[input.Key] {
			return manifest, fmt.Errorf("registry: duplicate batch input %q", input.Key)
		}
		seenInputs[input.Key] = true
		if !validSHA256(input.SHA256) {
			return manifest, fmt.Errorf("registry: input %q carries a malformed sha256", input.Key)
		}
		counts, ok := manifest.Counts[input.Key]
		if !ok {
			return manifest, fmt.Errorf("registry: input %q has no row accounting", input.Key)
		}
		if counts.Accepted+counts.Duplicates+counts.Quarantined != counts.Input {
			return manifest, fmt.Errorf("registry: input %q counts do not reconcile", input.Key)
		}
		if input.Rows != counts.Input {
			return manifest, fmt.Errorf("registry: input %q rows do not match accounting", input.Key)
		}
	}
	return manifest, nil
}

func validateBatch(manifestJSON []byte, streams BatchStreams) (*validatedBatch, *BatchManifest, error) {
	fail := func(format string, args ...any) (*validatedBatch, *BatchManifest, error) {
		return nil, nil, fmt.Errorf("registry: "+format, args...)
	}
	manifest, err := validateBatchManifest(manifestJSON)
	if err != nil {
		return fail("%v", err)
	}
	seenInputs := map[string]bool{}
	for _, input := range manifest.Inputs {
		seenInputs[input.Key] = true
	}
	outputs := map[string]BatchOutput{}
	for _, output := range manifest.Outputs {
		outputs[output.Path] = output
	}
	streamBytes := map[string][]byte{
		PrepAssertionsFile: streams.Assertions,
		PrepCandidatesFile: streams.Candidates,
		PrepQuarantineFile: streams.Quarantine,
	}
	for path, raw := range streamBytes {
		output, ok := outputs[path]
		if !ok {
			return fail("batch manifest omits output %q", path)
		}
		if !validSHA256(output.SHA256) {
			return fail("output %q carries a malformed sha256", path)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != strings.ToLower(output.SHA256) {
			return fail("output %q checksum mismatch", path)
		}
		if uint64(len(raw)) != output.Bytes {
			return fail("output %q size mismatch", path)
		}
		if bytes.Count(raw, []byte("\n")) != output.Rows {
			return fail("output %q row count mismatch", path)
		}
	}

	assertionLines, err := splitJSONL(streams.Assertions, PrepAssertionsFile)
	if err != nil {
		return fail("%v", err)
	}
	candidateLines, err := splitJSONL(streams.Candidates, PrepCandidatesFile)
	if err != nil {
		return fail("%v", err)
	}
	quarantineLines, err := splitJSONL(streams.Quarantine, PrepQuarantineFile)
	if err != nil {
		return fail("%v", err)
	}

	batch := &validatedBatch{
		manifest:   &manifest,
		assertions: map[string][]Assertion{},
		candidates: map[string][]Assertion{},
		quarantine: map[string]int{},
		supersedes: map[string]map[string]string{},
	}
	for _, line := range assertionLines {
		var row BatchAssertion
		if err := decodeStrict(line, &row, "assertion row"); err != nil {
			return fail("%v", err)
		}
		assertion, err := mapAssertionRow(row)
		if err != nil {
			return fail("%v", err)
		}
		if !seenInputs[row.Source] {
			return fail("assertion source %q matches no batch input", row.Source)
		}
		batch.assertions[row.Source] = append(batch.assertions[row.Source], assertion)
		if row.Supersedes != nil && *row.Supersedes != "" {
			if !validSHA256(*row.Supersedes) {
				return fail("assertion carries a malformed supersedes checksum")
			}
			if batch.supersedes[row.Source] == nil {
				batch.supersedes[row.Source] = map[string]string{}
			}
			batch.supersedes[row.Source][assertion.Checksum] = strings.ToLower(*row.Supersedes)
		}
	}
	for _, line := range candidateLines {
		var row BatchCandidate
		if err := decodeStrict(line, &row, "candidate row"); err != nil {
			return fail("%v", err)
		}
		assertion, err := mapCandidateRow(row)
		if err != nil {
			return fail("%v", err)
		}
		if !seenInputs[row.Source] {
			return fail("candidate source %q matches no batch input", row.Source)
		}
		batch.candidates[row.Source] = append(batch.candidates[row.Source], assertion)
	}
	for _, line := range quarantineLines {
		var row BatchQuarantine
		if err := decodeStrict(line, &row, "quarantine row"); err != nil {
			return fail("%v", err)
		}
		if row.SchemaVersion != SchemaQuarantine {
			return fail("quarantine row with schema %q", row.SchemaVersion)
		}
		if row.Source == "" || row.RowLocator == "" {
			return fail("quarantine row without source/locator")
		}
		if !validQuarantineReasons[row.Reason] {
			return fail("quarantine row with reason %q", row.Reason)
		}
		if !seenInputs[row.Source] {
			return fail("quarantine source %q matches no batch input", row.Source)
		}
		batch.quarantine[row.Source]++
	}
	for _, input := range manifest.Inputs {
		rows := len(batch.assertions[input.Key]) + len(batch.candidates[input.Key])
		counts := manifest.Counts[input.Key]
		if rows != counts.Accepted+counts.Duplicates {
			return fail("input %q staged rows do not match accounting", input.Key)
		}
		if batch.quarantine[input.Key] != counts.Quarantined {
			return fail("input %q quarantine rows do not match accounting", input.Key)
		}
	}
	return batch, &manifest, nil
}

func mapAssertionRow(row BatchAssertion) (Assertion, error) {
	invalid := func(format string, args ...any) (Assertion, error) {
		return Assertion{}, fmt.Errorf("registry: "+format, args...)
	}
	if row.SchemaVersion != SchemaAssertion && row.SchemaVersion != SchemaAssertionV2 {
		return invalid("assertion row with schema %q", row.SchemaVersion)
	}
	if row.SchemaVersion == SchemaAssertionV2 {
		if _, err := parseEffectiveDate(row.PublishedAt, "authorization publication date"); err != nil {
			return invalid("%v", err)
		}
		if _, err := parseEffectiveDate(row.BrandLinkedAt, "distributor linkage date"); err != nil {
			return invalid("%v", err)
		}
		if row.EffectiveAt != row.PublishedAt {
			return invalid("v2 effective date must match authorization publication")
		}
	} else if row.BrandLinkedAt != "" {
		return invalid("brand_linked_at requires assertion v2")
	}
	cnpj, err := ValidCNPJText(row.SourceKey)
	if err != nil {
		return invalid("assertion with invalid CNPJ")
	}
	if !validSHA256(row.Checksum) {
		return invalid("assertion carries a malformed checksum")
	}
	if strings.TrimSpace(row.BusinessNameNormalized) == "" {
		return invalid("assertion without business name")
	}
	if row.MunicipalityIBGE == "" || len(row.UF) != 2 {
		return invalid("assertion without municipality identity")
	}
	if !validAuthStates[row.AuthState] {
		return invalid("assertion with auth state %q", row.AuthState)
	}
	if !validEligibility[row.Eligibility] {
		return invalid("assertion with eligibility %q", row.Eligibility)
	}
	if row.LocationQuality != "unknown" {
		return invalid("assertion stream must stay unknown quality, got %q", row.LocationQuality)
	}
	if (row.Latitude != nil) != (row.Longitude != nil) {
		return invalid("assertion carries a half coordinate")
	}
	if row.Latitude != nil {
		return invalid("assertion stream must not carry coordinates")
	}
	effective, err := parseEffectiveDate(row.EffectiveAt, "assertion effective date")
	if err != nil {
		return invalid("%v", err)
	}
	address := make(map[string]string, len(row.AddressNormalized)+3)
	for key, value := range row.AddressNormalized {
		address[key] = value
	}
	address["raw"] = row.AddressRaw
	address["municipio_ibge"] = row.MunicipalityIBGE
	address["uf"] = strings.ToUpper(row.UF)

	return Assertion{
		Source:           SourcePrep,
		SourceKey:        cnpj,
		Checksum:         strings.ToLower(row.Checksum),
		DisplayName:      row.BusinessNameNormalized,
		Address:          address,
		MunicipalityCode: row.MunicipalityIBGE,
		State:            strings.ToUpper(row.UF),
		AuthState:        row.AuthState,
		Eligibility:      row.Eligibility,
		LocationQuality:  row.LocationQuality,
		SourceReference:  row.AuthorizationRef,
		EffectiveDate:    effective,
		HasCoords:        false,
	}, nil
}

func mapCandidateRow(row BatchCandidate) (Assertion, error) {
	invalid := func(format string, args ...any) (Assertion, error) {
		return Assertion{}, fmt.Errorf("registry: "+format, args...)
	}
	if row.SchemaVersion != SchemaCandidate {
		return invalid("candidate row with schema %q", row.SchemaVersion)
	}
	cnpj, err := ValidCNPJText(row.SourceKey)
	if err != nil {
		return invalid("candidate with invalid CNPJ")
	}
	if strings.TrimSpace(row.SampleRef) == "" || strings.TrimSpace(row.ObservedAt) == "" {
		return invalid("candidate without sample reference/date")
	}
	if row.ReviewState != "pending" {
		return invalid("candidate stream must stay pending review, got %q", row.ReviewState)
	}
	if row.AccuracyM != nil {
		return invalid("candidate stream must not claim accuracy")
	}
	if (row.Latitude != nil) != (row.Longitude != nil) {
		return invalid("candidate carries a half coordinate")
	}
	assertion := Assertion{
		Source:          SourcePrep,
		SourceKey:       cnpj,
		DisplayName:     "PMQC:" + strings.TrimSpace(row.SampleRef),
		AuthState:       "unknown",
		Eligibility:     "pending",
		LocationQuality: "unknown",
		SourceReference: row.EvidenceRef,
	}
	point := ""
	if row.Latitude != nil {
		latitude, longitude := *row.Latitude, *row.Longitude
		if latitude < -34 || latitude > 6 || longitude < -74 || longitude > -28 {
			return invalid("candidate point outside national bounds")
		}
		if latitude == 0 && longitude == 0 {
			return invalid("candidate carries a placeholder point")
		}
		assertion.Latitude, assertion.Longitude = latitude, longitude
		assertion.HasCoords = true
		assertion.CRS = row.OriginalCRS
		point = strconv.FormatFloat(latitude, 'f', 6, 64) + "," + strconv.FormatFloat(longitude, 'f', 6, 64)
	}
	// Candidate checksums are derived with the same canonical recipe as
	// station-prep (sample, CNPJ, date and point joined by \x1f): the
	// candidate schema carries identity, not hashes.
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(row.SampleRef), cnpj, strings.TrimSpace(row.ObservedAt), point,
	}, "\x1f")))
	assertion.Checksum = hex.EncodeToString(sum[:])
	effective, err := parseEffectiveDate(row.ObservedAt, "candidate observed date")
	if err != nil {
		return invalid("%v", err)
	}
	assertion.EffectiveDate = effective
	return assertion, nil
}

// LoadBatch validates a prepared batch and stages it through the existing
// Store ownership, one run per manifest input. Invalid batches fail before
// any run exists; store failures finish the input run as failed, never
// complete. Reports key by manifest input key.
func LoadBatch(ctx context.Context, store Store, manifestJSON []byte, streams BatchStreams) (map[string]Report, error) {
	batch, manifest, err := validateBatch(manifestJSON, streams)
	if err != nil {
		return nil, err
	}
	reports := make(map[string]Report, len(manifest.Inputs))
	for _, input := range manifest.Inputs {
		snapshot := "station-prep:" + manifest.RunID + ":" + input.Key
		runID := newUUID()
		if _, created, err := store.CreateRun(ctx, runID, SourcePrep, snapshot, input.SHA256); err != nil {
			return nil, err
		} else if !created {
			existing, err := store.GetRun(ctx, SourcePrep, snapshot)
			if err != nil {
				return nil, err
			}
			// A non-terminal snapshot never reports success: retry
			// loops treat nil error as convergence, so an orphaned
			// running run or a failed run must fail loudly here and
			// stay visible for resolve-before-retry instead.
			if existing.State != "complete" {
				return nil, fmt.Errorf("registry: input %q unfinished %s (run %s): resolve before retry", input.Key, existing.State, existing.RunID)
			}
			reports[input.Key] = existing
			continue
		}
		report := Report{RunID: runID, State: "running"}
		finish := func(state, code string) (map[string]Report, error) {
			report.State = state
			report.ErrorCode = code
			finishErr := store.FinishRun(ctx, runID, state, report.Accepted, report.Duplicates, report.Rejected, code)
			if finishErr != nil {
				return nil, finishErr
			}
			if state != "complete" {
				return nil, fmt.Errorf("registry: input %q finished %s (%s)", input.Key, state, code)
			}
			reports[input.Key] = report
			return reports, nil
		}
		counts := manifest.Counts[input.Key]
		seen := map[string]bool{}
		stage := func(assertion Assertion) error {
			if seen[assertion.Checksum] {
				report.Duplicates++
				return nil
			}
			seen[assertion.Checksum] = true
			accepted, err := store.StageAssertion(ctx, assertion.WithRun(runID))
			if err != nil {
				return err
			}
			if accepted {
				report.Accepted++
			} else {
				report.Duplicates++
			}
			return nil
		}
		for _, assertion := range batch.assertions[input.Key] {
			if err := stage(assertion); err != nil {
				return finish("failed", "stage_error")
			}
		}
		for _, assertion := range batch.candidates[input.Key] {
			if err := stage(assertion); err != nil {
				return finish("failed", "stage_error")
			}
		}
		report.Rejected = int64(batch.quarantine[input.Key])
		if report.Accepted != int64(counts.Accepted) || report.Duplicates != int64(counts.Duplicates) || report.Rejected != int64(counts.Quarantined) {
			return finish("failed", "count_mismatch")
		}
		if err := linkSupersedes(ctx, store, runID, batch.supersedes[input.Key]); err != nil {
			return finish("failed", "supersede_error")
		}
		returnable, err := finish("complete", "")
		if err != nil {
			return nil, err
		}
		reports = returnable
	}
	return reports, nil
}

// linkSupersedes resolves staged succession links (older checksum to newer
// checksum within the batch) to assertion identities. A dangling link
// fails the run: history must stay auditable, never silently dropped.
func linkSupersedes(ctx context.Context, store Store, runID string, links map[string]string) error {
	if len(links) == 0 {
		return nil
	}
	assertions, err := store.ListAssertions(ctx, runID)
	if err != nil {
		return err
	}
	ids := make(map[string]string, len(assertions))
	for _, assertion := range assertions {
		ids[assertion.Checksum] = assertion.ID
	}
	for older, newer := range links {
		olderID, ok := ids[older]
		if !ok {
			return fmt.Errorf("registry: superseded checksum %q not staged", older)
		}
		newerID, ok := ids[newer]
		if !ok {
			return fmt.Errorf("registry: superseding checksum %q not staged", newer)
		}
		if err := store.SetAssertionSuperseded(ctx, olderID, newerID); err != nil {
			return err
		}
	}
	return nil
}
