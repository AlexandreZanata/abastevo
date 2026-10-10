//! `station-prep`: offline preparation of official fuel-station records
//! into validated, deterministic batches for the Go-owned staging loader.
//!
//! RST-02 implements the pure local-file parser only: `;`-delimited
//! streaming CSV to typed rows with bounded memory, UTF-8/BOM handling,
//! frozen header matching, full CNPJ text validation (numeric and
//! alphanumeric vectors, leading zeroes preserved) and municipality
//! resolution through a versioned alias table. Every input row is
//! accepted, counted as a within-run duplicate, or quarantined with a
//! typed reason and a stable row locator; the counts always reconcile.
//! Transport failures (oversize, row cap, truncation, bad encoding)
//! return `Err` with no partial output. There is no network, no database
//! and no automatic promotion of coordinate candidates.

pub mod cnpj;
pub mod datasets;
pub mod join;
pub mod municipality;
pub mod output;
pub mod pmqc;
pub mod registry;
pub mod replay;
pub mod types;

pub use cnpj::{normalize_cnpj, InvalidCnpj};
pub use datasets::{
    alias_table_json, delta_edition, generate, ibge_for, representative_profile, request_trace,
    stratum, stress_100k_profile, stress_1m_profile, tiny_profile, CityWeight, DatasetOracle,
    DatasetProfile, DeltaEdition, GeneratedDataset, RequestTrace, StrataSummary, SyntheticLocation,
    TraceDistribution, DATASETS_VERSION, EMPTY_CITIES, UNIVERSE_CITIES,
};
pub use join::{join_candidates, JoinedBatch, JoinedCandidate, MatchState};
pub use municipality::{AliasError, AliasTable, MunicipalityError};
pub use output::{
    emit_run, emit_to_directory, write_outputs, EmitOptions, EmittedRun, Manifest, ManifestCounts,
    SourceMeta, ASSERTIONS_FILE, CANDIDATES_FILE, MANIFEST_FILE, PARSER_VERSION, POLICY_VERSION,
    QUARANTINE_FILE,
};
pub use pmqc::{parse_pmqc, PmqcBatch, PmqcCandidate, PmqcError};
pub use registry::{parse_registry, RegistryBatch, RegistryError, RegistryRow};
pub use replay::{replay_decision, ReplayDecision};
pub use types::{Counts, Limits, QuarantineRow, RunState};
