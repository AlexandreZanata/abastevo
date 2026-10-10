#!/usr/bin/env bash
# RST-21 collection checker: read-only verification that every bundle
# row exists and the reproduction commands are available. Exits nonzero
# naming anything missing. No writes, no network, no database.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
missing=0
need() {
  if [ ! -e "$ROOT/$1" ]; then
    echo "MISSING: $1"
    missing=1
  fi
}
for phase in RST10 RST11 RST12 RST13 RST14 RST15 RST16 RST17 RST18 RST19 RST20; do
  need "docs/planning/RUST_STATION_INGESTION_${phase}.md"
done
need "docs/planning/RST21_BUNDLE.md"
need "docs/planning/RUST_STATION_BENCHMARK_PLAN.md"
need "contracts/testdata/station-prep/datasets/tiny.csv"
need "contracts/testdata/station-prep/datasets/tiny-oracle.json"
need "contracts/testdata/station-prep/datasets/emit-tiny/manifest.json"
need "contracts/testdata/station-prep/datasets/emit-tiny-delta1/manifest.json"
need "tools/station-prep/src/bin/bench_datasets.rs"
need "tools/station-prep/src/bin/bench_stages.rs"
need "backend/cmd/station-load/main.go"
need "backend/internal/modules/directory/adapters/read/read_soak_bench_test.go"
need "backend/internal/modules/directory/adapters/read/read_capacity_bench_test.go"
need "backend/internal/modules/directory/adapters/registry/construction_bench_test.go"
need "backend/internal/modules/directory/adapters/registry/loadbatch_fault_test.go"
command -v cargo >/dev/null || { echo "MISSING: cargo"; missing=1; }
command -v go >/dev/null || { echo "MISSING: go"; missing=1; }
if [ "$missing" -ne 0 ]; then
  echo "bundle INCOMPLETE"
  exit 1
fi
echo "bundle COMPLETE"
