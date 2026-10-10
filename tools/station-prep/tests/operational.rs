//! Relocating the same frozen source must not change quarantine provenance.
use std::path::PathBuf;
#[test]
fn operational_paths_are_portable_between_workstation_and_vps() {
    let root = std::env::temp_dir().join(format!("station-prep-portable-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let fixtures =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/testdata/station-prep");
    for site in ["workstation", "vps"] {
        let dir = root.join(site);
        std::fs::create_dir(&dir).unwrap();
        std::fs::copy(
            fixtures.join("registry-13col-sample.csv"),
            dir.join("registry.csv"),
        )
        .unwrap();
        let status = std::process::Command::new(env!("CARGO_BIN_EXE_prepare_registry"))
            .args([
                dir.join("registry.csv"),
                fixtures.join("ibge-mapping-sample.json"),
                dir.join("out"),
            ])
            .args([
                "https://example.test/registry.csv",
                "frozen",
                "11111111-1111-4111-8111-111111111111",
                "2026-10-10T00:00:00Z",
            ])
            .status()
            .unwrap();
        assert!(status.success());
    }
    for name in [
        "assertions.jsonl",
        "candidates.jsonl",
        "quarantine.jsonl",
        "manifest.json",
    ] {
        assert_eq!(
            std::fs::read(root.join("workstation/out").join(name)).unwrap(),
            std::fs::read(root.join("vps/out").join(name)).unwrap(),
            "{name}"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
