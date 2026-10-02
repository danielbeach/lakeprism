use std::fs;
use std::path::PathBuf;
use std::process::Command;

use uuid::Uuid;

fn catalog() -> PathBuf {
    let root = PathBuf::from("target")
        .join("lakeprism-cli-acceptance")
        .join(Uuid::new_v4().to_string());
    fs::create_dir_all(&root).unwrap();
    root
}

fn invoke(catalog: &PathBuf, arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lakeprism"))
        .arg("--catalog")
        .arg(catalog)
        .args(arguments)
        .output()
        .unwrap()
}

#[test]
fn cli_persists_catalog_and_executes_local_sql() {
    let catalog = catalog();
    let init = invoke(&catalog, &["init"]);
    assert!(init.status.success());

    let register = invoke(
        &catalog,
        &["register", "videos", "file:///acceptance/clip.mp4", "video"],
    );
    assert!(register.status.success(), "{register:?}");

    let query = invoke(
        &catalog,
        &[
            "sql",
            "--format",
            "csv",
            "SELECT lakeprism_media_uri(media) AS uri FROM videos",
        ],
    );
    assert!(query.status.success(), "{query:?}");
    assert_eq!(
        String::from_utf8(query.stdout).unwrap(),
        "\"uri\"\n\"file:///acceptance/clip.mp4\"\n"
    );
    assert!(
        String::from_utf8(query.stderr)
            .unwrap()
            .starts_with("query_id=")
    );

    let show = invoke(&catalog, &["ddl", "SHOW TABLES"]);
    assert!(show.status.success());
    assert!(String::from_utf8(show.stdout).unwrap().contains("videos"));

    let explain = invoke(
        &catalog,
        &[
            "explain-media",
            "SELECT lakeprism_media_type(media) FROM videos",
        ],
    );
    assert!(explain.status.success(), "{explain:?}");
    assert!(
        String::from_utf8(explain.stderr)
            .unwrap()
            .contains("EXPLAIN MEDIA")
    );
    fs::remove_dir_all(catalog).unwrap();
}

#[test]
fn cli_rejects_remote_flight_binding_and_credential_media_uri() {
    let catalog = catalog();
    let flight = invoke(&catalog, &["flight", "--addr", "0.0.0.0:5005"]);
    assert!(!flight.status.success());
    assert!(
        String::from_utf8(flight.stderr)
            .unwrap()
            .contains("local-only")
    );

    let media = invoke(
        &catalog,
        &[
            "register",
            "unsafe",
            "https://example.test/file.mp4?token=secret",
            "video",
        ],
    );
    assert!(!media.status.success());
    assert!(
        String::from_utf8(media.stderr)
            .unwrap()
            .contains("must not contain credentials")
    );
    fs::remove_dir_all(catalog).unwrap();
}
