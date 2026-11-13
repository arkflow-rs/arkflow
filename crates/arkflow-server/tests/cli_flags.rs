//! Binary-level CLI surface: exit codes and help text for the serve path's
//! argument handling (only `--config <FILE>` is recognized; anything else is
//! a usage error).

use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_arkflow-server"))
}

#[test]
fn help_exits_zero_and_documents_config() {
    let output = binary().arg("--help").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--config"), "help must mention --config");
    assert!(stdout.contains("migrate"), "help must keep subcommands");
}

#[test]
fn unknown_flag_exits_two_instead_of_starting_the_hub() {
    let output = binary().args(["--confg", "hub.yaml"]).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unrecognized argument"));
}

#[test]
fn missing_config_value_exits_two() {
    let output = binary().arg("--config").output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--config requires a file path"));
}

#[test]
fn unreadable_config_file_exits_two() {
    let output = binary()
        .args(["--config", "/nonexistent/path/hub.yaml"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed to read config file"));
}

#[test]
fn stray_positional_argument_exits_two() {
    // Previously any unknown first argument (including a bare word) silently
    // started the Hub; it must now be a usage error.
    let output = binary().arg("serve").output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unrecognized argument"));
}

#[test]
fn unexpected_argument_after_config_value_exits_two() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hub.yaml");
    std::fs::write(&path, "{}\n").unwrap();
    let output = binary()
        .args(["--config", path.to_str().unwrap(), "extra"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unexpected argument"));
}
