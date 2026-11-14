/*
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! Runs `init_logging` in its own process: it installs the process-global
//! tracing subscriber, which would otherwise race the lib-test OTel
//! subscriber installed by the executor span tests.

use arkflow_core::cli::{init_logging, init_logging_from};
use arkflow_core::config::{EngineConfig, LoggingConfig, TracingConfig};

fn config_from_yaml(yaml: &str) -> EngineConfig {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, yaml).unwrap();
    EngineConfig::from_file(path.to_str().unwrap()).unwrap()
}

#[test]
fn logging_initializes_from_config_for_each_format_and_writer() {
    // A subscriber can be installed only once per process, so each
    // format/writer/level case runs in its own child process and asserts on
    // what that subscriber actually emitted. (The previous version called
    // init four times in one process: only the first subscriber was ever
    // installed, so three cases asserted nothing.)
    if let Ok(case) = std::env::var("ARKFLOW_TEST_LOGGING_CHILD") {
        let dir = std::env::var("ARKFLOW_TEST_LOGGING_DIR").unwrap();
        let file = std::path::Path::new(&dir)
            .join("logs")
            .join("engine.log")
            .to_str()
            .unwrap()
            .to_string();
        let (level, format, path) = match case.as_str() {
            "plain-file" => ("debug", "plain", file),
            "json-file" => ("trace", "json", file),
            "json-console" => ("warn", "json", String::new()),
            "bogus-level-console" => ("bogus-level-falls-back-to-info", "plain", String::new()),
            other => panic!("unknown logging case {other}"),
        };
        let yaml = format!(
            "logging:\n  level: {level}\n  format: {format}\n  file_path: {path}\nstreams: []\n"
        );
        init_logging(&config_from_yaml(&yaml));
        tracing::debug!("ARKFLOW_CHILD_DEBUG_EVENT");
        tracing::info!("ARKFLOW_CHILD_INFO_EVENT");
        tracing::warn!("ARKFLOW_CHILD_WARN_EVENT");
        return;
    }

    let exe = std::env::current_exe().unwrap();
    let run = |case: &str, dir: &std::path::Path| {
        let output = std::process::Command::new(&exe)
            .args([
                "--exact",
                "logging_initializes_from_config_for_each_format_and_writer",
                "--nocapture",
            ])
            .env("ARKFLOW_TEST_LOGGING_CHILD", case)
            .env("ARKFLOW_TEST_LOGGING_DIR", dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "case {case} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    };

    // File cases: each gets its own directory so both formats are checked.
    let plain_dir = tempfile::tempdir().unwrap();
    let plain_console = run("plain-file", plain_dir.path());
    assert!(
        !plain_console.contains("ARKFLOW_CHILD_INFO_EVENT"),
        "a file-configured writer must not also log to the console: {plain_console}"
    );
    assert!(plain_dir.path().join("logs").exists());
    let plain = std::fs::read_to_string(plain_dir.path().join("logs/engine.log")).unwrap();
    assert!(
        plain.contains("ARKFLOW_CHILD_DEBUG_EVENT")
            && plain.contains("ARKFLOW_CHILD_INFO_EVENT")
            && plain.contains("ARKFLOW_CHILD_WARN_EVENT"),
        "plain file must receive every event admitted by level=debug: {plain}"
    );
    assert!(
        !plain.lines().any(|line| line.trim_start().starts_with('{')),
        "plain format must not emit JSON lines: {plain}"
    );

    let json_dir = tempfile::tempdir().unwrap();
    run("json-file", json_dir.path());
    let json = std::fs::read_to_string(json_dir.path().join("logs/engine.log")).unwrap();
    assert!(
        json.contains("\"ARKFLOW_CHILD_DEBUG_EVENT\"")
            && json.contains("\"ARKFLOW_CHILD_INFO_EVENT\""),
        "json file must receive trace/debug events: {json}"
    );
    assert!(
        json.lines()
            .filter(|line| !line.trim().is_empty())
            .all(|line| line.trim_start().starts_with('{')),
        "json format must emit one JSON object per line: {json}"
    );

    // Console cases: no file writer path.
    let console_dir = tempfile::tempdir().unwrap();
    let json_console = run("json-console", console_dir.path());
    assert!(
        json_console.contains("\"ARKFLOW_CHILD_WARN_EVENT\""),
        "console JSON must show the warn event: {json_console}"
    );
    assert!(
        !json_console.contains("ARKFLOW_CHILD_INFO_EVENT"),
        "level=warn must filter info events: {json_console}"
    );

    let bogus = run("bogus-level-console", console_dir.path());
    assert!(
        bogus.contains("ARKFLOW_CHILD_INFO_EVENT") && bogus.contains("ARKFLOW_CHILD_WARN_EVENT"),
        "an unknown level must fall back to info: {bogus}"
    );
    assert!(
        !bogus.contains("ARKFLOW_CHILD_DEBUG_EVENT"),
        "info fallback must not admit debug events: {bogus}"
    );
}

/// RUST_LOG semantics, re-executed as a child process so the env var and the
/// global subscriber cannot leak into sibling tests: with parseable
/// directives the EnvFilter overrides the static level; with an unparsable
/// value a warning is printed and the static level applies. The parent
/// controls the child's `RUST_LOG` entirely via `Command::env`/`env_remove`,
/// so a developer's own `RUST_LOG` never leaks into a child.
#[test]
fn rust_log_overrides_static_level_and_falls_back_when_invalid() {
    if std::env::var("ARKFLOW_TEST_RUST_LOG_CHILD").is_ok() {
        // Child mode: the parent re-executed this exact test with RUST_LOG
        // already prepared. Emit one event per level and stop.
        init_logging_from(
            &LoggingConfig {
                level: "info".to_string(),
                file_path: None,
                format: arkflow_core::config::LogFormat::PLAIN,
            },
            &TracingConfig::default(),
        );
        tracing::info!("ARKFLOW_CHILD_INFO_EVENT");
        tracing::warn!("ARKFLOW_CHILD_WARN_EVENT");
        return;
    }

    let exe = std::env::current_exe().unwrap();
    // (case, RUST_LOG value for the child, info event expected)
    let cases: [(&str, Option<&str>, bool); 3] = [
        ("override", Some("warn"), false),
        (
            "fallback",
            Some("arkflow_core=definitely_not_a_level"),
            true,
        ),
        ("unset", None, true),
    ];
    for (case, rust_log, expect_info) in cases {
        let mut command = std::process::Command::new(&exe);
        command
            .args([
                "--exact",
                "rust_log_overrides_static_level_and_falls_back_when_invalid",
                "--nocapture",
            ])
            .env("ARKFLOW_TEST_RUST_LOG_CHILD", case);
        match rust_log {
            Some(directives) => {
                command.env("RUST_LOG", directives);
            }
            None => {
                command.env_remove("RUST_LOG");
            }
        }
        let child = command.output().unwrap();
        let stdout = String::from_utf8_lossy(&child.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&child.stderr).into_owned();
        assert_eq!(
            stdout.contains("ARKFLOW_CHILD_INFO_EVENT"),
            expect_info,
            "case {case}: info event visibility mismatch"
        );
        assert!(
            stdout.contains("ARKFLOW_CHILD_WARN_EVENT"),
            "case {case}: warn event must always be visible"
        );
        if case == "fallback" {
            assert!(
                stderr.contains("Ignoring invalid RUST_LOG"),
                "case {case}: fallback warning missing"
            );
        }
    }
}
