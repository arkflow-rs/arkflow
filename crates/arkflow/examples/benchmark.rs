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

//! Public ArkFlow benchmark (issue #87).
//!
//! ```text
//! cargo run -p arkflow --release --example benchmark
//! cargo run -p arkflow --release --example benchmark -- --json
//! ```
//!
//! Flags: `--count <rows>` (default 200000), `--runs <n>` (default 3),
//! `--warmup <n>` (default 1), `--json` (machine-readable report).
//! The suite is self-contained: no network, no external services.

use arkflow_plugin::benchmark::{json_report, markdown_report, run_suite};

const USAGE: &str = "usage: benchmark [--count <rows>] [--runs <n>] [--warmup <n>] [--json]";

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut count: usize = 200_000;
    let mut runs: usize = 3;
    let mut warmup: usize = 1;
    let mut json = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--count" => {
                count = next_value(&mut args, "--count")?.parse().map_err(|_| {
                    format!("--count needs a number\n{USAGE}")
                })?
            }
            "--runs" => {
                runs = next_value(&mut args, "--runs")?
                    .parse()
                    .map_err(|_| format!("--runs needs a number\n{USAGE}"))?
            }
            "--warmup" => {
                warmup = next_value(&mut args, "--warmup")?
                    .parse()
                    .map_err(|_| format!("--warmup needs a number\n{USAGE}"))?
            }
            "--json" => json = true,
            other => {
                eprintln!("unknown argument: {other}\n{USAGE}");
                std::process::exit(2);
            }
        }
    }

    let results = run_suite(count, warmup, runs).await?;
    if json {
        println!("{}", json_report(&results)?);
    } else {
        println!("{}", markdown_report(&results));
    }
    Ok(())
}
