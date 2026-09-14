use arkflow_core::state::{RedbStateBackend, StateBackend};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let backend = RedbStateBackend::open(dir.path(), 1)?;
    let total = 1_000u64;
    let started = Instant::now();
    for namespace in ["aggregate", "window", "join"] {
        for index in 0..total {
            let key = index.to_be_bytes();
            backend.put(namespace, &key, &index.to_be_bytes())?;
        }
    }
    for namespace in ["aggregate", "window", "join"] {
        for index in 0..total {
            assert!(backend.get(namespace, &index.to_be_bytes())?.is_some());
        }
    }
    let elapsed = started.elapsed();
    println!(
        "redb aggregate put/get: {total} records in {:?} ({:.0} ops/s)",
        elapsed,
        total as f64 * 2.0 / elapsed.as_secs_f64()
    );
    println!(
        "logical aggregate/window/join state: {:?}",
        backend.metrics()?
    );
    Ok(())
}
