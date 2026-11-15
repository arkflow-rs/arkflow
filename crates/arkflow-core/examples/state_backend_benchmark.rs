use arkflow_core::state::{RedbStateBackend, StateBackend};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let backend = RedbStateBackend::open(dir.path(), 1)?;
    let total = 1_000u64;
    let namespaces = ["aggregate", "window", "join"];
    let started = Instant::now();
    for namespace in namespaces {
        for index in 0..total {
            let key = index.to_be_bytes();
            backend.put(namespace, &key, &index.to_be_bytes())?;
        }
    }
    for namespace in namespaces {
        for index in 0..total {
            assert!(backend.get(namespace, &index.to_be_bytes())?.is_some());
        }
    }
    let elapsed = started.elapsed();
    // Three namespaces, each with `total` puts and `total` gets.
    let operations = total * namespaces.len() as u64 * 2;
    println!(
        "redb aggregate put/get: {operations} operations in {:?} ({:.0} ops/s)",
        elapsed,
        operations as f64 / elapsed.as_secs_f64()
    );
    println!(
        "logical aggregate/window/join state: {:?}",
        backend.metrics()?
    );
    Ok(())
}
