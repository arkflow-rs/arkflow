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

//! End-to-end coverage for the offline store maintenance subcommands:
//! `backup` produces consistent SQLite-format snapshots (lease-free, valid
//! under concurrent writes), `restore` imports them into empty stores only,
//! and every refusal path is fail-closed with an actionable message.

use arkflow_server::bootstrap;
use arkflow_server::storage::{
    ControlPlaneStore, DesiredMutation, JobRecord, NodeMutation, StorageActor, StorageBackend,
    WebhookSetMutation, WebhookSubscription,
};

/// Collect sink messages behind a shared handle so assertions can read them
/// after the `&mut dyn FnMut` borrow has ended.
fn collector() -> (
    std::rc::Rc<std::cell::RefCell<Vec<String>>>,
    impl FnMut(String),
) {
    let messages: std::rc::Rc<std::cell::RefCell<Vec<String>>> = std::rc::Rc::default();
    let handle = messages.clone();
    (messages, move |message: String| {
        handle.borrow_mut().push(message)
    })
}

fn args(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

async fn seed_store(store: &ControlPlaneStore) {
    StorageBackend::upsert_job(
        store,
        JobRecord {
            job_id: "backup-job".into(),
            version: 1,
            spec_json: "{}".into(),
            desired_state: "running".into(),
            observed_state: "running".into(),
            convergence: "converged".into(),
            generation: 3,
            node_ids: vec!["node-1".into()],
            checkpoint_id: None,
            last_error: None,
            updated_at_ms: 7,
        },
    )
    .await
    .unwrap();
    StorageBackend::upsert_node(
        store,
        NodeMutation {
            node_id: "node-1".into(),
            version: "test".into(),
            state: "online".into(),
            capabilities_json: "[]".into(),
            boot_id: None,
            report_seq: None,
            last_seen_at_ms: 5,
            lease_expires_at_ms: 0,
            maintenance_state: None,
            maintenance_updated_at_ms: None,
        },
    )
    .await
    .unwrap();
    StorageBackend::replace_webhooks(
        store,
        WebhookSetMutation {
            subscriptions: vec![WebhookSubscription {
                id: "hook-1".into(),
                url: "https://example.test/hook".into(),
                event_types: vec!["job.".into()],
                enabled: true,
                secret: Some("hmac".into()),
                created_by: Some("operator".into()),
                created_at_ms: 9,
            }],
            actor: None,
            correlation_id: None,
        },
        9,
    )
    .await
    .unwrap();
    StorageBackend::set_desired(
        store,
        DesiredMutation {
            node_id: "node-1".into(),
            stream_id: "stream-1".into(),
            desired_state: "running".into(),
            config_version_id: None,
            action_id: None,
            expected_generation: None,
            actor: None,
            correlation_id: None,
            idempotency_key: None,
            intent_type: None,
            payload_json: None,
        },
    )
    .await
    .unwrap();
}

fn sqlite_int(connection: &rusqlite::Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

/// A leftover lease row must never survive into a snapshot: the restored
/// Hub rebuilds its lease through the existing CAS path.
#[tokio::test]
async fn backup_strips_the_lease_row_and_restores_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.sqlite");
    let snapshot_path = dir.path().join("snapshot.sqlite");
    let restored_path = dir.path().join("restored.sqlite");

    let store = ControlPlaneStore::open(store_path.to_str().unwrap())
        .await
        .unwrap();
    seed_store(&store).await;
    store
        .with_connection(|connection| {
            connection.execute(
                "INSERT INTO cp_hub_lease (id, holder, epoch, expires_at_ms) \
                 VALUES (1, 'old-hub', 41, 9999999999999)",
                [],
            )?;
            Ok(())
        })
        .unwrap();

    let (messages, mut sink) = collector();
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            store_path.to_str().unwrap(),
            "--to",
            snapshot_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0, "{:?}", messages.borrow());
    assert!(messages
        .borrow()
        .iter()
        .any(|m| m.contains("backup complete")));

    let snapshot = rusqlite::Connection::open(&snapshot_path).unwrap();
    assert_eq!(
        sqlite_int(&snapshot, "SELECT COUNT(*) FROM cp_hub_lease"),
        0,
        "snapshots never carry a lease row"
    );
    assert_eq!(sqlite_int(&snapshot, "SELECT COUNT(*) FROM cp_jobs"), 1);
    drop(snapshot);

    let code = bootstrap::run_restore(
        &args(&[
            "--from",
            snapshot_path.to_str().unwrap(),
            "--to",
            restored_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0, "{:?}", messages.borrow());
    assert!(messages
        .borrow()
        .iter()
        .any(|m| m.contains("restore complete")));

    // The restored store serves the contract, without the stale lease.
    let restored = ControlPlaneStore::open(restored_path.to_str().unwrap())
        .await
        .unwrap();
    let job = StorageBackend::get_job(&restored, "backup-job")
        .await
        .unwrap()
        .expect("restored job");
    assert_eq!(job.generation, 3);
    let hooks = StorageBackend::list_webhooks(&restored).await.unwrap();
    assert_eq!(hooks.len(), 1);
    restored
        .with_connection(|connection| {
            assert_eq!(
                connection.query_row("SELECT COUNT(*) FROM cp_hub_lease", [], |row| row
                    .get::<_, i64>(0))?,
                0
            );
            Ok(())
        })
        .unwrap();

    // Minimal reconcile check: the restored store accepts desired-state
    // work and recovery replays cleanly — a Hub started on it converges.
    StorageBackend::recover_reconciliation(&restored, 100)
        .await
        .unwrap();
    let intent = StorageBackend::set_desired(
        &restored,
        DesiredMutation {
            node_id: "node-1".into(),
            stream_id: "stream-after-restore".into(),
            desired_state: "running".into(),
            config_version_id: None,
            action_id: None,
            expected_generation: None,
            actor: None,
            correlation_id: None,
            idempotency_key: None,
            intent_type: None,
            payload_json: None,
        },
    )
    .await
    .unwrap();
    let fetched = StorageBackend::get_intent(&restored, &intent.intent_id)
        .await
        .unwrap()
        .expect("intent readable after restore");
    assert_eq!(fetched.stream_id, "stream-after-restore");
}

/// A backup taken while writes are in flight reflects one consistent
/// point: the actor serializes the snapshot against every write, so the
/// snapshot's intent/event/outbox counts agree with each other and every
/// generated intent is one of the written ones. A quiesced re-backup then
/// matches the final counts exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_under_concurrent_writes_is_point_in_time_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store.sqlite");
    let store = ControlPlaneStore::open(store_path.to_str().unwrap())
        .await
        .unwrap();
    let actor = StorageActor::start(store.clone(), 128);

    const WRITES: usize = 20;
    const BASE: usize = 5;
    // A deterministic base (5 writes land before the backup exists) plus a
    // racing writer tail: the snapshot must land somewhere in between.
    for index in 0..BASE {
        actor
            .set_desired(DesiredMutation {
                node_id: "node-1".into(),
                stream_id: format!("stream-{index}"),
                desired_state: "running".into(),
                config_version_id: None,
                action_id: None,
                expected_generation: None,
                actor: None,
                correlation_id: None,
                idempotency_key: None,
                intent_type: None,
                payload_json: None,
            })
            .await
            .unwrap();
    }
    let writer = {
        let actor = actor.clone();
        tokio::spawn(async move {
            for index in BASE..WRITES {
                actor
                    .set_desired(DesiredMutation {
                        node_id: "node-1".into(),
                        stream_id: format!("stream-{index}"),
                        desired_state: "running".into(),
                        config_version_id: None,
                        action_id: None,
                        expected_generation: None,
                        actor: None,
                        correlation_id: None,
                        idempotency_key: None,
                        intent_type: None,
                        payload_json: None,
                    })
                    .await
                    .unwrap();
            }
        })
    };
    let snapshot_path = dir.path().join("live.sqlite");
    let live = {
        let actor = actor.clone();
        let snapshot_path = snapshot_path.clone();
        tokio::spawn(async move { actor.backup_sqlite(snapshot_path.to_str().unwrap()).await })
    };
    writer.await.unwrap();
    let live_report = live.await.unwrap().unwrap();

    // Cross-table consistency at the snapshot point: each set_desired
    // writes exactly one intent, one event, and one outbox row, so a
    // consistent snapshot shows equal counts.
    let snapshot = rusqlite::Connection::open(&snapshot_path).unwrap();
    let intents = sqlite_int(&snapshot, "SELECT COUNT(*) FROM cp_intents");
    let events = sqlite_int(&snapshot, "SELECT COUNT(*) FROM cp_events");
    let outbox = sqlite_int(&snapshot, "SELECT COUNT(*) FROM cp_outbox");
    assert!(
        intents >= BASE as i64 && intents <= WRITES as i64,
        "intents {intents} must land between the base writes and all writes"
    );
    assert_eq!(intents, events, "snapshot must be cross-table consistent");
    assert_eq!(intents, outbox, "snapshot must be cross-table consistent");
    assert_eq!(
        sqlite_int(&snapshot, "SELECT COUNT(*) FROM cp_hub_lease"),
        0
    );
    drop(snapshot);
    let _ = live_report;

    // Once writes stop, the counts match exactly.
    let final_path = dir.path().join("final.sqlite");
    let final_report = actor
        .backup_sqlite(final_path.to_str().unwrap())
        .await
        .unwrap();
    let find = |report: &arkflow_server::storage::table_copier::CopyReport, table: &str| {
        report
            .rows_per_table
            .iter()
            .find(|(name, _)| name == table)
            .map(|(_, count)| *count)
            .unwrap()
    };
    assert_eq!(find(&final_report, "cp_intents"), WRITES);
    assert_eq!(find(&final_report, "cp_events"), WRITES);
    assert_eq!(find(&final_report, "cp_outbox"), WRITES);
    // The lease table is not part of the copy order; check the file directly.
    let final_conn = rusqlite::Connection::open(&final_path).unwrap();
    assert_eq!(
        sqlite_int(&final_conn, "SELECT COUNT(*) FROM cp_hub_lease"),
        0
    );
}

/// Every malformed or precondition-failing invocation exits non-zero with
/// an actionable message and leaves the filesystem untouched.
#[tokio::test]
async fn backup_and_restore_refusals_are_actionable_and_fail_closed() {
    let dir = tempfile::tempdir().unwrap();

    // Missing flags on both subcommands.
    let mut sink = |_: String| {};
    assert_eq!(
        bootstrap::run_backup(&args(&[]), &mut sink).await.unwrap(),
        2
    );
    assert_eq!(
        bootstrap::run_restore(&args(&[]), &mut sink).await.unwrap(),
        2
    );

    // A postgres URL is not a valid snapshot file.
    let code = bootstrap::run_restore(
        &args(&["--from", "postgres://x/y", "--to", "whatever"]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 2);

    // A missing source store fails without creating the snapshot.
    let missing_store = dir.path().join("absent.sqlite");
    let snapshot_path = dir.path().join("never.sqlite");
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            missing_store.to_str().unwrap(),
            "--to",
            snapshot_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 2);
    assert!(!snapshot_path.exists(), "no snapshot may be created");

    // An existing snapshot file is never overwritten.
    let existing = dir.path().join("existing.sqlite");
    std::fs::write(&existing, b"sentinel").unwrap();
    let seeded_path = dir.path().join("seeded.sqlite");
    let store = ControlPlaneStore::open(seeded_path.to_str().unwrap())
        .await
        .unwrap();
    seed_store(&store).await;
    drop(store);
    let (messages, mut sink) = collector();
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            seeded_path.to_str().unwrap(),
            "--to",
            existing.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 2);
    assert!(
        messages
            .borrow()
            .iter()
            .any(|m| m.contains("already exists") && m.contains("refusing")),
        "{:?}",
        messages.borrow()
    );
    assert_eq!(std::fs::read(&existing).unwrap(), b"sentinel");

    // Restoring into a non-control-plane SQLite file is refused outright.
    let unrelated = dir.path().join("unrelated.sqlite");
    let connection = rusqlite::Connection::open(&unrelated).unwrap();
    connection
        .execute(
            "CREATE TABLE user_data (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .unwrap();
    drop(connection);
    let snapshot_path = dir.path().join("snap2.sqlite");
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            seeded_path.to_str().unwrap(),
            "--to",
            snapshot_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0);
    let (messages, mut sink) = collector();
    let code = bootstrap::run_restore(
        &args(&[
            "--from",
            snapshot_path.to_str().unwrap(),
            "--to",
            unrelated.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 2);
    assert!(
        messages
            .borrow()
            .iter()
            .any(|m| m.contains("not a control-plane database")),
        "{:?}",
        messages.borrow()
    );

    // Restoring FROM a non-control-plane "snapshot" is refused too: an
    // unrelated SQLite file would restore nothing while looking successful.
    let (messages, mut sink) = collector();
    let code = bootstrap::run_restore(
        &args(&[
            "--from",
            unrelated.to_str().unwrap(),
            "--to",
            dir.path().join("unused-target.sqlite").to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 2);
    assert!(
        messages
            .borrow()
            .iter()
            .any(|m| m.contains("not a control-plane database")),
        "{:?}",
        messages.borrow()
    );

    // Backing up an unrelated file as a source is refused as well.
    let (messages, mut sink) = collector();
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            unrelated.to_str().unwrap(),
            "--to",
            dir.path().join("unused-snap.sqlite").to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 2);
    assert!(
        messages
            .borrow()
            .iter()
            .any(|m| m.contains("not a control-plane database")),
        "{:?}",
        messages.borrow()
    );
}

/// A restore into a store that already holds rows names every offending
/// table and writes nothing; a snapshot newer than the binary is refused
/// before any target is touched.
#[tokio::test]
async fn restore_rejects_nonempty_targets_and_future_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let seeded_path = dir.path().join("seeded.sqlite");
    let store = ControlPlaneStore::open(seeded_path.to_str().unwrap())
        .await
        .unwrap();
    seed_store(&store).await;
    drop(store);

    let snapshot_path = dir.path().join("snap.sqlite");
    let (_, mut sink) = collector();
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            seeded_path.to_str().unwrap(),
            "--to",
            snapshot_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0);

    // Non-empty target: the message lists EVERY non-empty business table.
    let (_, mut sink) = collector();
    let result = bootstrap::run_restore(
        &args(&[
            "--from",
            snapshot_path.to_str().unwrap(),
            "--to",
            seeded_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await;
    let error = result.unwrap_err().to_string();
    assert!(error.contains("cp_jobs"), "{error}");
    assert!(error.contains("cp_webhooks"), "{error}");
    assert!(error.contains("cp_intents"), "{error}");

    // Future snapshot: refused at open, before any write.
    let future = dir.path().join("future.sqlite");
    std::fs::copy(&snapshot_path, &future).unwrap();
    {
        let store = ControlPlaneStore::open(future.to_str().unwrap())
            .await
            .unwrap();
        store
            .with_connection(|connection| {
                connection.pragma_update(
                    None,
                    "user_version",
                    arkflow_server::storage::SCHEMA_VERSION + 1,
                )?;
                Ok(())
            })
            .unwrap();
    }
    let target_path = dir.path().join("target.sqlite");
    let result = bootstrap::run_restore(
        &args(&[
            "--from",
            future.to_str().unwrap(),
            "--to",
            target_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await;
    let error = result.unwrap_err().to_string();
    assert!(error.contains("newer"), "{error}");
    assert!(
        !target_path.exists(),
        "the version check must fire before the target is touched"
    );
}

/// Gated: the full bootstrap-level round trip against a live PostgreSQL —
/// backup from a PostgreSQL store, restore into a fresh empty database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_restore_round_trip_against_live_postgres_when_available() {
    let Ok(base) = std::env::var("ARKFLOW_TEST_POSTGRES_URL") else {
        eprintln!("skipping: ARKFLOW_TEST_POSTGRES_URL not set");
        return;
    };
    // Replace (not append to) the base URL's path: the base may point at a
    // specific database while the tests need dedicated ones.
    let (scheme, rest) = base.split_once("://").expect("url with scheme");
    let authority = rest.split('/').next().expect("authority");
    let url_for = |db: &str| format!("{scheme}://{authority}/{db}");
    use sqlx::Connection as _;
    let mut admin = sqlx::PgConnection::connect(&url_for("postgres"))
        .await
        .unwrap();
    for name in ["ct_backup_restore_src", "ct_backup_restore_dst"] {
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
        )))
        .execute(&mut admin)
        .await;
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&mut admin)
            .await
            .unwrap();
    }
    drop(admin);
    let source_url = url_for("ct_backup_restore_src");
    let target_url = url_for("ct_backup_restore_dst");

    let store = ControlPlaneStore::open(&source_url).await.unwrap();
    seed_store(&store).await;
    drop(store);

    let dir = tempfile::tempdir().unwrap();
    let snapshot_path = dir.path().join("snapshot.sqlite");
    let (messages, mut sink) = collector();
    let code = bootstrap::run_backup(
        &args(&[
            "--from",
            &source_url,
            "--to",
            snapshot_path.to_str().unwrap(),
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0, "{:?}", messages.borrow());

    let code = bootstrap::run_restore(
        &args(&[
            "--from",
            snapshot_path.to_str().unwrap(),
            "--to",
            &target_url,
        ]),
        &mut sink,
    )
    .await
    .unwrap();
    assert_eq!(code, 0, "{:?}", messages.borrow());
    assert!(messages
        .borrow()
        .iter()
        .any(|m| m.contains("restore complete")));

    let restored = ControlPlaneStore::open(&target_url).await.unwrap();
    let job = StorageBackend::get_job(&restored, "backup-job")
        .await
        .unwrap()
        .expect("restored job");
    assert_eq!(job.generation, 3);
    let hooks = StorageBackend::list_webhooks(&restored).await.unwrap();
    assert_eq!(hooks.len(), 1);
    assert!(hooks[0].enabled, "BOOLEAN round-trips through the snapshot");
}
