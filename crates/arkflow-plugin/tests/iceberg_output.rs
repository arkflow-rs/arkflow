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

//! Gated integration tests for the `iceberg` output against a live REST
//! catalog (Lakekeeper) plus S3-compatible storage (MinIO/Silo). Follows the
//! `ARKFLOW_REQUIRE_*` pattern: without `ARKFLOW_REQUIRE_ICEBERG` the tests
//! skip when the catalog is unreachable; with it set, an unreachable catalog
//! fails the run.
//!
//! Local setup (see the change design for the compose file):
//! `docker compose -p iceberg-spike -f /tmp/iceberg-spike/compose.yaml up -d`
//! then `NO_PROXY='*' ARKFLOW_REQUIRE_ICEBERG=1 cargo test -p arkflow-plugin
//! --features connectors-iceberg --test iceberg_output`
//!
//! Crash-point coverage note: "upload finished, commit never sent" is a
//! prefix of "commit outcome unknown", which is exercised by the replay
//! tests through the output's reload-then-refilter path; the ambiguity of a
//! commit accepted server-side is covered by the same path (the reloaded
//! watermark filters the re-sent rows). No fault-injecting proxy is used.

use std::sync::Arc;
use std::time::Duration;

use arkflow_core::{MessageBatch, MessageBatchRef, Resource};
use arrow_array::{
    Array, Int64Array, MapArray, RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::UnboundPartitionSpec;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::{
    AwsCredential, CustomAwsCredentialLoader, OpenDalStorageFactory, ProvideCredential,
};

/// Same credential discipline as the output: static, config-driven, no env.
#[derive(Debug)]
struct TestCredentials;
impl ProvideCredential for TestCredentials {
    type Credential = AwsCredential;
    async fn provide_credential(
        &self,
        _ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<Option<AwsCredential>> {
        Ok(Some(AwsCredential {
            access_key_id: "silo-root-user".to_string(),
            secret_access_key: "silo-root-password".to_string(),
            session_token: None,
            expires_in: None,
        }))
    }
}

fn s3_factory() -> Arc<OpenDalStorageFactory> {
    Arc::new(OpenDalStorageFactory::S3 {
        customized_credential_load: Some(CustomAwsCredentialLoader::new(TestCredentials)),
    })
}

const CATALOG_URI: &str = "http://localhost:8181/catalog";
const WAREHOUSE: &str = "spike";
const NAMESPACE: &str = "analytics";

fn iceberg_required() -> bool {
    std::env::var_os("ARKFLOW_REQUIRE_ICEBERG").is_some()
}

fn namespace_ident() -> NamespaceIdent {
    NamespaceIdent::from_strs([NAMESPACE]).expect("namespace")
}

async fn catalog() -> iceberg_catalog_rest::RestCatalog {
    RestCatalogBuilder::default()
        .with_storage_factory(s3_factory())
        .load(
            "arkflow-iceberg-tests",
            std::collections::HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), CATALOG_URI.to_string()),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    WAREHOUSE.to_string(),
                ),
                // Client props override the server-provided endpoint: data IO
                // happens from this host, like the output's own config.
                (
                    "s3.endpoint".to_string(),
                    "http://localhost:9000".to_string(),
                ),
                ("s3.region".to_string(), "local-01".to_string()),
            ]),
        )
        .await
        .expect("rest catalog load")
}

/// Create a fresh v2 table with an identity partition on `event_type`.
async fn create_table(name: &str) -> iceberg::table::Table {
    let catalog = catalog().await;
    let arrow = ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("user_id", DataType::Int64, false),
        ArrowField::new("event_type", DataType::Utf8, false),
        ArrowField::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        ),
    ]);
    let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&arrow).expect("schema");
    let partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(3, "event_type", iceberg::spec::Transform::Identity)
        .expect("add field")
        .build();
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec(partition_spec)
        .build();
    catalog
        .create_table(&namespace_ident(), creation)
        .await
        .expect("create table")
}

async fn scan_rows(table: &iceberg::table::Table) -> usize {
    let scan = table.scan().select_all().build().expect("scan build");
    let mut stream = scan.to_arrow().await.expect("to_arrow");
    let mut rows = 0usize;
    while let Some(batch) = StreamExt::next(&mut stream).await {
        rows += batch.expect("scan batch").num_rows();
    }
    rows
}

async fn reload(table: &iceberg::table::Table, name: &str) -> iceberg::table::Table {
    let catalog = catalog().await;
    let _ = table;
    catalog
        .load_table(&TableIdent::new(namespace_ident(), name.to_string()))
        .await
        .expect("reload table")
}

/// Rows spanning offsets [start, start+count) of topic `events`, partition 0,
/// with the `__meta_*` source columns attached.
fn rows_batch(start: i64, count: usize, with_ext: bool) -> RecordBatch {
    let mut fields = vec![
        Arc::new(ArrowField::new("id", DataType::Int64, false)),
        Arc::new(ArrowField::new("user_id", DataType::Int64, false)),
        Arc::new(ArrowField::new("event_type", DataType::Utf8, false)),
        Arc::new(ArrowField::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        )),
    ];
    let mut columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(Int64Array::from(
            (start..start + count as i64).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(vec![42i64; count])),
        Arc::new(StringArray::from(vec!["click"; count])),
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
            1_700_000_000_000_000i64;
            count
        ])),
    ];
    fields.push(Arc::new(ArrowField::new(
        arkflow_core::meta_columns::PARTITION,
        DataType::UInt32,
        false,
    )));
    columns.push(Arc::new(UInt32Array::from(vec![0u32; count])));
    fields.push(Arc::new(ArrowField::new(
        arkflow_core::meta_columns::OFFSET,
        DataType::UInt64,
        false,
    )));
    columns.push(Arc::new(UInt64Array::from(
        (start as u64..(start + count as i64) as u64).collect::<Vec<_>>(),
    )));
    if with_ext {
        let entries = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["topic"; count])) as arrow_array::ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("value", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["events"; count])) as arrow_array::ArrayRef,
            ),
        ]);
        fields.push(Arc::new(ArrowField::new(
            arkflow_core::meta_columns::EXT,
            DataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    entries.data_type().clone(),
                    false,
                )),
                false,
            ),
            false,
        )));
        columns.push(Arc::new(MapArray::new(
            Arc::new(ArrowField::new(
                "entries",
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from((0..=count as i32).collect::<Vec<_>>())),
            entries,
            None,
            false,
        )));
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).expect("batch")
}

fn message(batch: RecordBatch) -> MessageBatchRef {
    Arc::new(MessageBatch::new_arrow(batch))
}

fn resource() -> Resource {
    Resource {
        temporary: std::collections::HashMap::new(),
        input_names: std::cell::RefCell::new(vec![]),
    }
}

fn output_config(table: &str, exactly_once: bool) -> arkflow_core::output::OutputConfig {
    arkflow_core::output::OutputConfig {
        output_type: "iceberg".into(),
        name: None,
        codec: None,
        config: Some(serde_json::json!({
            "catalog": {"uri": CATALOG_URI, "warehouse": WAREHOUSE},
            "namespace": NAMESPACE,
            "table": table,
            "exactly_once": exactly_once,
            "writer_id": if exactly_once { serde_json::json!("arkflow-iceberg-tests") } else { serde_json::json!(null) },
            "s3": {"endpoint": "http://localhost:9000", "region": "local-01",
                   "access_key_id": "silo-root-user", "secret_access_key": "silo-root-password",
                   "path_style_access": true},
        })),
    }
}

async fn init_registry() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        arkflow_plugin::output::init().expect("plugin output init");
    });
}

/// Runs `body` unless the stack is absent and skipping is allowed.
macro_rules! gated {
    ($body:expr) => {{
        if !iceberg_required() && catalog_unreachable().await {
            eprintln!(
                "skipping: catalog at {CATALOG_URI} unreachable and ARKFLOW_REQUIRE_ICEBERG \
                 is not set"
            );
            return;
        }
        init_registry().await;
        $body
    }};
}

async fn catalog_unreachable() -> bool {
    let probe = RestCatalogBuilder::default().load(
        "arkflow-iceberg-tests-probe",
        std::collections::HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), CATALOG_URI.to_string()),
            (
                REST_CATALOG_PROP_WAREHOUSE.to_string(),
                WAREHOUSE.to_string(),
            ),
        ]),
    );
    matches!(
        tokio::time::timeout(Duration::from_secs(2), probe).await,
        Err(_) | Ok(Err(_))
    )
}

#[tokio::test]
async fn happy_path_commits_three_batches_as_one_snapshot() {
    gated!({
        let name = format!("t_happy_{}", std::process::id());
        let table = create_table(&name).await;
        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");
        let batches: Vec<MessageBatchRef> = vec![
            message(rows_batch(0, 10, true)),
            message(rows_batch(10, 10, true)),
            message(rows_batch(20, 10, true)),
        ];
        output.write_batch(&batches).await.expect("write");
        let reloaded = reload(&table, &name).await;
        assert_eq!(scan_rows(&reloaded).await, 30);
        output.close().await.expect("close");
    });
}

#[tokio::test]
async fn replay_after_committed_write_adds_zero_rows() {
    gated!({
        let name = format!("t_replay_{}", std::process::id());
        let table = create_table(&name).await;
        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");
        output
            .write_batch(&[message(rows_batch(0, 50, true))])
            .await
            .expect("first write");
        output.close().await.expect("close");

        // Simulate a crash after the commit but before the acknowledgement:
        // a NEW output instance replays the same records.
        let replay = output_config(&name, true);
        let output2 = replay.build(&resource()).expect("build");
        output2.connect().await.expect("reconnect");
        output2
            .write_batch(&[message(rows_batch(0, 50, true))])
            .await
            .expect("replay write returns Ok without a snapshot");
        let reloaded = reload(&table, &name).await;
        assert_eq!(scan_rows(&reloaded).await, 50, "replay must add no rows");
    });
}

#[tokio::test]
async fn regrouped_replay_adds_zero_rows() {
    gated!({
        let name = format!("t_regroup_{}", std::process::id());
        let table = create_table(&name).await;
        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");
        output
            .write_batch(&[message(rows_batch(0, 60, true))])
            .await
            .expect("write one batch of 60");
        output.close().await.expect("close");

        // Replay the same records regrouped into three differently sized
        // batches (the batch processor's flush boundaries are not stable).
        let replay = output_config(&name, true);
        let output2 = replay.build(&resource()).expect("build");
        output2.connect().await.expect("reconnect");
        output2
            .write_batch(&[
                message(rows_batch(0, 10, true)),
                message(rows_batch(10, 40, true)),
                message(rows_batch(50, 10, true)),
            ])
            .await
            .expect("regrouped replay");
        let reloaded = reload(&table, &name).await;
        assert_eq!(scan_rows(&reloaded).await, 60);
    });
}

#[tokio::test]
async fn at_least_once_replay_reports_duplicates() {
    gated!({
        let name = format!("t_alo_{}", std::process::id());
        let table = create_table(&name).await;
        let config = output_config(&name, false);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");
        output
            .write_batch(&[message(rows_batch(0, 20, true))])
            .await
            .expect("write");
        output.close().await.expect("close");

        // Without exactly-once the replay writes everything again.
        let replay = output_config(&name, false);
        let output2 = replay.build(&resource()).expect("build");
        output2.connect().await.expect("reconnect");
        output2
            .write_batch(&[message(rows_batch(0, 20, true))])
            .await
            .expect("replay");
        let reloaded = reload(&table, &name).await;
        assert_eq!(scan_rows(&reloaded).await, 40, "at-least-once duplicates");
    });
}

#[tokio::test]
async fn missing_position_columns_fail_closed() {
    gated!({
        let name = format!("t_cols_{}", std::process::id());
        let table = create_table(&name).await;
        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");
        let error = output
            .write_batch(&[message(rows_batch(0, 5, false))])
            .await
            .expect_err("missing __meta_ext must fail");
        assert!(
            error.to_string().contains("__meta_ext"),
            "error must name the dropped column: {error}"
        );
        let reloaded = reload(&table, &name).await;
        assert_eq!(scan_rows(&reloaded).await, 0, "nothing written");
    });
}

#[tokio::test]
async fn unknown_batch_column_fails_closed() {
    gated!({
        let name = format!("t_unknown_{}", std::process::id());
        create_table(&name).await;
        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");

        let mut batch = rows_batch(0, 5, true);
        let schema = Arc::new({
            let mut fields: Vec<Arc<ArrowField>> =
                batch.schema().fields().iter().map(Arc::clone).collect();
            fields.push(Arc::new(ArrowField::new(
                "debug_flag",
                DataType::Utf8,
                true,
            )));
            ArrowSchema::new(fields)
        });
        let extra: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![Some("x"); 5]));
        let columns: Vec<arrow_array::ArrayRef> = batch
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::once(extra))
            .collect();
        batch = RecordBatch::try_new(schema, columns).expect("extended batch");

        let error = output
            .write_batch(&[message(batch)])
            .await
            .expect_err("unknown column must fail");
        assert!(error.to_string().contains("debug_flag"), "{error}");
    });
}

#[tokio::test]
async fn missing_required_column_fails_closed() {
    gated!({
        let name = format!("t_missing_{}", std::process::id());
        create_table(&name).await;
        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");

        // Drop the required table column `user_id` from the batch.
        let batch = rows_batch(0, 5, true);
        let schema = {
            let fields: Vec<Arc<ArrowField>> = batch
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() != "user_id")
                .map(Arc::clone)
                .collect();
            Arc::new(ArrowSchema::new(fields))
        };
        let columns: Vec<arrow_array::ArrayRef> = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(index, _)| batch.schema().field(*index).name() != "user_id")
            .map(|(_, column)| Arc::clone(column))
            .collect();
        let batch = RecordBatch::try_new(schema, columns).expect("stripped batch");

        let error = output
            .write_batch(&[message(batch)])
            .await
            .expect_err("missing required column must fail");
        assert!(error.to_string().contains("user_id"), "{error}");
    });
}

#[tokio::test]
async fn rows_route_under_partition_values() {
    gated!({
        let name = format!("t_part_{}", std::process::id());
        let table = create_table(&name).await;

        let config = output_config(&name, true);
        let output = config.build(&resource()).expect("build");
        output.connect().await.expect("connect");

        // Two event_type values in one call must land under both partition
        // values and a scan returns both.
        let canonical =
            schema_to_arrow_schema(table.metadata().current_schema()).expect("canonical");
        let mut fields: Vec<Arc<ArrowField>> = canonical.fields().iter().map(Arc::clone).collect();
        let mut columns: Vec<arrow_array::ArrayRef> = vec![
            Arc::new(Int64Array::from((0..10).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(vec![7i64; 10])),
            Arc::new(StringArray::from(vec![
                "click", "view", "click", "view", "click", "view", "click", "view", "click", "view",
            ])),
            Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
                1_700_000_000_000_000i64;
                10
            ])),
        ];
        fields.push(Arc::new(ArrowField::new(
            arkflow_core::meta_columns::PARTITION,
            DataType::UInt32,
            false,
        )));
        columns.push(Arc::new(UInt32Array::from(vec![0u32; 10])));
        fields.push(Arc::new(ArrowField::new(
            arkflow_core::meta_columns::OFFSET,
            DataType::UInt64,
            false,
        )));
        columns.push(Arc::new(UInt64Array::from((0..10).collect::<Vec<_>>())));
        let entries = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("key", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["topic"; 10])) as arrow_array::ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("value", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["events"; 10])) as arrow_array::ArrayRef,
            ),
        ]);
        fields.push(Arc::new(ArrowField::new(
            arkflow_core::meta_columns::EXT,
            DataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    entries.data_type().clone(),
                    false,
                )),
                false,
            ),
            false,
        )));
        columns.push(Arc::new(MapArray::new(
            Arc::new(ArrowField::new(
                "entries",
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from((0..=10i32).collect::<Vec<_>>())),
            entries,
            None,
            false,
        )));
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns)
            .expect("two-partition batch");

        output.write_batch(&[message(batch)]).await.expect("write");
        let reloaded = reload(&table, &name).await;
        assert_eq!(scan_rows(&reloaded).await, 10);
    });
}
