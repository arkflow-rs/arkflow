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

//! Integration tests for the SQL output (`sql`) against a real MySQL server,
//! following the probe-and-skip discipline of `postgres_io.rs`: every test
//! first connects with a bounded timeout and returns Ok (printing a skip
//! note) when the server is unavailable.
//!
//! The default target is a local Docker container; override with
//! `ARKFLOW_TEST_MYSQL_URI`:
//!
//! - MySQL 8: mysql://root:arkflow@127.0.0.1:3307/mysql_test (container
//!   `arkflow-test-mysql`)
//!
//! Set `ARKFLOW_REQUIRE_MYSQL=1` where the server is expected to be
//! reachable: an unavailable server then fails the test instead of
//! reporting success without running any assertion.

use arkflow_core::output::{Output, OutputConfig};
use arkflow_core::{MessageBatch, MessageBatchRef, Resource};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use sqlx::{Connection, MySqlConnection};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

fn mysql_uri() -> String {
    std::env::var("ARKFLOW_TEST_MYSQL_URI")
        .unwrap_or_else(|_| "mysql://root:arkflow@127.0.0.1:3307/mysql_test".to_string())
}

static INIT: Once = Once::new();
static UNIQUE: AtomicUsize = AtomicUsize::new(0);

fn ensure_init() {
    INIT.call_once(|| {
        arkflow_plugin::output::init().expect("plugin output init");
    });
}

fn resource() -> Resource {
    Resource {
        temporary: HashMap::new(),
        input_names: RefCell::new(vec![]),
    }
}

/// Fresh name per run and per use, so repeated runs and parallel tests never
/// collide. MySQL caps identifiers at 64 characters, hence the short prefix.
fn unique_name(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        UNIQUE.fetch_add(1, Ordering::SeqCst)
    )
}

/// Connection strings carry the password; skip notes must not echo it.
fn redacted_uri(uri: &str) -> String {
    match uri.split_once("://").and_then(|(scheme, rest)| {
        rest.split_once('@')
            .map(|(userinfo, host)| (scheme, userinfo, host))
    }) {
        Some((scheme, userinfo, host)) => {
            let user = userinfo.split(':').next().unwrap_or_default();
            format!("{scheme}://{user}:***@{host}")
        }
        None => "<redacted connection string>".to_string(),
    }
}

/// Connect with a bounded timeout; `None` means "skip this test".
async fn try_connect(uri: &str, case: &str) -> Option<MySqlConnection> {
    match tokio::time::timeout(Duration::from_secs(5), MySqlConnection::connect(uri)).await {
        Ok(Ok(conn)) => Some(conn),
        Ok(Err(e)) => {
            if std::env::var_os("ARKFLOW_REQUIRE_MYSQL").is_some() {
                panic!(
                    "{case}: MySQL is required (ARKFLOW_REQUIRE_MYSQL is set) but {} refused a \
                     connection: {e}",
                    redacted_uri(uri)
                );
            }
            eprintln!(
                "--- SKIPPED {case}: MySQL at {} refused a connection: {e} ---",
                redacted_uri(uri)
            );
            None
        }
        Err(_) => {
            if std::env::var_os("ARKFLOW_REQUIRE_MYSQL").is_some() {
                panic!(
                    "{case}: MySQL is required (ARKFLOW_REQUIRE_MYSQL is set) but {} did not \
                     answer within 5s",
                    redacted_uri(uri)
                );
            }
            eprintln!(
                "--- SKIPPED {case}: MySQL at {} did not answer within 5s ---",
                redacted_uri(uri)
            );
            None
        }
    }
}

async fn exec(admin: &mut MySqlConnection, sql: &str) {
    sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .execute(admin)
        .await
        .unwrap_or_else(|e| panic!("admin statement failed ({sql}): {e}"));
}

async fn count(admin: &mut MySqlConnection, table: &str) -> i64 {
    let (count,): (i64,) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(admin)
            .await
            .unwrap();
    count
}

fn build_output(config: serde_json::Value) -> Arc<dyn Output> {
    OutputConfig {
        output_type: "sql".to_string(),
        name: None,
        codec: None,
        config: Some(config),
    }
    .build(&resource())
    .unwrap_or_else(|e| panic!("build sql output: {e}"))
}

/// Quotes `name` as one MySQL identifier.
fn mysql_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// One row `(key, quoted, backticked, value)` under column names that would
/// end the column list or open a string if they were interpolated raw.
fn hostile_identifier_batch(
    key: &str,
    quoted: &str,
    backticked: &str,
    k: i64,
    value: i64,
) -> MessageBatchRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new(key, DataType::Int64, false),
        Field::new(quoted, DataType::Int64, false),
        Field::new(backticked, DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![k])),
        Arc::new(Int64Array::from(vec![k + 1])),
        Arc::new(Int64Array::from(vec![k + 2])),
        Arc::new(Int64Array::from(vec![value])),
    ];
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, columns).unwrap(),
    ))
}

/// A one-column `v` batch holding a single row.
fn value_batch(value: i64) -> MessageBatchRef {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    Arc::new(MessageBatch::new_arrow(
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap(),
    ))
}

/// Spec "标识符 SHALL 按方言加引号转义" (sql-output) on the MySQL dialect: a
/// table and columns whose names carry quotes, backticks, and semicolons are
/// written as single identifiers, an upsert replay stays idempotent, a
/// database-qualified table resolves one part at a time, a malformed name
/// fails the build, and nothing else in the database moves.
#[tokio::test]
async fn sql_output_mysql_quotes_hostile_identifiers() {
    ensure_init();
    let case = "sql_output_mysql_quotes_hostile_identifiers";
    let uri = mysql_uri();
    let Some(mut admin) = try_connect(&uri, case).await else {
        return;
    };
    let tag = unique_name("q");
    let bystander = format!("{tag}_bystander");
    // The table name carries a complete second statement.
    let table = format!("{tag}; DROP TABLE {bystander}; --");
    let table_sql = mysql_ident(&table);
    let (key, quoted, backticked) = ("k`ey", "a\"b", "a`b");

    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table_sql}")).await;
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {bystander}")).await;
    exec(&mut admin, &format!("CREATE TABLE {bystander} (n BIGINT)")).await;
    exec(&mut admin, &format!("INSERT INTO {bystander} VALUES (7)")).await;
    exec(
        &mut admin,
        &format!(
            "CREATE TABLE {table_sql} ({} BIGINT PRIMARY KEY, {} BIGINT, {} BIGINT, v BIGINT)",
            mysql_ident(key),
            mysql_ident(quoted),
            mysql_ident(backticked)
        ),
    )
    .await;

    let output = build_output(serde_json::json!({
        "output_type": {"type": "mysql", "uri": uri},
        "table_name": table,
        "upsert": true,
        "upsert_keys": [key]
    }));
    output.connect().await.expect("connect sql output");
    output
        .write(hostile_identifier_batch(key, quoted, backticked, 1, 10))
        .await
        .expect("write hostile identifiers");
    output
        .write(hostile_identifier_batch(key, quoted, backticked, 1, 20))
        .await
        .expect("upsert replay");
    output.close().await.expect("close");

    assert_eq!(
        count(&mut admin, &table_sql).await,
        1,
        "the replay upserts instead of duplicating"
    );
    let (stored,): (i64,) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT v FROM {table_sql}")))
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert_eq!(stored, 20);
    let (stored_key, stored_quoted, stored_backticked): (i64, i64, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {}, {}, {} FROM {table_sql}",
            mysql_ident(key),
            mysql_ident(quoted),
            mysql_ident(backticked)
        )))
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!((stored_key, stored_quoted, stored_backticked), (1, 2, 3));
    // The statement smuggled into the table name never ran.
    assert_eq!(count(&mut admin, &bystander).await, 1);

    // A database-qualified name resolves one part at a time; backticks keep
    // the case the configuration wrote.
    let database = format!("{tag}_db");
    exec(&mut admin, &format!("DROP DATABASE IF EXISTS {database}")).await;
    exec(&mut admin, &format!("CREATE DATABASE {database}")).await;
    exec(
        &mut admin,
        &format!("CREATE TABLE {database}.`Events` (v BIGINT)"),
    )
    .await;
    let output = build_output(serde_json::json!({
        "output_type": {"type": "mysql", "uri": uri},
        "table_name": format!("{database}.Events")
    }));
    output.connect().await.expect("connect sql output");
    output.write(value_batch(1)).await.expect("qualified write");
    output.close().await.expect("close");
    assert_eq!(count(&mut admin, &format!("{database}.`Events`")).await, 1);

    // A malformed name is a build error, never a statement.
    for malformed in ["a..b", "`events", "`events`x", "a\0b"] {
        let error = OutputConfig {
            output_type: "sql".to_string(),
            name: None,
            codec: None,
            config: Some(serde_json::json!({
                "output_type": {"type": "mysql", "uri": uri},
                "table_name": malformed
            })),
        }
        .build(&resource())
        .err()
        .unwrap_or_else(|| panic!("{malformed:?} must fail the build"));
        assert!(
            error.to_string().contains("invalid `table_name`"),
            "{malformed:?}: {error}"
        );
    }

    exec(&mut admin, &format!("DROP DATABASE IF EXISTS {database}")).await;
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {table_sql}")).await;
    exec(&mut admin, &format!("DROP TABLE IF EXISTS {bystander}")).await;
}
