//! Execution-local state mutation journal for stateful and window operators.
//!
//! The [`StateBackend`] trait has no cross-component transaction, so a stateful
//! operator cannot ask the backend to "commit with the output". Instead, the
//! mutations of one processing unit are staged in an execution-local journal:
//!
//! * writes go to the pending overlay (bounded by count and bytes);
//! * reads see committed state plus the overlay, so later batches in the same
//!   chain observe staged-but-unacknowledged mutations;
//! * the backend itself only ever contains applied state, which is exactly
//!   what a checkpoint barrier snapshots (the committed epoch);
//! * [`CommitOnAck`] ties the apply to the final output acknowledgement —
//!   staged mutations reach the backend only after the downstream write is
//!   confirmed, and if the wrapped (source-side) acknowledgement then fails,
//!   the applied prefix is compensated so an at-least-once replay cannot
//!   double-apply it.

use crate::input::Ack;
use crate::state::StateBackend;
use crate::Error;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Bounds on staged (uncommitted) state. Exceeding either bound fails the
/// staging call so the affected chain surfaces the error instead of growing
/// memory without limit.
#[derive(Debug, Clone, Copy)]
pub struct JournalLimits {
    /// Maximum number of simultaneously registered transactions (one per
    /// unacknowledged output / open window group).
    pub max_pending_transactions: usize,
    /// Maximum total staged payload bytes across all transactions.
    pub max_staged_bytes: usize,
}

impl Default for JournalLimits {
    fn default() -> Self {
        Self {
            max_pending_transactions: 4_096,
            max_staged_bytes: 64 * 1024 * 1024,
        }
    }
}

/// One staged mutation, in apply order within its transaction.
#[derive(Debug, Clone)]
enum StagedMutation {
    Put {
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    },
    Delete {
        namespace: String,
        key: Vec<u8>,
    },
    /// Counter delta — order-independent across transactions, so two
    /// transactions touching the same key may apply in any order.
    Increment {
        namespace: String,
        key: Vec<u8>,
        delta: i64,
        ttl_ms: Option<u64>,
    },
}

impl StagedMutation {
    fn bytes(&self) -> usize {
        match self {
            Self::Put { key, value, .. } => key.len() + value.len(),
            Self::Delete { key, .. } | Self::Increment { key, .. } => key.len(),
        }
    }

    fn storage(&self) -> (&str, &[u8]) {
        match self {
            Self::Put { namespace, key, .. }
            | Self::Delete { namespace, key }
            | Self::Increment { namespace, key, .. } => (namespace, key),
        }
    }

    fn apply_to(&self, value: Option<Vec<u8>>) -> Option<Vec<u8>> {
        match self {
            Self::Put { value: staged, .. } => Some(staged.clone()),
            Self::Delete { .. } => None,
            Self::Increment { delta, .. } => {
                let current = value
                    .map(|raw| serde_json::from_slice::<i64>(&raw).ok())
                    .flatten()
                    .unwrap_or_default();
                Some(
                    serde_json::to_vec(&current.saturating_add(*delta))
                        .expect("i64 serialization cannot fail"),
                )
            }
        }
    }
}

/// Handle for one staging group. `Copy` by design: the journal tracks the
/// group's lifecycle, making `apply`/`complete`/`undo`/`rollback` idempotent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateTxn {
    id: u64,
}

/// Lifecycle of a registered transaction.
#[derive(Debug)]
enum TxnState {
    /// Mutations not yet applied to the backend.
    Staged(Vec<StagedMutation>),
    /// Applied to the backend; `previous` holds the pre-apply value of every
    /// mutation (in apply order) so `undo` can compensate.
    Applied {
        mutations: Vec<StagedMutation>,
        previous: Vec<Option<Vec<u8>>>,
    },
}

#[derive(Debug, Default)]
struct JournalInner {
    next_id: u64,
    txns: BTreeMap<u64, TxnState>,
    staged_bytes: usize,
}

impl JournalInner {
    fn release_bytes(&mut self, mutations: &[StagedMutation]) {
        self.staged_bytes = self
            .staged_bytes
            .saturating_sub(mutations.iter().map(StagedMutation::bytes).sum::<usize>());
    }
}

/// The execution-local mutation journal over one state backend.
pub struct StateJournal {
    backend: Arc<dyn StateBackend>,
    inner: Mutex<JournalInner>,
    limits: JournalLimits,
}

impl StateJournal {
    pub fn new(backend: Arc<dyn StateBackend>) -> Self {
        Self::with_limits(backend, JournalLimits::default())
    }

    pub fn with_limits(backend: Arc<dyn StateBackend>, limits: JournalLimits) -> Self {
        Self {
            backend,
            inner: Mutex::new(JournalInner::default()),
            limits,
        }
    }

    /// The committed-only backend. Barrier snapshots read through this handle
    /// so a checkpoint captures the applied epoch, never the pending overlay.
    pub fn backend(&self) -> &Arc<dyn StateBackend> {
        &self.backend
    }

    pub fn pending_transactions(&self) -> usize {
        self.inner.lock().unwrap().txns.len()
    }

    pub fn staged_bytes(&self) -> usize {
        self.inner.lock().unwrap().staged_bytes
    }

    /// Begin a new staging group.
    pub fn begin(&self) -> Result<StateTxn, Error> {
        let mut inner = self.inner.lock().unwrap();
        if inner.txns.len() >= self.limits.max_pending_transactions {
            return Err(Error::Process(format!(
                "state journal exceeds the {}-transaction pending bound",
                self.limits.max_pending_transactions
            )));
        }
        inner.next_id = inner.next_id.saturating_add(1);
        let id = inner.next_id;
        inner.txns.insert(id, TxnState::Staged(Vec::new()));
        Ok(StateTxn { id })
    }

    /// Read through the overlay: the applied backend value with every staged
    /// transaction's mutations applied in begin order.
    pub fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let staged = self.overlay_value(namespace, key)?;
        if let Some(value) = staged {
            return Ok(value);
        }
        self.backend.get(namespace, key)
    }

    /// Overlay value for one key across staged (unapplied) transactions, or
    /// `None` when no staged mutation touches it.
    fn overlay_value(&self, namespace: &str, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, Error> {
        let inner = self.inner.lock().unwrap();
        let mut value = self.backend.get(namespace, key)?;
        let mut touched = false;
        for state in inner.txns.values() {
            let TxnState::Staged(mutations) = state else {
                continue;
            };
            for mutation in mutations {
                let (mutation_namespace, mutation_key) = mutation.storage();
                if mutation_namespace == namespace && mutation_key == key {
                    touched = true;
                    value = mutation.apply_to(value);
                }
            }
        }
        Ok(touched.then_some(value))
    }

    /// Stage a raw put.
    pub fn put(
        &self,
        txn: StateTxn,
        namespace: &str,
        key: &[u8],
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<(), Error> {
        self.stage(
            txn,
            StagedMutation::Put {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value,
                ttl_ms,
            },
        )
    }

    /// Stage a delete.
    pub fn delete(&self, txn: StateTxn, namespace: &str, key: &[u8]) -> Result<(), Error> {
        self.stage(
            txn,
            StagedMutation::Delete {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
            },
        )
    }

    /// Stage a counter delta. Returns the next effective value computed from
    /// the applied backend state plus the staged overlay.
    pub fn update_i64(
        &self,
        txn: StateTxn,
        namespace: &str,
        key: &[u8],
        delta: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64, Error> {
        let current = self
            .get(namespace, key)?
            .map(|value| serde_json::from_slice::<i64>(&value))
            .transpose()
            .map_err(Error::Serialization)?
            .unwrap_or_default();
        let next = current.saturating_add(delta);
        self.stage(
            txn,
            StagedMutation::Increment {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                delta,
                ttl_ms,
            },
        )?;
        Ok(next)
    }

    fn stage(&self, txn: StateTxn, mutation: StagedMutation) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        let byte_len = mutation.bytes();
        if !matches!(inner.txns.get(&txn.id), Some(TxnState::Staged(_))) {
            return Err(Error::Process(
                "state journal transaction is no longer staged".into(),
            ));
        }
        if inner.staged_bytes + byte_len > self.limits.max_staged_bytes {
            return Err(Error::Process(format!(
                "state journal exceeds the {}-byte staging bound",
                self.limits.max_staged_bytes
            )));
        }
        inner.staged_bytes += byte_len;
        if let Some(TxnState::Staged(mutations)) = inner.txns.get_mut(&txn.id) {
            mutations.push(mutation);
        }
        Ok(())
    }

    /// Apply a staged transaction to the backend. The transaction stays
    /// registered (as applied) until [`StateJournal::complete`], so a
    /// wrapped-acknowledgement failure can still [`StateJournal::undo`] it.
    /// A backend failure during apply compensates the applied prefix and
    /// returns the transaction to the staged state so the caller may retry.
    pub fn apply(&self, txn: StateTxn) -> Result<(), Error> {
        let mutations = {
            let inner = self.inner.lock().unwrap();
            match inner.txns.get(&txn.id) {
                Some(TxnState::Staged(mutations)) => mutations.clone(),
                Some(TxnState::Applied { .. }) | None => return Ok(()),
            }
        };
        // Capture the pre-apply value of each mutation so `undo` and the
        // failure compensation can restore exactly what changed.
        let mut previous = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            let (namespace, key) = mutation.storage();
            previous.push(self.backend.get(namespace, key)?);
        }
        let mut applied = 0usize;
        for mutation in &mutations {
            let result = match mutation {
                StagedMutation::Put {
                    namespace,
                    key,
                    value,
                    ttl_ms,
                } => self.backend.put_with_ttl(
                    namespace,
                    key,
                    value,
                    *ttl_ms,
                    crate::state::now_ms(),
                ),
                StagedMutation::Delete { namespace, key } => {
                    self.backend.delete(namespace, key).map(|_| ())
                }
                StagedMutation::Increment {
                    namespace,
                    key,
                    delta,
                    ttl_ms,
                } => self
                    .backend
                    .update_i64_with_ttl(namespace, key, *delta, *ttl_ms)
                    .map(|_| ()),
            };
            match result {
                Ok(_) => applied += 1,
                Err(error) => {
                    // Compensate the applied prefix so the backend does not
                    // keep a partial transaction a retry would double-apply.
                    if let Err(rollback_error) =
                        self.restore_previous(&mutations[..applied], &previous[..applied])
                    {
                        return Err(Error::Process(format!(
                            "state journal apply failed ({error}); rollback also failed ({rollback_error})"
                        )));
                    }
                    return Err(error);
                }
            }
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some(state) = inner.txns.get_mut(&txn.id) {
            if matches!(state, TxnState::Staged(_)) {
                *state = TxnState::Applied {
                    mutations,
                    previous,
                };
            }
        }
        Ok(())
    }

    /// Complete an applied transaction: its wrapped acknowledgement
    /// succeeded, so the applied state becomes final. No-op otherwise.
    pub fn complete(&self, txn: StateTxn) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(state) = inner.txns.remove(&txn.id) {
            match state {
                TxnState::Staged(mutations) | TxnState::Applied { mutations, .. } => {
                    inner.release_bytes(&mutations);
                }
            }
        }
    }

    /// Compensating rollback of an applied transaction: restore the
    /// pre-apply value of every mutation so an at-least-once replay cannot
    /// double-apply it. The transaction is then discarded.
    pub fn undo(&self, txn: StateTxn) -> Result<(), Error> {
        let applied = {
            let inner = self.inner.lock().unwrap();
            match inner.txns.get(&txn.id) {
                Some(TxnState::Applied {
                    mutations,
                    previous,
                }) => (mutations.clone(), previous.clone()),
                _ => return Ok(()),
            }
        };
        let result = self.restore_previous(&applied.0, &applied.1);
        self.complete(txn);
        result
    }

    /// Apply and complete in one step, for callers that gate the commit on a
    /// condition they already know succeeded (no wrapped acknowledgement).
    pub fn commit(&self, txn: StateTxn) -> Result<(), Error> {
        self.apply(txn)?;
        self.complete(txn);
        Ok(())
    }

    /// Discard a staged (unapplied) transaction without applying it.
    pub fn rollback(&self, txn: StateTxn) {
        self.complete(txn);
    }

    /// Restore pre-apply values in reverse order (a transaction may mutate
    /// the same key more than once).
    fn restore_previous(
        &self,
        mutations: &[StagedMutation],
        previous: &[Option<Vec<u8>>],
    ) -> Result<(), Error> {
        for (mutation, value) in mutations.iter().zip(previous).rev() {
            let (namespace, key) = mutation.storage();
            match value {
                Some(value) => self.backend.put(namespace, key, value.as_slice())?,
                None => {
                    self.backend.delete(namespace, key)?;
                }
            }
        }
        Ok(())
    }
}

/// Acknowledgement that applies a journal transaction before delegating, so
/// state finalization is ordered after the downstream output confirms its
/// write (and before the WAL cursor / source commit of the wrapped ack).
///
/// If the wrapped acknowledgement fails after the apply, the transaction is
/// undone: the source position stays uncommitted, the record replays, and the
/// replayed mutation applies exactly once.
pub struct CommitOnAck {
    journal: Arc<StateJournal>,
    txn: StateTxn,
    inner: Arc<dyn Ack>,
}

impl CommitOnAck {
    pub fn new(journal: Arc<StateJournal>, txn: StateTxn, inner: Arc<dyn Ack>) -> Self {
        Self {
            journal,
            txn,
            inner,
        }
    }
}

#[async_trait]
impl Ack for CommitOnAck {
    async fn ack(&self) -> Result<(), Error> {
        // Idempotent at the journal level: a retry after a transient backend
        // failure re-applies only the unapplied transaction.
        self.journal.apply(self.txn)?;
        match self.inner.ack().await {
            Ok(()) => {
                self.journal.complete(self.txn);
                Ok(())
            }
            Err(error) => {
                if let Err(undo_error) = self.journal.undo(self.txn) {
                    return Err(Error::Process(format!(
                        "source acknowledgement failed ({error}); state rollback also failed ({undo_error})"
                    )));
                }
                Err(error)
            }
        }
    }

    fn mark_held(&self) {
        self.inner.mark_held();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::RedbStateBackend;

    fn backend() -> Arc<dyn StateBackend> {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn StateBackend> =
            Arc::new(RedbStateBackend::open(dir.path(), 1).unwrap());
        // Keep the tempdir alive for the test's duration; the OS reclaims it
        // after the test process exits.
        std::mem::forget(dir);
        backend
    }

    #[test]
    fn reads_see_applied_plus_staged_overlay() {
        let journal = StateJournal::new(backend());
        journal.backend().put("ns", b"k", b"1").unwrap();
        let txn = journal.begin().unwrap();
        assert_eq!(journal.update_i64(txn, "ns", b"k", 2, None).unwrap(), 3);
        // Overlay visible to readers, invisible to the backend.
        assert_eq!(journal.get("ns", b"k").unwrap(), Some(b"3".to_vec()));
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"1".to_vec())
        );
        journal.commit(txn).unwrap();
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"3".to_vec())
        );
        assert_eq!(journal.pending_transactions(), 0);
    }

    #[test]
    fn staged_increments_compose_across_pending_transactions_in_any_order() {
        let journal = StateJournal::new(backend());
        let first = journal.begin().unwrap();
        journal.update_i64(first, "ns", b"k", 1, None).unwrap();
        let second = journal.begin().unwrap();
        // The second batch sees the first batch's staged increment.
        assert_eq!(journal.update_i64(second, "ns", b"k", 1, None).unwrap(), 2);
        // Commits fire in acknowledgement order, not begin order.
        journal.commit(second).unwrap();
        journal.commit(first).unwrap();
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"2".to_vec())
        );
    }

    #[test]
    fn rollback_discards_staged_mutations() {
        let journal = StateJournal::new(backend());
        let txn = journal.begin().unwrap();
        journal.update_i64(txn, "ns", b"k", 5, None).unwrap();
        journal.rollback(txn);
        journal.rollback(txn); // idempotent
        assert!(journal.get("ns", b"k").unwrap().is_none());
        assert_eq!(journal.staged_bytes(), 0);
        assert_eq!(journal.pending_transactions(), 0);
    }

    #[test]
    fn commit_is_idempotent() {
        let journal = StateJournal::new(backend());
        let txn = journal.begin().unwrap();
        journal.put(txn, "ns", b"k", b"v".to_vec(), None).unwrap();
        journal.commit(txn).unwrap();
        journal.commit(txn).unwrap(); // second commit is a no-op
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn undo_restores_the_pre_apply_state() {
        let journal = StateJournal::new(backend());
        journal.backend().put("ns", b"pre", b"old").unwrap();
        let txn = journal.begin().unwrap();
        journal
            .put(txn, "ns", b"pre", b"new".to_vec(), None)
            .unwrap();
        journal
            .put(txn, "ns", b"fresh", b"1".to_vec(), None)
            .unwrap();
        journal.update_i64(txn, "ns", b"counter", 7, None).unwrap();
        journal.apply(txn).unwrap();
        assert_eq!(
            journal.backend().get("ns", b"counter").unwrap(),
            Some(b"7".to_vec())
        );
        journal.undo(txn).unwrap();
        assert_eq!(
            journal.backend().get("ns", b"pre").unwrap(),
            Some(b"old".to_vec())
        );
        assert!(journal.backend().get("ns", b"fresh").unwrap().is_none());
        assert!(journal.backend().get("ns", b"counter").unwrap().is_none());
        assert_eq!(journal.pending_transactions(), 0);
    }

    #[test]
    fn staging_bounds_are_enforced() {
        let journal = StateJournal::with_limits(
            backend(),
            JournalLimits {
                max_pending_transactions: 1,
                max_staged_bytes: 4,
            },
        );
        let txn = journal.begin().unwrap();
        assert!(journal.begin().is_err());
        assert!(journal.put(txn, "ns", b"k", vec![0; 8], None).is_err());
    }

    struct FailingBackend {
        inner: Arc<dyn StateBackend>,
        fail_next: Mutex<bool>,
    }

    impl StateBackend for FailingBackend {
        fn format_version(&self) -> u32 {
            self.inner.format_version()
        }
        fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
            self.inner.get(namespace, key)
        }
        fn put_with_ttl(
            &self,
            namespace: &str,
            key: &[u8],
            value: &[u8],
            ttl_ms: Option<u64>,
            now_ms: u64,
        ) -> Result<(), Error> {
            if *self.fail_next.lock().unwrap() {
                return Err(Error::Process("injected put failure".into()));
            }
            self.inner
                .put_with_ttl(namespace, key, value, ttl_ms, now_ms)
        }
        fn update_i64(&self, namespace: &str, key: &[u8], delta: i64) -> Result<i64, Error> {
            self.inner.update_i64(namespace, key, delta)
        }
        fn delete(&self, namespace: &str, key: &[u8]) -> Result<bool, Error> {
            self.inner.delete(namespace, key)
        }
        fn purge_expired(&self, now_ms: u64) -> Result<u64, Error> {
            self.inner.purge_expired(now_ms)
        }
        fn scan(&self, namespace: &str) -> Result<Vec<crate::state::StateEntry>, Error> {
            self.inner.scan(namespace)
        }
        fn snapshot_at(&self, now_ms: u64) -> Result<crate::state::StateSnapshot, Error> {
            self.inner.snapshot_at(now_ms)
        }
        fn restore(&self, snapshot: &crate::state::StateSnapshot) -> Result<(), Error> {
            self.inner.restore(snapshot)
        }
        fn metrics(&self) -> Result<crate::state::StateMetrics, Error> {
            self.inner.metrics()
        }
        fn close(&self) -> Result<(), Error> {
            self.inner.close()
        }
    }

    #[test]
    fn failed_apply_compensates_and_stays_staged() {
        let inner = backend();
        let failing = Arc::new(FailingBackend {
            inner: inner.clone(),
            fail_next: Mutex::new(true),
        });
        let journal = StateJournal::new(failing.clone() as Arc<dyn StateBackend>);
        let txn = journal.begin().unwrap();
        journal.put(txn, "ns", b"a", b"1".to_vec(), None).unwrap();
        journal.put(txn, "ns", b"b", b"2".to_vec(), None).unwrap();
        assert!(journal.commit(txn).is_err());
        // Nothing from the failed transaction reached the backend.
        assert!(inner.get("ns", b"a").unwrap().is_none());
        assert!(inner.get("ns", b"b").unwrap().is_none());
        // The transaction stays staged: a retry after the transient failure
        // applies it exactly once.
        *failing.fail_next.lock().unwrap() = false;
        journal.commit(txn).unwrap();
        assert_eq!(inner.get("ns", b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(inner.get("ns", b"b").unwrap(), Some(b"2".to_vec()));
    }

    struct RecordingAck {
        acked: Mutex<bool>,
        fail: bool,
    }

    #[async_trait]
    impl Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail {
                return Err(Error::Process("downstream ack failed".into()));
            }
            *self.acked.lock().unwrap() = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn commit_on_ack_applies_before_the_wrapped_ack() {
        let journal = Arc::new(StateJournal::new(backend()));
        let inner = Arc::new(RecordingAck {
            acked: Mutex::new(false),
            fail: false,
        });
        let txn = journal.begin().unwrap();
        journal.update_i64(txn, "ns", b"k", 1, None).unwrap();
        let ack: Arc<dyn Ack> = Arc::new(CommitOnAck::new(
            journal.clone(),
            txn,
            inner.clone() as Arc<dyn Ack>,
        ));
        ack.ack().await.unwrap();
        assert!(*inner.acked.lock().unwrap());
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(journal.pending_transactions(), 0);
    }

    #[tokio::test]
    async fn commit_on_ack_wrapped_failure_undoes_the_apply() {
        // Output succeeded but the source-side commit failed: the applied
        // state must be compensated so the at-least-once replay of the same
        // record applies the mutation exactly once.
        let journal = Arc::new(StateJournal::new(backend()));
        let inner = Arc::new(RecordingAck {
            acked: Mutex::new(false),
            fail: true,
        });
        let txn = journal.begin().unwrap();
        journal.update_i64(txn, "ns", b"k", 1, None).unwrap();
        let ack: Arc<dyn Ack> = Arc::new(CommitOnAck::new(
            journal.clone(),
            txn,
            inner.clone() as Arc<dyn Ack>,
        ));
        assert!(ack.ack().await.is_err());
        assert_eq!(journal.pending_transactions(), 0);
        assert!(journal.backend().get("ns", b"k").unwrap().is_none());
        // Replay: the record is re-delivered and applied once.
        let replay = Arc::new(RecordingAck {
            acked: Mutex::new(false),
            fail: false,
        });
        let replay_txn = journal.begin().unwrap();
        journal.update_i64(replay_txn, "ns", b"k", 1, None).unwrap();
        let replay_ack: Arc<dyn Ack> = Arc::new(CommitOnAck::new(
            journal.clone(),
            replay_txn,
            replay.clone() as Arc<dyn Ack>,
        ));
        replay_ack.ack().await.unwrap();
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"1".to_vec())
        );
    }
}
