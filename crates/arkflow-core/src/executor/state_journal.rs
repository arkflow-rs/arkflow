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
use crate::state::{StateBackend, StateEntry};
use crate::Error;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

// A stateful processor and a window can be nested in the same acknowledgement
// chain. Both use this journal, so a non-reentrant Tokio mutex would deadlock
// when the outer window finalization invokes the inner processor's
// `CommitOnAck`. The scope is task-local: top-level acknowledgements still
// serialize the complete apply -> wrapped ack -> complete/undo interval, while
// nested acknowledgements in that same interval reuse the held lock.
tokio::task_local! {
    static ACTIVE_FINALIZATION_JOURNALS: std::cell::RefCell<Vec<usize>>;
}

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
        previous: Vec<Option<StateEntry>>,
        previous_versions: Vec<Option<u64>>,
        applied_versions: Vec<u64>,
    },
}

/// Durable compensation data retained by an acknowledgement after a journal
/// transaction has been completed.  A later sibling acknowledgement may
/// still need to undo that already-completed state change; dropping the
/// pre-apply values at `complete` would make that impossible.
#[derive(Debug, Clone)]
pub struct StateRollback {
    txn: StateTxn,
    mutations: Vec<StagedMutation>,
    previous: Vec<Option<StateEntry>>,
    previous_versions: Vec<Option<u64>>,
    applied_versions: Vec<u64>,
}

#[derive(Debug, Default)]
struct JournalInner {
    next_id: u64,
    next_version: u64,
    txns: BTreeMap<u64, TxnState>,
    staged_bytes: usize,
    key_versions: BTreeMap<(String, Vec<u8>), u64>,
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
    /// Serialize backend mutations and their journal version transitions.
    commit_lock: Mutex<()>,
    /// Serialize acknowledgement finalization for this journal across the
    /// whole `apply -> wrapped ack -> complete/undo` interval. Without a
    /// lock spanning that interval, transaction A could be applied, B could
    /// commit the same key, and A could then be retried after undoing over B.
    /// Journals are scoped to one stateful operator task, so this conservative
    /// per-journal lock preserves correctness without a process-wide mutex.
    finalize_lock: tokio::sync::Mutex<()>,
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
            commit_lock: Mutex::new(()),
            finalize_lock: tokio::sync::Mutex::new(()),
            limits,
        }
    }

    async fn with_finalize_scope<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        let journal_key = self as *const Self as usize;
        let active = ACTIVE_FINALIZATION_JOURNALS
            .try_with(|journals| journals.borrow().contains(&journal_key))
            .unwrap_or(false);
        if active {
            return future.await;
        }

        let mut journals = ACTIVE_FINALIZATION_JOURNALS
            .try_with(|journals| journals.borrow().clone())
            .unwrap_or_default();
        journals.push(journal_key);
        let _finalize_guard = self.finalize_lock.lock().await;
        ACTIVE_FINALIZATION_JOURNALS
            .scope(std::cell::RefCell::new(journals), future)
            .await
    }

    /// The committed-only backend. Barrier snapshots read through this handle
    /// so a checkpoint captures the applied epoch, never the pending overlay.
    pub fn backend(&self) -> &Arc<dyn StateBackend> {
        &self.backend
    }

    pub fn pending_transactions(&self) -> usize {
        self.inner.lock().unwrap().txns.len()
    }

    fn transaction_is_registered(&self, txn: StateTxn) -> bool {
        self.inner.lock().unwrap().txns.contains_key(&txn.id)
    }

    /// Capture the pre-apply values for a transaction that is about to be
    /// completed.  The returned token is bounded by the transaction itself
    /// and lets a composite acknowledgement compensate a successful state
    /// child if a later sibling fails.
    fn capture_applied(&self, txn: StateTxn) -> Option<StateRollback> {
        let inner = self.inner.lock().unwrap();
        match inner.txns.get(&txn.id) {
            Some(TxnState::Applied {
                mutations,
                previous,
                previous_versions,
                applied_versions,
            }) => Some(StateRollback {
                txn,
                mutations: mutations.clone(),
                previous: previous.clone(),
                previous_versions: previous_versions.clone(),
                applied_versions: applied_versions.clone(),
            }),
            _ => None,
        }
    }

    /// Restore a completed transaction's bytes conditionally.  Version
    /// fencing in `restore_previous` prevents an older compensation from
    /// erasing a newer commit on the same key.
    fn undo_snapshot(&self, snapshot: &StateRollback) -> Result<(), Error> {
        let _commit_guard = self.commit_lock.lock().unwrap();
        self.restore_previous(
            &snapshot.mutations,
            &snapshot.previous,
            &snapshot.previous_versions,
            &snapshot.applied_versions,
        )
    }

    /// Re-stage a completed transaction after a compensating undo.  A
    /// composite acknowledgement can finish its state child before a
    /// sibling source acknowledgement fails; if the composite is retried,
    /// the same transaction id must contain the original mutations again or
    /// the retry would acknowledge the source without reapplying state.
    fn restage_snapshot(&self, snapshot: &StateRollback) -> Result<(), Error> {
        let _commit_guard = self.commit_lock.lock().unwrap();
        let mut inner = self.inner.lock().unwrap();
        if inner.txns.contains_key(&snapshot.txn.id) {
            return Ok(());
        }
        let bytes = snapshot
            .mutations
            .iter()
            .map(StagedMutation::bytes)
            .sum::<usize>();
        if inner.txns.len() >= self.limits.max_pending_transactions {
            return Err(Error::Process(format!(
                "state journal exceeds the {}-transaction pending bound",
                self.limits.max_pending_transactions
            )));
        }
        if inner.staged_bytes.saturating_add(bytes) > self.limits.max_staged_bytes {
            return Err(Error::Process(format!(
                "state journal exceeds the {}-byte staging bound",
                self.limits.max_staged_bytes
            )));
        }
        inner.staged_bytes = inner.staged_bytes.saturating_add(bytes);
        inner.txns.insert(
            snapshot.txn.id,
            TxnState::Staged(snapshot.mutations.clone()),
        );
        Ok(())
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

    /// Replace the snapshot for one key inside a staged transaction.
    ///
    /// Window buffers are serialized as complete values. Appending another
    /// `Put` for the same open window on every input batch would make the
    /// journal's byte bound grow with traffic even though only the latest
    /// snapshot is live. This operation removes older mutations for the key
    /// before staging the replacement while preserving the transaction's
    /// overlay semantics.
    pub fn put_compact(
        &self,
        txn: StateTxn,
        namespace: &str,
        key: &[u8],
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<(), Error> {
        let mutation = StagedMutation::Put {
            namespace: namespace.to_owned(),
            key: key.to_vec(),
            value,
            ttl_ms,
        };
        let _commit_guard = self.commit_lock.lock().unwrap();
        let mut inner = self.inner.lock().unwrap();
        let removed_bytes = {
            let Some(TxnState::Staged(mutations)) = inner.txns.get(&txn.id) else {
                return Err(Error::Process(
                    "state journal transaction is no longer staged".into(),
                ));
            };
            mutations
                .iter()
                .filter(|existing| existing.storage() == mutation.storage())
                .map(StagedMutation::bytes)
                .sum::<usize>()
        };
        let next_bytes = inner
            .staged_bytes
            .saturating_sub(removed_bytes)
            .saturating_add(mutation.bytes());
        if next_bytes > self.limits.max_staged_bytes {
            return Err(Error::Process(format!(
                "state journal exceeds the {}-byte staging bound",
                self.limits.max_staged_bytes
            )));
        }
        inner.staged_bytes = next_bytes;
        if let Some(TxnState::Staged(mutations)) = inner.txns.get_mut(&txn.id) {
            mutations.retain(|existing| existing.storage() != mutation.storage());
            mutations.push(mutation);
        }
        Ok(())
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
        let _commit_guard = self.commit_lock.lock().unwrap();
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
        let _commit_guard = self.commit_lock.lock().unwrap();
        let (mutations, previous_versions) = {
            let inner = self.inner.lock().unwrap();
            match inner.txns.get(&txn.id) {
                Some(TxnState::Staged(mutations)) => (
                    mutations.clone(),
                    mutations
                        .iter()
                        .map(|mutation| {
                            let (namespace, key) = mutation.storage();
                            inner
                                .key_versions
                                .get(&(namespace.to_owned(), key.to_vec()))
                                .copied()
                        })
                        .collect::<Vec<_>>(),
                ),
                Some(TxnState::Applied { .. }) | None => return Ok(()),
            }
        };
        // Capture the pre-apply value of each mutation so `undo` and the
        // failure compensation can restore exactly what changed.
        let mut previous = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            let (namespace, key) = mutation.storage();
            previous.push(self.backend.get_entry(namespace, key)?);
        }
        let mut applied = 0usize;
        let mut applied_versions = Vec::with_capacity(mutations.len());
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
                Ok(_) => {
                    let version = {
                        let mut inner = self.inner.lock().unwrap();
                        inner.next_version = inner.next_version.saturating_add(1);
                        let version = inner.next_version;
                        let (namespace, key) = mutation.storage();
                        inner
                            .key_versions
                            .insert((namespace.to_owned(), key.to_vec()), version);
                        version
                    };
                    applied_versions.push(version);
                    applied += 1;
                }
                Err(error) => {
                    // Compensate the applied prefix so the backend does not
                    // keep a partial transaction a retry would double-apply.
                    if let Err(rollback_error) = self.restore_previous(
                        &mutations[..applied],
                        &previous[..applied],
                        &previous_versions[..applied],
                        &applied_versions,
                    ) {
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
                    previous_versions,
                    applied_versions,
                };
            }
        }
        Ok(())
    }

    /// Complete an applied transaction: its wrapped acknowledgement
    /// succeeded, so the applied state becomes final. No-op otherwise.
    pub fn complete(&self, txn: StateTxn) {
        let _commit_guard = self.commit_lock.lock().unwrap();
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
    /// double-apply it. A successful undo returns the transaction to
    /// `Staged`, because a transient wrapped-ack failure retries this same
    /// acknowledgement and must apply the mutation again.
    pub fn undo(&self, txn: StateTxn) -> Result<(), Error> {
        let _commit_guard = self.commit_lock.lock().unwrap();
        let applied = {
            let inner = self.inner.lock().unwrap();
            match inner.txns.get(&txn.id) {
                Some(TxnState::Applied {
                    mutations,
                    previous,
                    previous_versions,
                    applied_versions,
                }) => (
                    mutations.clone(),
                    previous.clone(),
                    previous_versions.clone(),
                    applied_versions.clone(),
                ),
                _ => return Ok(()),
            }
        };
        self.restore_previous(&applied.0, &applied.1, &applied.2, &applied.3)?;
        let mut inner = self.inner.lock().unwrap();
        if let Some(TxnState::Applied { mutations, .. }) = inner.txns.remove(&txn.id) {
            inner.txns.insert(txn.id, TxnState::Staged(mutations));
        }
        Ok(())
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
        previous: &[Option<StateEntry>],
        previous_versions: &[Option<u64>],
        applied_versions: &[u64],
    ) -> Result<(), Error> {
        for (((mutation, value), previous_version), applied_version) in mutations
            .iter()
            .zip(previous)
            .zip(previous_versions)
            .zip(applied_versions)
            .rev()
        {
            let (namespace, key) = mutation.storage();
            let version_key = (namespace.to_owned(), key.to_vec());
            let owns_current_value = self
                .inner
                .lock()
                .unwrap()
                .key_versions
                .get(&version_key)
                .copied()
                == Some(*applied_version);
            if !owns_current_value {
                // A later committed transaction owns this key. Restoring the
                // older bytes would erase that valid commit.
                continue;
            }
            self.backend.restore_entry(namespace, key, value.as_ref())?;
            let mut inner = self.inner.lock().unwrap();
            if inner.key_versions.get(&version_key).copied() == Some(*applied_version) {
                match previous_version {
                    Some(version) => {
                        inner.key_versions.insert(version_key, *version);
                    }
                    None => {
                        inner.key_versions.remove(&version_key);
                    }
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
    completed_rollback: Mutex<Option<StateRollback>>,
}

/// Acknowledgement that atomically finalizes several journal transactions with
/// one wrapped acknowledgement. Window firing can touch multiple groups in a
/// single output; keeping the source acknowledgement inside this composite is
/// what lets a source failure undo every group instead of leaving finalized
/// window state behind.
pub struct CommitGroupOnAck {
    journal: Arc<StateJournal>,
    txns: Vec<StateTxn>,
    inner: Arc<dyn Ack>,
    completed_rollbacks: Mutex<Option<Vec<StateRollback>>>,
}

impl CommitGroupOnAck {
    pub fn new(journal: Arc<StateJournal>, txns: Vec<StateTxn>, inner: Arc<dyn Ack>) -> Self {
        Self {
            journal,
            txns,
            inner,
            completed_rollbacks: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Ack for CommitGroupOnAck {
    async fn ack(&self) -> Result<(), Error> {
        self.journal
            .with_finalize_scope(async {
                let mut applied = Vec::with_capacity(self.txns.len());
                for txn in &self.txns {
                    if let Err(error) = self.journal.apply(*txn) {
                        for applied_txn in applied.iter().rev() {
                            let _ = self.journal.undo(*applied_txn);
                        }
                        return Err(error);
                    }
                    applied.push(*txn);
                }
                match self.inner.ack().await {
                    Ok(()) => {
                        let rollbacks = self
                            .txns
                            .iter()
                            .filter_map(|txn| self.journal.capture_applied(*txn))
                            .collect::<Vec<_>>();
                        for txn in &self.txns {
                            self.journal.complete(*txn);
                        }
                        let should_store = !rollbacks.is_empty()
                            || self.completed_rollbacks.lock().unwrap().is_none();
                        if should_store {
                            *self.completed_rollbacks.lock().unwrap() = Some(rollbacks);
                        }
                        Ok(())
                    }
                    Err(error) => {
                        let mut rollback_error = None;
                        for txn in applied.iter().rev() {
                            if let Err(undo_error) = self.journal.undo(*txn) {
                                rollback_error.get_or_insert(undo_error);
                            }
                        }
                        match rollback_error {
                            Some(undo_error) => Err(Error::Process(format!(
                                "source acknowledgement failed ({error}); state rollback also failed ({undo_error})"
                            ))),
                            None => Err(error),
                        }
                    }
                }
            })
            .await
    }

    fn mark_held(&self) {
        self.inner.mark_held();
    }

    fn release_held(&self) {
        self.inner.release_held();
    }

    async fn undo(&self) -> Result<(), Error> {
        self.journal
            .with_finalize_scope(async {
                // Source compensation and state compensation are independent
                // recovery steps.  Even if the source cannot currently undo,
                // still restore the state snapshot; otherwise a retry can
                // observe a durable state mutation with an uncommitted source
                // position and apply it twice.
                let mut first_error = self.inner.undo().await.err();
                for txn in &self.txns {
                    if self.journal.transaction_is_registered(*txn) {
                        if let Err(error) = self.journal.undo(*txn) {
                            first_error.get_or_insert(error);
                        }
                    }
                }
                let completed_rollbacks = { self.completed_rollbacks.lock().unwrap().take() };
                if let Some(rollbacks) = completed_rollbacks {
                    for rollback in rollbacks.iter().rev() {
                        if let Err(error) = self.journal.undo_snapshot(rollback) {
                            first_error.get_or_insert(error);
                        }
                    }
                    for rollback in &rollbacks {
                        if let Err(error) = self.journal.restage_snapshot(rollback) {
                            first_error.get_or_insert(error);
                        }
                    }
                    *self.completed_rollbacks.lock().unwrap() = Some(rollbacks);
                }
                first_error.map_or(Ok(()), Err)
            })
            .await
    }

    async fn abort(&self) -> Result<(), Error> {
        // The output may fail before this acknowledgement is ever called.
        // `fired_ack` has already removed the transactions from the window's
        // lookup map, so explicitly discard their staged mutations here;
        // otherwise a one-target send failure leaks a transaction forever and
        // can eventually exhaust the journal bound.
        self.journal
            .with_finalize_scope(async {
                let inner_result = self.inner.abort().await;
                for txn in &self.txns {
                    if self.journal.transaction_is_registered(*txn) {
                        let _ = self.journal.undo(*txn);
                        self.journal.rollback(*txn);
                    }
                }
                if let Some(rollbacks) = self.completed_rollbacks.lock().unwrap().take() {
                    for rollback in rollbacks.iter().rev() {
                        let _ = self.journal.undo_snapshot(rollback);
                    }
                }
                inner_result
            })
            .await
    }
}

impl CommitOnAck {
    pub fn new(journal: Arc<StateJournal>, txn: StateTxn, inner: Arc<dyn Ack>) -> Self {
        Self {
            journal,
            txn,
            inner,
            completed_rollback: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Ack for CommitOnAck {
    async fn ack(&self) -> Result<(), Error> {
        self.journal
            .with_finalize_scope(async {
                // Idempotent at the journal level: a retry after a transient
                // backend failure re-applies only the unapplied transaction.
                self.journal.apply(self.txn)?;
                match self.inner.ack().await {
                    Ok(()) => {
                        if let Some(rollback) = self.journal.capture_applied(self.txn) {
                            *self.completed_rollback.lock().unwrap() = Some(rollback);
                        }
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
            })
            .await
    }

    fn mark_held(&self) {
        self.inner.mark_held();
    }

    fn release_held(&self) {
        self.inner.release_held();
    }

    async fn undo(&self) -> Result<(), Error> {
        self.journal
            .with_finalize_scope(async {
                // Do not let a source-side compensation error bypass the
                // journal rollback.  The source remains retryable, while the
                // in-memory/backend state must not retain an unacknowledged
                // mutation.
                let mut first_error = self.inner.undo().await.err();
                if self.journal.transaction_is_registered(self.txn) {
                    if let Err(error) = self.journal.undo(self.txn) {
                        first_error.get_or_insert(error);
                    }
                } else {
                    let completed_rollback = { self.completed_rollback.lock().unwrap().take() };
                    if let Some(rollback) = completed_rollback {
                        if let Err(error) = self.journal.undo_snapshot(&rollback) {
                            first_error.get_or_insert(error);
                        }
                        if let Err(error) = self.journal.restage_snapshot(&rollback) {
                            first_error.get_or_insert(error);
                        }
                        *self.completed_rollback.lock().unwrap() = Some(rollback);
                    }
                }
                first_error.map_or(Ok(()), Err)
            })
            .await
    }

    async fn abort(&self) -> Result<(), Error> {
        self.journal
            .with_finalize_scope(async {
                let inner_result = self.inner.abort().await;
                if self.journal.transaction_is_registered(self.txn) {
                    let _ = self.journal.undo(self.txn);
                    self.journal.rollback(self.txn);
                } else {
                    let completed_rollback = { self.completed_rollback.lock().unwrap().take() };
                    if let Some(rollback) = completed_rollback {
                        let _ = self.journal.undo_snapshot(&rollback);
                    }
                }
                inner_result
            })
            .await
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
        // Undo keeps the transaction staged so the same acknowledgement can
        // retry after a transient wrapped-ack failure.
        assert_eq!(journal.pending_transactions(), 1);
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
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl Ack for RecordingAck {
        async fn ack(&self) -> Result<(), Error> {
            if self.fail.load(std::sync::atomic::Ordering::Acquire) {
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
            fail: std::sync::atomic::AtomicBool::new(false),
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
            fail: std::sync::atomic::AtomicBool::new(true),
        });
        let txn = journal.begin().unwrap();
        journal.update_i64(txn, "ns", b"k", 1, None).unwrap();
        let ack: Arc<dyn Ack> = Arc::new(CommitOnAck::new(
            journal.clone(),
            txn,
            inner.clone() as Arc<dyn Ack>,
        ));
        assert!(ack.ack().await.is_err());
        assert_eq!(journal.pending_transactions(), 1);
        assert!(journal.backend().get("ns", b"k").unwrap().is_none());
        // The fan-out/ack retry reuses the same CommitOnAck and therefore the
        // same staged transaction. It must apply the mutation once, rather
        // than observing a removed transaction and acknowledging the source
        // without restoring state.
        inner
            .fail
            .store(false, std::sync::atomic::Ordering::Release);
        ack.ack().await.unwrap();
        assert_eq!(journal.pending_transactions(), 0);
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"1".to_vec())
        );
    }

    #[tokio::test]
    async fn commit_group_on_ack_rolls_back_all_transactions_and_retries() {
        let journal = Arc::new(StateJournal::new(backend()));
        let inner = Arc::new(RecordingAck {
            acked: Mutex::new(false),
            fail: std::sync::atomic::AtomicBool::new(true),
        });
        let first = journal.begin().unwrap();
        journal.update_i64(first, "ns", b"first", 1, None).unwrap();
        let second = journal.begin().unwrap();
        journal
            .update_i64(second, "ns", b"second", 2, None)
            .unwrap();
        let ack: Arc<dyn Ack> = Arc::new(CommitGroupOnAck::new(
            journal.clone(),
            vec![first, second],
            inner.clone() as Arc<dyn Ack>,
        ));

        // A source-side failure must undo every applied window transaction,
        // rather than leaving a partially finalized aggregate behind.
        assert!(ack.ack().await.is_err());
        assert!(journal.backend().get("ns", b"first").unwrap().is_none());
        assert!(journal.backend().get("ns", b"second").unwrap().is_none());
        assert_eq!(journal.pending_transactions(), 2);

        // Retrying the same composite re-stages/applies both transactions and
        // completes them exactly once after the wrapped source ack succeeds.
        inner
            .fail
            .store(false, std::sync::atomic::Ordering::Release);
        ack.ack().await.unwrap();
        assert_eq!(
            journal.backend().get("ns", b"first").unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(
            journal.backend().get("ns", b"second").unwrap(),
            Some(b"2".to_vec())
        );
        assert_eq!(journal.pending_transactions(), 0);
    }

    #[tokio::test]
    async fn completed_state_can_be_compensated_and_retried() {
        let journal = Arc::new(StateJournal::new(backend()));
        let inner = Arc::new(RecordingAck {
            acked: Mutex::new(false),
            fail: std::sync::atomic::AtomicBool::new(false),
        });
        let txn = journal.begin().unwrap();
        journal.update_i64(txn, "ns", b"k", 1, None).unwrap();
        let ack: Arc<dyn Ack> = Arc::new(CommitOnAck::new(
            journal.clone(),
            txn,
            inner.clone() as Arc<dyn Ack>,
        ));

        ack.ack().await.unwrap();
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"1".to_vec())
        );

        // A sibling acknowledgement failure may compensate a state child
        // after its transaction has already completed. The next retry must
        // re-stage the mutation, not merely re-ack the source.
        ack.undo().await.unwrap();
        assert!(journal.backend().get("ns", b"k").unwrap().is_none());
        assert_eq!(journal.pending_transactions(), 1);

        ack.ack().await.unwrap();
        assert_eq!(journal.pending_transactions(), 0);
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"1".to_vec())
        );
    }

    #[tokio::test]
    async fn nested_same_journal_ack_does_not_deadlock() {
        let journal = Arc::new(StateJournal::new(backend()));
        let source = Arc::new(RecordingAck {
            acked: Mutex::new(false),
            fail: std::sync::atomic::AtomicBool::new(false),
        });
        let window_txn = journal.begin().unwrap();
        journal
            .update_i64(window_txn, "window", b"k", 1, None)
            .unwrap();
        let processor_txn = journal.begin().unwrap();
        journal
            .update_i64(processor_txn, "processor", b"k", 1, None)
            .unwrap();
        let processor_ack: Arc<dyn Ack> = Arc::new(CommitOnAck::new(
            journal.clone(),
            processor_txn,
            source.clone() as Arc<dyn Ack>,
        ));
        let group: Arc<dyn Ack> = Arc::new(CommitGroupOnAck::new(
            journal.clone(),
            vec![window_txn],
            processor_ack,
        ));

        tokio::time::timeout(std::time::Duration::from_secs(1), group.ack())
            .await
            .expect("nested journal acknowledgements must not deadlock")
            .unwrap();
        assert_eq!(
            journal.backend().get("window", b"k").unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(
            journal.backend().get("processor", b"k").unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(journal.pending_transactions(), 0);
    }

    #[test]
    fn undo_preserves_ttl_metadata() {
        let backend = Arc::new(crate::state::InMemoryStateBackend::new(1).unwrap());
        let backend_dyn: Arc<dyn StateBackend> = backend.clone();
        let now = crate::state::now_ms();
        backend_dyn
            .put_with_ttl("ns", b"k", b"old", Some(60_000), now)
            .unwrap();
        let before = backend_dyn.get_entry("ns", b"k").unwrap().unwrap();

        let journal = StateJournal::new(backend_dyn.clone());
        let txn = journal.begin().unwrap();
        journal.put(txn, "ns", b"k", b"new".to_vec(), None).unwrap();
        journal.apply(txn).unwrap();
        journal.undo(txn).unwrap();

        let after = backend_dyn.get_entry("ns", b"k").unwrap().unwrap();
        assert_eq!(after.value, before.value);
        assert_eq!(after.expires_at_ms, before.expires_at_ms);
    }

    #[test]
    fn older_undo_does_not_restore_over_a_later_commit() {
        let journal = StateJournal::new(backend());
        journal.backend().put("ns", b"k", b"0").unwrap();

        let first = journal.begin().unwrap();
        journal.update_i64(first, "ns", b"k", 1, None).unwrap();
        journal.apply(first).unwrap();

        let second = journal.begin().unwrap();
        journal.update_i64(second, "ns", b"k", 1, None).unwrap();
        journal.apply(second).unwrap();
        journal.complete(second);

        // The first transaction's compensation must not erase the second
        // transaction's already committed increment.
        journal.undo(first).unwrap();
        assert_eq!(
            journal.backend().get("ns", b"k").unwrap(),
            Some(b"2".to_vec())
        );
    }
}
