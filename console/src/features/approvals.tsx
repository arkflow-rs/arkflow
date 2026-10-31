import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { api, ConfigApproval, formatTime } from '../api'
import { useT } from '../i18n'
import { useConfirm, usePrompt } from './confirm'
import { useConfigApprovals, useMutationFailure } from '../queries'

/// The approval queue: pending requests first with approve/reject actions,
/// decided/expired history below. Resubmission of a rejected request
/// pre-fills the rollouts page state through a URL hand-off (the new
/// request gets a fresh id; the old record is immutable).
type ApprovalsProps = {
  onError: (message: string) => void
  /** Decide pending requests, enforced as `approve_rollouts`. */
  canMutate?: boolean
  /** Resubmit a rejected request: a submission, which the Hub enforces as
   * `manage_rollouts` (not the approval decision's `approve_rollouts`). */
  canSubmit?: boolean
}

export function Approvals({ onError, canMutate = true, canSubmit = true }: ApprovalsProps) {
  const t = useT()
  const queryClient = useQueryClient()
  const reportMutationFailure = useMutationFailure()
  const confirm = useConfirm()
  const prompt = usePrompt()
  const pending = useConfigApprovals('pending').data ?? []
  const decided = useConfigApprovals().data ?? []
  const [busy, setBusy] = useState(false)
  const [filter, setFilter] = useState<'pending' | 'all'>('pending')
  const shown = filter === 'pending' ? pending : decided
  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['live', 'config-approvals'] })
    void queryClient.invalidateQueries({ queryKey: ['live', 'rollouts'] })
  }
  const decide = async (id: string, decision: 'approve' | 'reject') => {
    // Approving publishes the pinned configuration to the fleet: confirm
    // before acting. Rejecting asks for a reason recorded in the audit.
    const confirmed =
      decision === 'approve'
        ? await confirm({
            title: t('approvals.approveConfirmTitle', { id }),
            body: t('approvals.approveConfirmBody'),
            confirmLabel: t('approvals.approve'),
          })
        : true
    if (!confirmed) return
    const note =
      decision === 'reject'
        ? ((await prompt({
            title: t('approvals.rejectPromptTitle', { id }),
            label: t('approvals.rejectNoteLabel'),
            confirmLabel: t('approvals.reject'),
          })) ?? undefined)
        : undefined
    if (decision === 'reject' && note === undefined) return
    try {
      setBusy(true)
      await api.decideConfigApproval(id, decision, note)
      setBusy(false)
      invalidate()
      toast.success(t('toast.accepted'))
    } catch (cause) {
      setBusy(false)
      reportMutationFailure(onError, cause)
    }
  }
  const resubmit = async (request: ConfigApproval) => {
    try {
      setBusy(true)
      await api.submitConfigApproval(request.config_version_id, request.targets, request.batch_size)
      setBusy(false)
      invalidate()
      toast.success(t('approvals.submitted'))
    } catch (cause) {
      setBusy(false)
      reportMutationFailure(onError, cause)
    }
  }
  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('approvals.title')}</h3>
        <div className="actions">
          <button aria-pressed={filter === 'pending'} onClick={() => setFilter('pending')}>
            {t('approvals.pending')}
          </button>
          <button aria-pressed={filter === 'all'} onClick={() => setFilter('all')}>
            {t('approvals.all')}
          </button>
        </div>
      </div>
      {shown.length === 0 ? (
        <p className="empty">{t('approvals.empty')}</p>
      ) : (
        <div className="table">
          {shown.map((request) => (
            <div className="rollout-row" key={request.request_id}>
              <strong>{request.request_id}</strong>
              <span>
                {request.config_version_id} · {request.targets.join(', ')} ·{' '}
                {t('rollouts.batchSize', { size: request.batch_size })}
              </span>
              <span className={`state ${request.status}`}>{request.status}</span>
              <small>
                {t('approvals.by', { actor: request.requester_id })} · {formatTime(request.created_at_ms)}
              </small>
              {request.status === 'pending' && (
                <div className="actions">
                  <button
                    disabled={!canMutate || busy}
                    title={!canMutate ? t('perm.requires', { action: 'approve_rollouts' }) : undefined}
                    onClick={() => void decide(request.request_id, 'approve')}
                  >
                    {t('approvals.approve')}
                  </button>
                  <button
                    disabled={!canMutate || busy}
                    title={!canMutate ? t('perm.requires', { action: 'approve_rollouts' }) : undefined}
                    onClick={() => void decide(request.request_id, 'reject')}
                  >
                    {t('approvals.reject')}
                  </button>
                </div>
              )}
              {request.status === 'rejected' && (
                <div className="actions">
                  <button
                    disabled={!canSubmit || busy}
                    title={!canSubmit ? t('perm.requires', { action: 'manage_rollouts' }) : undefined}
                    onClick={() => void resubmit(request)}
                  >
                    {t('approvals.resubmit')}
                  </button>
                </div>
              )}
              {request.status === 'approved' && request.rollout_id && (
                <small>{t('approvals.createdRollout', { id: request.rollout_id })}</small>
              )}
              {request.failure_note && (
                <small className="warning">
                  {t('approvals.failureNote', { note: request.failure_note })}
                </small>
              )}
            </div>
          ))}
        </div>
      )}
    </section>
  )
}
