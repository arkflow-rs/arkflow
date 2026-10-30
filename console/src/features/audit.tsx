import { useEffect } from 'react'
import { useSearchParams } from 'react-router'
import { type AuditRecord, errorMessage, formatTime } from '../api'
import { useAudit } from '../queries'
import { useT } from '../i18n'
import { Pagination, SkeletonRows } from './shared'

const PAGE_SIZES = [25, 50, 100]

export function Audit({ onError }: { onError: (message: string) => void }) {
  const t = useT()
  // Server-side pagination and resource filtering: the URL carries the whole
  // view state, replacing the old fetch-once + client text filter.
  const [searchParams, setSearchParams] = useSearchParams()
  const resourceId = searchParams.get('resource_id') ?? undefined
  const page = Math.max(1, Number(searchParams.get('page') ?? '1') || 1)
  const pageSize = Number(searchParams.get('page_size') ?? '50') || 50
  const auditQuery = useAudit({ resource_id: resourceId, page, page_size: pageSize })
  const records = auditQuery.data?.items ?? []
  const total = auditQuery.data?.total
  const pages = Math.max(1, Math.ceil((total ?? 0) / pageSize))

  useEffect(() => {
    if (auditQuery.isError) onError(errorMessage(auditQuery.error))
  }, [auditQuery.isError, auditQuery.error, onError])

  const setParam = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams)
    if (value) next.set(key, value)
    else next.delete(key)
    if (key !== 'page') next.delete('page')
    setSearchParams(next, { replace: true })
  }

  return (
    <section className="panel">
      <div className="panel-title">
        <h3>{t('audit.title')}</h3>
        <div className="actions">
          <input
            aria-label={t('list.filterResourceId')}
            placeholder={t('list.filterResourceId')}
            value={resourceId ?? ''}
            onChange={(event) => setParam('resource_id', event.target.value.trim())}
          />
          <select
            aria-label={t('list.pageSize')}
            value={pageSize}
            onChange={(event) => setParam('page_size', event.target.value)}
          >
            {PAGE_SIZES.map((value) => (
              <option key={value} value={value}>
                {t('list.pageSize')}: {value}
              </option>
            ))}
          </select>
          <span>{total !== undefined ? t('list.total', { count: total }) : ''}</span>
        </div>
      </div>
      {/* Page transitions keep the previous payload (keepPreviousData):
          render skeleton rows instead of flashing the empty state. */}
      {auditQuery.isPending || auditQuery.isPlaceholderData ? (
        <SkeletonRows rows={5} />
      ) : records.length ? (
        records.map((record) => <AuditRow key={record.event_id} record={record} />)
      ) : (
        <p className="empty">
          {t('audit.noMatching')}
          {resourceId && (
            <button onClick={() => setParam('resource_id', '')}>{t('common.clearFilters')}</button>
          )}
        </p>
      )}
      <Pagination page={page} pages={pages} onChange={(next) => setParam('page', String(next))} />
    </section>
  )
}

function AuditRow({ record }: { record: AuditRecord }) {
  const t = useT()
  return (
    <div className="event-row">
      <strong>{record.action}</strong>
      <span>
        {record.outcome}
        {record.failure_code ? ` · ${record.failure_code}` : ''} · {record.actor ?? t('audit.unknownActor')}
      </span>
      <small>
        {formatTime(record.occurred_at_ms)}
        {record.resource_type ? ` · ${record.resource_type}` : ''}
        {record.resource_id ? ` ${record.resource_id}` : ''}
        {record.node_id ? ` · ${record.node_id}` : ''}
        {record.correlation_id ? ` · ${record.correlation_id}` : ''}
      </small>
      {record.message && <p>{record.message}</p>}
    </div>
  )
}
