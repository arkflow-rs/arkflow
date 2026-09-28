import { useEffect, useState } from 'react'
import { api, AuditRecord, errorMessage, formatTime } from '../api'

export function Audit({ onError }: { onError: (message: string) => void }) {
  const [records, setRecords] = useState<AuditRecord[]>()
  const [total, setTotal] = useState<number>()
  const [filter, setFilter] = useState('')
  useEffect(() => {
    api
      .audit()
      .then((page) => {
        setRecords(page.items)
        setTotal(page.total)
      })
      .catch((cause) => onError(errorMessage(cause)))
  }, [onError])
  const visible = (records ?? []).filter(
    (record) =>
      !filter ||
      `${record.action} ${record.actor ?? ''} ${record.resource_type} ${record.resource_id ?? ''} ${
        record.node_id ?? ''
      } ${record.outcome} ${record.message ?? ''}`
        .toLowerCase()
        .includes(filter.toLowerCase()),
  )
  return (
    <section className="panel">
      <div className="panel-title">
        <h3>Audit history</h3>
        <div className="actions">
          <input
            aria-label="Audit filter"
            placeholder="Filter by action, actor, or resource"
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
          />
          <span>
            {records === undefined
              ? 'Loading…'
              : `${visible.length} matching${
                  total !== undefined && total > records.length ? ` · ${total} stored server-side` : ''
                }`}
          </span>
        </div>
      </div>
      {visible.length ? (
        visible.map((record) => <AuditRow key={record.event_id} record={record} />)
      ) : (
        <p className="empty">
          {records === undefined ? 'Loading audit records…' : 'No matching audit records.'}
        </p>
      )}
    </section>
  )
}

function AuditRow({ record }: { record: AuditRecord }) {
  return (
    <div className="event-row">
      <strong>{record.action}</strong>
      <span>
        {record.outcome}
        {record.failure_code ? ` · ${record.failure_code}` : ''} · {record.actor ?? 'unknown actor'}
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
