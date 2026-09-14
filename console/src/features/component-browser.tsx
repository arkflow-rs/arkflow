import { Component } from '../api'

export const componentKinds = ['input', 'processor', 'output'] as const
export type ComponentKind = typeof componentKinds[number]

export function filterComponents(items: Component[], kind: ComponentKind, query: string) {
  const normalized = query.trim().toLowerCase()
  return items.filter(item => item.kind === kind && (!normalized || `${item.name} ${item.description ?? ''}`.toLowerCase().includes(normalized)))
}

export function ComponentBrowserControls({ kind, query, onKindChange, onQueryChange, count }: { kind: ComponentKind; query: string; onKindChange: (kind: ComponentKind) => void; onQueryChange: (query: string) => void; count: number }) {
  return <div className="component-browser-controls"><input aria-label="Component search" placeholder="Search components" value={query} onChange={event => onQueryChange(event.target.value)} /><div className="component-kind-tabs" role="tablist" aria-label="Component kind">{componentKinds.map(value => <button type="button" role="tab" aria-selected={kind === value} className={kind === value ? 'active' : ''} key={value} onClick={() => onKindChange(value)}>{value}</button>)}</div><small>{count} matching</small></div>
}
