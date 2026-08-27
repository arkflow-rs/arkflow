import type { Edge, Node } from '@xyflow/react'

export type JobSpec = Record<string, any>
export type DagNodeData = { label: string; kind: 'source' | 'processor' | 'sink'; component: string; operatorKind?: string; config: Record<string, any>; stateful?: boolean; key_field?: string; description?: string; schema?: any; example?: any }
export type DagNode = Node<DagNodeData>

const clone = <T,>(value: T): T => structuredClone(value)

export function jobSpecToDag(spec: JobSpec): { nodes: DagNode[]; edges: Edge[] } {
  const operators = Array.isArray(spec.operators) ? spec.operators : []
  const sources = Array.isArray(spec.sources) ? spec.sources : []
  const sinks = Array.isArray(spec.sinks) ? spec.sinks : []
  const nodes = operators.map((operator: any, index: number) => {
    const source = sources.find((item: any) => item.operator_id === operator.id)
    const sink = sinks.find((item: any) => item.operator_id === operator.id)
    const kind: DagNodeData['kind'] = source ? 'source' : sink ? 'sink' : 'processor'
    const component = source?.input_type ?? sink?.output_type ?? operator.kind ?? 'processor'
    const config = clone(source?.config ?? sink?.config ?? operator.config ?? {})
    return { id: operator.id, type: 'jobNode', position: { x: (index % 3) * 280, y: Math.floor(index / 3) * 150 }, data: { label: operator.id, kind, component, operatorKind: operator.kind, config, stateful: operator.stateful, key_field: operator.key_field } }
  })
  const edges = (Array.isArray(spec.edges) ? spec.edges : []).map((edge: any) => ({ id: edge.id ?? `${edge.from}-${edge.to}`, source: edge.from, target: edge.to, data: { partitioned: edge.partitioned !== false }, label: edge.partitioned !== false ? 'partitioned' : undefined, animated: false }))
  return { nodes, edges }
}

export function dagToJobSpec(nodes: DagNode[], edges: Edge[], base: JobSpec = {}): JobSpec {
  const sources: any[] = [], sinks: any[] = []
  const operators = nodes.map(node => {
    const data = node.data
    if (data.kind === 'source') sources.push({ operator_id: node.id, input_type: data.component, config: clone(data.config ?? {}), time: clone(base.sources?.find((item: any) => item.operator_id === node.id)?.time ?? { mode: 'processing_time' }) })
    if (data.kind === 'sink') sinks.push({ operator_id: node.id, output_type: data.component, config: clone(data.config ?? {}) })
    const config = clone(data.config ?? {})
    if (data.kind === 'processor' && !config.type) config.type = data.component
    return { id: node.id, kind: data.kind === 'processor' ? (data.operatorKind || 'map') : data.kind, ...(data.stateful ? { stateful: true, key_field: data.key_field || undefined } : {}), config }
  })
  return { ...clone(base), operators, edges: edges.map(edge => ({ id: edge.id, from: edge.source, to: edge.target, partitioned: (edge.data as any)?.partitioned !== false })), sources, sinks }
}

export function edgeIssue(nodes: DagNode[], edges: Edge[], connection: { source: string | null; target: string | null }): string | undefined {
  if (!connection.source || !connection.target) return 'Both endpoints are required'
  if (connection.source === connection.target) return 'A node cannot connect to itself'
  const source = nodes.find(node => node.id === connection.source)?.data
  const target = nodes.find(node => node.id === connection.target)?.data
  if (source?.kind === 'sink') return 'A sink cannot have outgoing edges'
  if (target?.kind === 'source') return 'A source cannot have incoming edges'
  if (edges.some(edge => edge.source === connection.source && edge.target === connection.target)) return 'Duplicate edges are not allowed'
  const adjacency = new Map<string, string[]>()
  for (const edge of edges) adjacency.set(edge.source, [...(adjacency.get(edge.source) ?? []), edge.target])
  adjacency.set(connection.source, [...(adjacency.get(connection.source) ?? []), connection.target])
  const seen = new Set<string>(), stack = new Set<string>()
  const visit = (id: string): boolean => { if (stack.has(id)) return true; if (seen.has(id)) return false; seen.add(id); stack.add(id); if ((adjacency.get(id) ?? []).some(visit)) return true; stack.delete(id); return false }
  if ([...adjacency.keys()].some(visit)) return 'This connection would create a cycle'
  return undefined
}

export function defaultJobSpec(id = 'new-job'): JobSpec {
  return { id, version: 1, parallelism: 1, max_parallelism: 128, operators: [], edges: [], sources: [], sinks: [], state: { backend: 'embedded_kv', namespace: id, format_version: 1 }, checkpoint: { interval_ms: 30000, retention: 3, object_store_uri: 's3://arkflow-checkpoints/' }, recovery: 'latest_checkpoint' }
}
