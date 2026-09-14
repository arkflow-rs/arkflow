import { describe, expect, it } from 'vitest'
import { dagToJobSpec, edgeIssue, jobSpecToDag } from './job-dag'

describe('Job DAG model', () => {
  const spec = { id: 'orders', version: 2, parallelism: 2, max_parallelism: 32, operators: [{ id: 'in', kind: 'source', config: {} }, { id: 'map', kind: 'map', stateful: true, key_field: 'id', config: { expression: 'x' } }, { id: 'out', kind: 'sink', config: {} }], edges: [{ id: 'in-map', from: 'in', to: 'map', partitioned: true }, { id: 'map-out', from: 'map', to: 'out', partitioned: false }], sources: [{ operator_id: 'in', input_type: 'generate', config: { batch_size: 1 }, time: { mode: 'event_time', timestamp_field: 'ts' } }], sinks: [{ operator_id: 'out', output_type: 'stdout', config: { pretty: true } }], recovery: 'latest_checkpoint' }
  it('round-trips persisted JobSpecs without layout state', () => {
    const graph = jobSpecToDag(spec)
    expect(graph.nodes.map(node => node.data.kind)).toEqual(['source', 'processor', 'sink'])
    expect(dagToJobSpec(graph.nodes, graph.edges, spec)).toMatchObject(spec)
  })
  it('rejects self loops, duplicate and invalid source/sink connections', () => {
    const graph = jobSpecToDag(spec)
    expect(edgeIssue(graph.nodes, graph.edges, { source: 'in', target: 'in' })).toMatch(/itself/)
    expect(edgeIssue(graph.nodes, graph.edges, { source: 'in', target: 'map' })).toMatch(/Duplicate/)
    expect(edgeIssue(graph.nodes, graph.edges, { source: 'out', target: 'map' })).toMatch(/sink/)
    expect(edgeIssue(graph.nodes, graph.edges, { source: 'map', target: 'in' })).toMatch(/source/)
  })
  it('rejects graph cycles', () => {
    const graph = jobSpecToDag(spec)
    expect(edgeIssue(graph.nodes, graph.edges, { source: 'out', target: 'in' })).toMatch(/sink/)
    expect(edgeIssue(graph.nodes, graph.edges, { source: 'map', target: 'in' })).toMatch(/source/)
    const processorOnly = jobSpecToDag({ ...spec, operators: spec.operators.slice(0, 2), sources: [], sinks: [], edges: [{ id: 'in-map', from: 'in', to: 'map', partitioned: true }] })
    expect(edgeIssue(processorOnly.nodes, processorOnly.edges, { source: 'map', target: 'in' })).toMatch(/cycle/)
  })
})
