import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  addEdge,
  applyNodeChanges,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api, Component, ControlNode, Job, JobCheckpoint, JobValidation } from '../api'
import {
  dagToJobSpec,
  defaultJobSpec,
  edgeIssue,
  jobSpecToDag,
  type DagNode,
  type DagNodeData,
  type JobSpec,
} from './job-dag'
import { ComponentBrowserControls, ComponentKind, filterComponents } from './component-browser'

const copy = <T,>(v: T): T => structuredClone(v)
function JobNode({ data }: { data: DagNodeData }) {
  return (
    <>
      <Handle type="target" position={Position.Left} />
      <div className={`dag-node ${data.kind}`}>
        <strong>{data.label}</strong>
        <small>
          {data.kind} · {data.component}
        </small>
      </div>
      <Handle type="source" position={Position.Right} />
    </>
  )
}
const nodeTypes = { jobNode: JobNode }

function SchemaForm({
  schema,
  value,
  onChange,
  path = '',
}: {
  schema: any
  value: any
  onChange: (value: any) => void
  path?: string
}) {
  if (!schema) return <KeyValueForm value={value} onChange={onChange} />
  if (schema.oneOf || schema.anyOf) {
    const options = schema.oneOf ?? schema.anyOf
    const valueKeys = typeof value === 'object' && value !== null ? Object.keys(value) : []
    const matches = (item: any) =>
      item &&
      typeof item === 'object' &&
      item.properties &&
      Object.keys(item.properties).length > 0 &&
      Object.keys(item.properties).every((key: string) => valueKeys.includes(key))
    let selected = options.findIndex((item: any) => item.const !== undefined && item.const === value)
    if (selected < 0) selected = options.findIndex(matches)
    return (
      <>
        <select
          value={selected < 0 ? 0 : selected}
          onChange={(event) => onChange(copy(options[Number(event.target.value)].default ?? {}))}
        >
          {options.map((item: any, i: number) => (
            <option key={i} value={i}>
              {item.title ?? item.const ?? item.type ?? `Option ${i + 1}`}
            </option>
          ))}
        </select>
        <SchemaForm
          schema={options[selected < 0 ? 0 : selected]}
          value={value}
          onChange={onChange}
          path={path}
        />
      </>
    )
  }
  if (schema.type === 'object' || schema.properties)
    return (
      <div className="schema-fields">
        {Object.entries(schema.properties ?? {}).map(([key, child]: [string, any]) => (
          <label key={key}>
            {child.title ?? key}
            {child.description && <small>{child.description}</small>}
            <SchemaForm
              schema={child}
              value={value?.[key] ?? child.default ?? ''}
              onChange={(next) => onChange({ ...(value ?? {}), [key]: next })}
              path={`${path}.${key}`}
            />
          </label>
        ))}
        {schema.additionalProperties && <KeyValueForm value={value} onChange={onChange} />}
      </div>
    )
  if (schema.type === 'array') {
    const items = Array.isArray(value) ? value : []
    return (
      <div className="array-fields">
        {items.map((item, i) => (
          <div className="array-row" key={i}>
            <SchemaForm
              schema={schema.items}
              value={item}
              onChange={(next) => onChange(items.map((current, index) => (index === i ? next : current)))}
              path={`${path}[${i}]`}
            />
            <button type="button" onClick={() => onChange(items.filter((_, index) => index !== i))}>
              Remove
            </button>
          </div>
        ))}
        <button type="button" onClick={() => onChange([...items, schema.items?.default ?? ''])}>
          Add item
        </button>
      </div>
    )
  }
  if (schema.enum)
    return (
      <select value={value ?? ''} onChange={(event) => onChange(event.target.value)}>
        <option value="">Select…</option>
        {schema.enum.map((item: any) => (
          <option key={String(item)} value={item}>
            {String(item)}
          </option>
        ))}
      </select>
    )
  const type = schema.type ?? typeof value
  return (
    <input
      type={type === 'integer' || type === 'number' ? 'number' : type === 'boolean' ? 'checkbox' : 'text'}
      checked={type === 'boolean' ? Boolean(value) : undefined}
      value={type === 'boolean' ? undefined : (value ?? '')}
      onChange={(event) =>
        onChange(
          type === 'boolean'
            ? event.target.checked
            : type === 'number' || type === 'integer'
              ? Number(event.target.value)
              : event.target.value,
        )
      }
    />
  )
}
function KeyValueForm({ value, onChange }: { value: any; onChange: (value: any) => void }) {
  // A key the operator is currently retyping. Holding it in local state keeps
  // the input controlled: rejecting an edit (an empty or duplicate name) would
  // otherwise reset the field to the old key and swallow the keystroke.
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const entries = Object.entries(value ?? {})
  const commitRename = (draftKey: string, renamed: string) => {
    setDrafts((current) => {
      const next = { ...current }
      delete next[draftKey]
      return next
    })
    if (renamed === draftKey || renamed.trim() === '') return
    // Own-property check: an inherited name such as `constructor` is a legal
    // config key and must not be rejected by the prototype chain.
    if (Object.hasOwn(value ?? {}, renamed)) return
    const next = { ...(value ?? {}) }
    const carried = next[draftKey]
    delete next[draftKey]
    next[renamed] = carried
    onChange(next)
  }
  return (
    <div className="kv-fields">
      {entries.map(([key, item]) => (
        <div className="array-row" key={key}>
          <input
            aria-label={`Key ${key}`}
            value={drafts[key] ?? key}
            onChange={(event) => setDrafts((current) => ({ ...current, [key]: event.target.value }))}
            onBlur={(event) => commitRename(key, event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') commitRename(key, (event.target as HTMLInputElement).value)
            }}
          />
          <input
            aria-label={`Value ${key}`}
            value={typeof item === 'string' ? item : JSON.stringify(item)}
            onChange={(event) => {
              let nextValue: any = event.target.value
              try {
                nextValue = JSON.parse(nextValue)
              } catch {
                /* keep string */
              }
              onChange({ ...(value ?? {}), [key]: nextValue })
            }}
          />
          <button
            type="button"
            onClick={() => {
              const next = { ...(value ?? {}) }
              delete next[key]
              onChange(next)
            }}
          >
            Remove
          </button>
        </div>
      ))}
      <button type="button" onClick={() => onChange({ ...(value ?? {}), key: '' })}>
        Add property
      </button>
    </div>
  )
}
type Props = {
  mode: 'create' | 'upgrade'
  job?: Job
  savepoint?: JobCheckpoint
  nodes: ControlNode[]
  busy: boolean
  onClose: () => void
  onError: (message: string) => void
  onSaved: () => void
  onRefresh: () => void
  onAction: (label: string, fn: () => Promise<unknown>) => Promise<void>
}
export function JobEditor({
  mode,
  job,
  savepoint,
  nodes,
  busy,
  onClose,
  onError,
  onSaved,
  onRefresh,
  onAction,
}: Props) {
  const initial = useMemo<JobSpec>(() => {
    if (mode === 'upgrade' && job) {
      if (job.spec && typeof job.spec === 'object') return copy(job.spec as JobSpec)
      if (job.spec_json)
        try {
          return JSON.parse(job.spec_json)
        } catch {
          /* validation will explain */
        }
    }
    return defaultJobSpec()
  }, [job, mode])
  const graph = useMemo(() => jobSpecToDag(initial), [initial])
  const [dagNodes, setDagNodes, onNodesChange] = useNodesState<DagNode>(graph.nodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(graph.edges)
  const [spec, setSpec] = useState(initial)
  const [selectedId, setSelectedId] = useState<string>()
  const [components, setComponents] = useState<Component[]>([])
  const [componentLoad, setComponentLoad] = useState<'loading' | 'ready' | 'error'>('loading')
  const [componentKind, setComponentKind] = useState<ComponentKind>('input')
  const [componentQuery, setComponentQuery] = useState('')
  const [nodeIds, setNodeIds] = useState<string[]>(job?.node_ids ?? [])
  const [validation, setValidation] = useState<JobValidation>()
  // The Hub records an upgraded Job as stopped / pending recovery, so the
  // editor reports that state and offers the start action instead of closing
  // as if the Job were running.
  const [upgraded, setUpgraded] = useState<string>()
  const editorTarget = `${mode}:${job?.job_id ?? ''}:${savepoint?.checkpoint_id ?? ''}`
  const [issues, setIssues] = useState<string[]>([])
  const loadComponents = () => {
    setComponentLoad('loading')
    void api
      .components()
      .then((items) => {
        setComponents(items)
        setComponentLoad('ready')
      })
      .catch((cause) => {
        setComponentLoad('error')
        onError(
          typeof cause === 'object' && cause && 'message' in cause
            ? String(cause.message)
            : 'Unable to load components',
        )
      })
  }
  useEffect(() => {
    loadComponents()
  }, [onError])
  // The editor may stay mounted while its target changes (an upgrade opened
  // from the detail panel while a create editor is open). Reset the whole
  // draft — spec, graph, validation, selection — to the new target, so an
  // upgrade never submits a stale create draft.
  useEffect(() => {
    setDagNodes(graph.nodes)
    setEdges(graph.edges)
    setSpec(initial)
    setSelectedId(undefined)
    setValidation(undefined)
    setIssues([])
    setUpgraded(undefined)
    setNodeIds(job?.node_ids ?? [])
  }, [editorTarget])
  const selected = dagNodes.find((node) => node.id === selectedId)
  const updateSpec = (nextNodes = dagNodes, nextEdges = edges, nextBase = spec) => {
    const derived = dagToJobSpec(nextNodes, nextEdges, nextBase)
    setSpec(derived)
    latestSpecJson.current = JSON.stringify(derived)
    setValidation(undefined)
    setIssues([])
  }
  // ReactFlow reports presentation changes (select, dimensions, position) through
  // `onNodesChange` too. Rebuilding the spec for them would clear a successful
  // validation result and disable submission merely because the operator
  // clicked a node, so the derived spec is rebuilt only when its content
  // actually differs from what is already stored.
  const specContent = (nextNodes: DagNode[], nextEdges: Edge[]) =>
    JSON.stringify(dagToJobSpec(nextNodes, nextEdges, spec))
  const updateSpecIfChanged = (nextNodes: DagNode[], nextEdges: Edge[], changesArePresentational = false) => {
    // Presentation changes cannot alter the derived spec, so skip the rebuild
    // (and the validation reset) without comparing anything.
    if (changesArePresentational) return
    if (specContent(nextNodes, nextEdges) === specContent(dagNodes, edges)) return
    updateSpec(nextNodes, nextEdges)
  }
  // Component node identifiers come from a monotonic counter: deriving them
  // from `dagNodes.length + 1` produced duplicates after add/delete/add
  // sequences, which breaks React Flow selection and yields a JobSpec with
  // duplicate operator ids.
  const nodeCounter = useRef(0)
  // Latest committed spec, tracked through a ref so an in-flight validate
  // response can compare against the CURRENT graph rather than the stale
  // closure captured when the request was issued.
  const latestSpecJson = useRef<string>()
  const connect = (connection: Connection) => {
    const issue = edgeIssue(dagNodes, edges, connection)
    if (issue) {
      setIssues([issue])
      return
    }
    const next = addEdge(
      {
        ...connection,
        id: `${connection.source}-${connection.target}`,
        data: { partitioned: true },
        label: 'partitioned',
      },
      edges,
    )
    setEdges(next)
    updateSpec(dagNodes, next)
  }
  const addComponent = (component: Component) => {
    const kind = /input|source/i.test(component.kind)
      ? 'source'
      : /output|sink/i.test(component.kind)
        ? 'sink'
        : 'processor'
    const id = `${component.name.replace(/[^a-zA-Z0-9_-]/g, '-')}-${++nodeCounter.current}`
    const data: DagNodeData = {
      label: id,
      kind,
      component: component.name,
      operatorKind: kind === 'processor' ? 'map' : kind,
      config: copy(component.example ?? {}),
      description: component.description,
      schema: component.schema,
      example: component.example,
    }
    const next = [
      ...dagNodes,
      { id, type: 'jobNode', position: { x: dagNodes.length * 80, y: dagNodes.length * 60 }, data },
    ]
    setDagNodes(next)
    setSelectedId(id)
    updateSpec(next, edges)
  }
  const updateNode = (id: string, data: Partial<DagNodeData>) => {
    const next = dagNodes.map((node) =>
      node.id === id ? { ...node, data: { ...node.data, ...data } } : node,
    )
    setDagNodes(next)
    updateSpec(next, edges)
  }
  const validate = async () => {
    const next = dagToJobSpec(dagNodes, edges, spec)
    setSpec(next)
    // Capture the exact request: a response that arrives after the operator
    // edited the graph (or the target nodes) describes a stale spec and must
    // not re-enable submission for the newer one. The comparison goes through
    // `latestSpecJson` — the async continuation would otherwise see the stale
    // closure captured when the request was issued.
    const validatedSpec = JSON.stringify(next)
    const validatedNodes = JSON.stringify([...nodeIds].sort())
    latestSpecJson.current = validatedSpec
    try {
      const result = await api.validateJob(next, nodeIds)
      if (latestSpecJson.current !== validatedSpec || JSON.stringify([...nodeIds].sort()) !== validatedNodes)
        return
      setValidation(result)
      setIssues(result.valid ? [] : ['The current graph or selected nodes are incompatible'])
    } catch (cause) {
      onError(cause instanceof Error ? cause.message : 'Validation failed')
    }
  }
  const submit = async () => {
    if (!validation?.valid) {
      setIssues(['Validate this exact graph before submitting'])
      return
    }
    if (
      !window.confirm(
        mode === 'create'
          ? 'Create this Job in stopped state?'
          : `Upgrade from ${savepoint?.checkpoint_id ?? 'the selected savepoint'}?`,
      )
    )
      return
    await onAction(mode === 'create' ? 'Creating Job…' : 'Submitting upgrade…', async () => {
      const next = dagToJobSpec(dagNodes, edges, spec)
      if (mode === 'create') {
        await api.createJob(next, nodeIds)
        onSaved()
      } else if (job && savepoint) {
        await api.upgradeJob(job.job_id, next, savepoint.checkpoint_id, job.generation, nodeIds)
        setUpgraded(job.job_id)
      }
    })
  }
  const setGlobal = (key: string, value: any) => {
    const next = { ...spec, [key]: value }
    setSpec(next)
    latestSpecJson.current = JSON.stringify(next)
    setValidation(undefined)
  }
  const nodeComponent =
    selected &&
    components.find(
      (item) =>
        item.name === selected.data.component &&
        item.kind ===
          (selected.data.kind === 'source'
            ? 'input'
            : selected.data.kind === 'sink'
              ? 'output'
              : 'processor'),
    )
  const selectedTime = selected
    ? (spec.sources?.find((item: any) => item.operator_id === selected.id)?.time ?? {
        mode: 'processing_time',
      })
    : undefined
  const palette = filterComponents(components, componentKind, componentQuery)
  return (
    <section className="panel detail job-editor">
      <div className="panel-title">
        <div>
          <span className="eyebrow">{mode === 'create' ? 'CREATE JOB' : 'UPGRADE JOB'}</span>
          <h3>{mode === 'create' ? 'Visual Job orchestrator' : `${job?.job_id} → v${spec.version ?? 1}`}</h3>
          <small>
            {mode === 'upgrade'
              ? `Recovery: ${savepoint?.checkpoint_id}`
              : 'A validated graph creates a stopped Job'}
          </small>
        </div>
        <div className="actions">
          <button onClick={onClose}>Cancel</button>
          <button disabled={busy} onClick={() => void validate()}>
            Validate Plan
          </button>
          <button disabled={busy || !validation?.valid} onClick={() => void submit()}>
            {mode === 'create' ? 'Create stopped' : 'Submit upgrade'}
          </button>
        </div>
      </div>
      <div className="job-settings">
        <label>
          Job ID
          <input
            disabled={mode === 'upgrade'}
            value={spec.id ?? ''}
            onChange={(event) => setGlobal('id', event.target.value)}
          />
        </label>
        <label>
          Version
          <input
            type="number"
            min={1}
            value={spec.version ?? 1}
            onChange={(event) => setGlobal('version', Number(event.target.value))}
          />
        </label>
        <label>
          Parallelism
          <input
            type="number"
            min={1}
            value={spec.parallelism ?? 1}
            onChange={(event) => setGlobal('parallelism', Number(event.target.value))}
          />
        </label>
        <label>
          Max parallelism
          <input
            type="number"
            min={1}
            value={spec.max_parallelism ?? 128}
            onChange={(event) => setGlobal('max_parallelism', Number(event.target.value))}
          />
        </label>
        <label>
          State backend
          <input
            value={spec.state?.backend ?? ''}
            onChange={(event) => setGlobal('state', { ...(spec.state ?? {}), backend: event.target.value })}
          />
        </label>
        <label>
          State namespace
          <input
            value={spec.state?.namespace ?? ''}
            onChange={(event) => setGlobal('state', { ...(spec.state ?? {}), namespace: event.target.value })}
          />
        </label>
        <label>
          State TTL (ms)
          <input
            type="number"
            value={spec.state?.ttl_ms ?? ''}
            onChange={(event) =>
              setGlobal('state', { ...(spec.state ?? {}), ttl_ms: Number(event.target.value) || undefined })
            }
          />
        </label>
        <label>
          Checkpoint URI
          <input
            value={spec.checkpoint?.object_store_uri ?? ''}
            onChange={(event) =>
              setGlobal('checkpoint', { ...(spec.checkpoint ?? {}), object_store_uri: event.target.value })
            }
          />
        </label>
        <label>
          Checkpoint interval
          <input
            type="number"
            value={spec.checkpoint?.interval_ms ?? 30000}
            onChange={(event) =>
              setGlobal('checkpoint', { ...(spec.checkpoint ?? {}), interval_ms: Number(event.target.value) })
            }
          />
        </label>
        <label>
          Recovery
          <select
            value={spec.recovery ?? 'latest_checkpoint'}
            onChange={(event) => setGlobal('recovery', event.target.value)}
          >
            <option value="latest_checkpoint">Latest checkpoint</option>
            <option value="latest_savepoint">Latest savepoint</option>
            <option value="fail">Fail</option>
          </select>
        </label>
      </div>
      <div className="node-picker">
        <span>Target nodes</span>
        {nodes.map((node) => (
          <label key={node.id}>
            <input
              type="checkbox"
              checked={nodeIds.includes(node.id)}
              onChange={(event) => {
                const next = event.target.checked
                  ? [...nodeIds, node.id]
                  : nodeIds.filter((id) => id !== node.id)
                setNodeIds(next)
                setValidation(undefined)
              }}
            />
            {node.id} · {node.state}
          </label>
        ))}
      </div>
      <div className="dag-layout">
        <aside className="palette">
          <h4>Add component</h4>
          <ComponentBrowserControls
            kind={componentKind}
            query={componentQuery}
            onKindChange={setComponentKind}
            onQueryChange={setComponentQuery}
            count={palette.length}
          />
          {palette.map((component) => (
            <button
              type="button"
              key={`${component.kind}-${component.name}`}
              onClick={() => addComponent(component)}
            >
              <strong>{component.name}</strong>
              <small>{component.description ?? component.kind}</small>
            </button>
          ))}
          {componentLoad === 'loading' && <p className="empty">Loading registered components…</p>}
          {componentLoad === 'ready' && palette.length === 0 && (
            <p className="empty">No matching components.</p>
          )}
          {componentLoad === 'error' && (
            <div className="palette-error">
              <p>Component catalogue could not be loaded.</p>
              <button type="button" onClick={loadComponents}>
                Retry
              </button>
            </div>
          )}
        </aside>
        <div className="dag-canvas">
          <ReactFlow
            nodes={dagNodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={(changes) => {
              const presentational = changes.every(
                (change) =>
                  change.type === 'select' || change.type === 'dimensions' || change.type === 'position',
              )
              const next = applyNodeChanges(changes, dagNodes)
              onNodesChange(changes)
              updateSpecIfChanged(next, edges, presentational)
            }}
            onEdgesChange={onEdgesChange}
            onNodesDelete={(deleted) => {
              const ids = new Set(deleted.map((node) => node.id))
              const next = dagNodes.filter((node) => !ids.has(node.id))
              const nextEdges = edges.filter((edge) => !ids.has(edge.source) && !ids.has(edge.target))
              setDagNodes(next)
              setEdges(nextEdges)
              updateSpec(next, nextEdges)
            }}
            onEdgesDelete={(deleted) => {
              const ids = new Set(deleted.map((edge) => edge.id))
              const next = edges.filter((edge) => !ids.has(edge.id))
              setEdges(next)
              updateSpec(dagNodes, next)
            }}
            onConnect={connect}
            onNodeClick={(_, node) => setSelectedId(node.id)}
            onPaneClick={() => setSelectedId(undefined)}
            fitView
          >
            <Background />
            <Controls />
            <MiniMap />
          </ReactFlow>
        </div>
        <aside className="node-properties">
          <h4>{selected ? selected.data.label : 'Job settings'}</h4>
          {selected ? (
            <>
              <p>{selected.data.description ?? 'Configure this operator and its runtime behavior.'}</p>
              <label>
                Component
                <input
                  value={selected.data.component}
                  onChange={(event) => updateNode(selected.id, { component: event.target.value })}
                />
              </label>
              {selected.data.kind === 'processor' && (
                <>
                  <label>
                    Operator kind
                    <select
                      value={selected.data.operatorKind ?? 'map'}
                      onChange={(event) => updateNode(selected.id, { operatorKind: event.target.value })}
                    >
                      <option value="map">Map</option>
                      <option value="filter">Filter</option>
                      <option value="aggregate">Aggregate</option>
                      <option value="window">Window</option>
                      <option value="join">Join</option>
                      <option value="udf">UDF</option>
                    </select>
                  </label>
                  <label>
                    Stateful
                    <input
                      type="checkbox"
                      checked={Boolean(selected.data.stateful)}
                      onChange={(event) => updateNode(selected.id, { stateful: event.target.checked })}
                    />
                  </label>
                  <label>
                    Key field
                    <input
                      value={selected.data.key_field ?? ''}
                      onChange={(event) => updateNode(selected.id, { key_field: event.target.value })}
                    />
                  </label>
                </>
              )}
              {selected.data.kind === 'source' && (
                <>
                  <label>
                    Time mode
                    <select
                      value={selectedTime?.mode ?? 'processing_time'}
                      onChange={(event) => {
                        const sources = (spec.sources ?? []).map((item: any) =>
                          item.operator_id === selected.id
                            ? { ...item, time: { ...(item.time ?? {}), mode: event.target.value } }
                            : item,
                        )
                        setGlobal('sources', sources)
                      }}
                    >
                      <option value="processing_time">Processing time</option>
                      <option value="event_time">Event time</option>
                    </select>
                  </label>
                  {selectedTime?.mode === 'event_time' && (
                    <>
                      <label>
                        Timestamp field
                        <input
                          value={selectedTime.timestamp_field ?? ''}
                          onChange={(event) => {
                            const sources = (spec.sources ?? []).map((item: any) =>
                              item.operator_id === selected.id
                                ? {
                                    ...item,
                                    time: {
                                      ...(item.time ?? {}),
                                      timestamp_field: event.target.value,
                                      watermark: item.time?.watermark ?? {
                                        strategy: 'bounded_out_of_orderness',
                                        out_of_orderness_ms: 0,
                                      },
                                    },
                                  }
                                : item,
                            )
                            setGlobal('sources', sources)
                          }}
                        />
                      </label>
                      <label>
                        Watermark strategy
                        <select
                          value={selectedTime.watermark?.strategy ?? 'bounded_out_of_orderness'}
                          onChange={(event) => {
                            const sources = (spec.sources ?? []).map((item: any) =>
                              item.operator_id === selected.id
                                ? {
                                    ...item,
                                    time: {
                                      ...(item.time ?? {}),
                                      watermark: {
                                        ...(item.time?.watermark ?? {}),
                                        strategy: event.target.value,
                                      },
                                    },
                                  }
                                : item,
                            )
                            setGlobal('sources', sources)
                          }}
                        >
                          <option value="bounded_out_of_orderness">Bounded out-of-orderness</option>
                          <option value="monotonous">Monotonous</option>
                        </select>
                      </label>
                      <label>
                        Out-of-orderness (ms)
                        <input
                          type="number"
                          value={selectedTime.watermark?.out_of_orderness_ms ?? 0}
                          onChange={(event) => {
                            const sources = (spec.sources ?? []).map((item: any) =>
                              item.operator_id === selected.id
                                ? {
                                    ...item,
                                    time: {
                                      ...(item.time ?? {}),
                                      watermark: {
                                        ...(item.time?.watermark ?? {}),
                                        out_of_orderness_ms: Number(event.target.value),
                                      },
                                    },
                                  }
                                : item,
                            )
                            setGlobal('sources', sources)
                          }}
                        />
                      </label>
                      <label>
                        Allowed lateness (ms)
                        <input
                          type="number"
                          value={selectedTime.allowed_lateness_ms ?? 0}
                          onChange={(event) => {
                            const sources = (spec.sources ?? []).map((item: any) =>
                              item.operator_id === selected.id
                                ? {
                                    ...item,
                                    time: {
                                      ...(item.time ?? {}),
                                      allowed_lateness_ms: Number(event.target.value),
                                    },
                                  }
                                : item,
                            )
                            setGlobal('sources', sources)
                          }}
                        />
                      </label>
                      <label>
                        Late events
                        <select
                          value={selectedTime.late_event_policy ?? 'drop'}
                          onChange={(event) => {
                            const sources = (spec.sources ?? []).map((item: any) =>
                              item.operator_id === selected.id
                                ? {
                                    ...item,
                                    time: { ...(item.time ?? {}), late_event_policy: event.target.value },
                                  }
                                : item,
                            )
                            setGlobal('sources', sources)
                          }}
                        >
                          <option value="drop">Drop</option>
                          <option value="route">Route</option>
                          <option value="update">Update</option>
                        </select>
                      </label>
                    </>
                  )}
                </>
              )}
              {nodeComponent?.schema ? (
                <SchemaForm
                  schema={nodeComponent.schema}
                  value={selected.data.config}
                  onChange={(config) => updateNode(selected.id, { config })}
                />
              ) : (
                <KeyValueForm
                  value={selected.data.config}
                  onChange={(config) => updateNode(selected.id, { config })}
                />
              )}
              <button
                type="button"
                onClick={() => {
                  const next = dagNodes.filter((node) => node.id !== selected.id)
                  const nextEdges = edges.filter(
                    (edge) => edge.source !== selected.id && edge.target !== selected.id,
                  )
                  setDagNodes(next)
                  setEdges(nextEdges)
                  setSelectedId(undefined)
                  updateSpec(next, nextEdges)
                }}
              >
                Delete node
              </button>
              {nodeComponent?.example && (
                <details>
                  <summary>Example</summary>
                  <pre className="schema">{JSON.stringify(nodeComponent.example, null, 2)}</pre>
                </details>
              )}
            </>
          ) : (
            <p className="empty">Select a node to edit its component, schema, time, and state settings.</p>
          )}
        </aside>
      </div>
      {upgraded && (
        <div className="success validation">
          <strong>Upgrade accepted</strong>
          <p>
            The Hub recorded Job {upgraded} as stopped and pending recovery; it processes nothing until it is
            started.
          </p>
          <div className="actions">
            <button
              disabled={busy}
              onClick={() =>
                void onAction('Starting…', async () => {
                  await api.setJobState(upgraded, 'running')
                  onRefresh()
                  onSaved()
                })
              }
            >
              Start {upgraded}
            </button>
          </div>
        </div>
      )}
      {validation && (
        <div className={validation.valid ? 'success validation' : 'validation'}>
          <strong>{validation.valid ? 'Plan is valid' : 'Plan needs attention'}</strong>
          {validation.plan !== undefined && (
            <details>
              <summary>Physical plan</summary>
              <pre className="schema">{String(JSON.stringify(validation.plan, null, 2))}</pre>
            </details>
          )}
          {validation.required_capabilities.map((capability) => (
            <p key={capability}>Capability: {capability}</p>
          ))}
          {validation.warnings.map((warning) => (
            <p key={warning}>Warning: {warning}</p>
          ))}
          {validation.nodes
            .filter((node) => !node.compatible)
            .map((node) => (
              <p key={node.node_id}>
                {node.node_id}: missing {node.missing_capabilities.join(', ')}
              </p>
            ))}
        </div>
      )}
      {issues.length > 0 && (
        <div className="validation">
          <strong>{issues.length} issue(s)</strong>
          {issues.map((issue) => (
            <p key={issue}>{issue}</p>
          ))}
        </div>
      )}
    </section>
  )
}
