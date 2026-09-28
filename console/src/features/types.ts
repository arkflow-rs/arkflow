import type {
  ControlEvent,
  ControlNode,
  EngineStatus,
  Job,
  MetricsResponse,
  Operation,
  StreamStatus,
  SystemResource,
} from '../api'

export type Snapshot = {
  system: SystemResource | null
  status: EngineStatus | null
  nodes: ControlNode[]
  streams: StreamStatus[]
  jobs: Job[]
  operations: Operation[]
  events: ControlEvent[]
  metrics?: MetricsResponse
  totals?: { nodes: number; streams: number; operations: number; events: number }
}
export type Command = (id: string, action: 'start' | 'stop' | 'restart') => Promise<void>
