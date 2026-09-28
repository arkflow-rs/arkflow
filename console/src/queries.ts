import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { api, SNAPSHOT_INTERVAL_MS } from './api'

// Live resources refetch on the snapshot cadence and keep the previous payload
// visible on refetch failure so the UI can show a stale banner over the last
// safe snapshot. Key prefix `['live', ...]` marks the resources the SSE-driven
// invalidation in app.tsx refreshes.
const live = {
  refetchInterval: SNAPSHOT_INTERVAL_MS,
  placeholderData: keepPreviousData,
}

export function useSystem() {
  return useQuery({ queryKey: ['live', 'system'], queryFn: api.system, ...live })
}

export function useStatus() {
  return useQuery({ queryKey: ['live', 'status'], queryFn: api.status, ...live })
}

export function useNodes() {
  return useQuery({ queryKey: ['live', 'nodes'], queryFn: () => api.nodes(), ...live })
}

export function useStreams(nodeId?: string) {
  return useQuery({
    queryKey: ['live', 'streams', nodeId ?? null],
    queryFn: () => api.streams(nodeId),
    ...live,
  })
}

export function useJobs() {
  return useQuery({ queryKey: ['live', 'jobs'], queryFn: () => api.jobs(), ...live })
}

export function useJobDetail(jobId: string | undefined) {
  return useQuery({
    queryKey: ['live', 'job-detail', jobId ?? null],
    queryFn: () => api.jobDetail(jobId!),
    enabled: Boolean(jobId),
    refetchInterval: 5_000,
    placeholderData: keepPreviousData,
  })
}

export function useOperations(nodeId?: string) {
  return useQuery({
    queryKey: ['live', 'operations', nodeId ?? null],
    queryFn: () => api.operations(nodeId),
    ...live,
  })
}

export function useEvents(nodeId?: string) {
  return useQuery({
    queryKey: ['live', 'events', nodeId ?? null],
    queryFn: () => api.events(nodeId),
    ...live,
  })
}

export function useMetrics(nodeId?: string) {
  return useQuery({
    queryKey: ['live', 'metrics', nodeId ?? null],
    queryFn: () => api.metrics(nodeId).then((metrics) => metrics ?? null),
    ...live,
  })
}

export function useRollouts() {
  return useQuery({ queryKey: ['live', 'rollouts'], queryFn: () => api.rollouts(), ...live })
}

export function useRolloutDetail(rolloutId: string | undefined) {
  return useQuery({
    queryKey: ['live', 'rollout-detail', rolloutId ?? null],
    queryFn: () => api.rollout(rolloutId!),
    enabled: Boolean(rolloutId),
    refetchInterval: 5_000,
    placeholderData: keepPreviousData,
  })
}
