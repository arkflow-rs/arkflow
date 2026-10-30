import { useEffect, useState } from 'react'
import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  api,
  type AuditListQuery,
  type EventListQuery,
  type OperationListQuery,
  JOB_DETAIL_INTERVAL_MS,
  ROLLOUT_DETAIL_INTERVAL_MS,
  SNAPSHOT_INTERVAL_MS,
} from './api'

// The last error raised by any mounted ['live', ...] query, or null while all
// live resources are healthy. Lets the shell mark the view stale even when the
// failure is in a page-scoped query (e.g. streams) while nodes stays healthy.
export function useLiveError(): Error | null {
  const queryClient = useQueryClient()
  const [error, setError] = useState<Error | null>(null)
  useEffect(() => {
    const cache = queryClient.getQueryCache()
    const recompute = () => {
      const failed = cache
        .getAll()
        .find(
          (query) =>
            Array.isArray(query.queryKey) &&
            query.queryKey[0] === 'live' &&
            query.state.status === 'error' &&
            query.state.fetchStatus !== 'fetching' &&
            query.state.error != null,
        )
      setError(failed ? (failed.state.error as Error) : null)
    }
    recompute()
    return cache.subscribe(recompute)
  }, [queryClient])
  return error
}

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

const DEFAULT_PREVIEW_SIZE = 256

// Webhook subscriptions change only through this page's own mutations —
// not 'live': refreshed by invalidation, not on the snapshot cadence.
export function useWebhooks() {
  return useQuery({ queryKey: ['webhooks'], queryFn: api.webhooks })
}

export function useNodes(label?: string) {
  return useQuery({
    queryKey: ['live', 'nodes', label ?? null],
    queryFn: () => (label ? api.nodes(1, DEFAULT_PREVIEW_SIZE, label) : api.nodes()),
    ...live,
  })
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
    refetchInterval: JOB_DETAIL_INTERVAL_MS,
    placeholderData: keepPreviousData,
  })
}

export function useOperations(query: OperationListQuery = {}) {
  return useQuery({
    queryKey: ['live', 'operations', query],
    queryFn: () => api.operations(query),
    ...live,
  })
}

export function useEvents(query: EventListQuery = {}) {
  return useQuery({
    queryKey: ['live', 'events', query],
    queryFn: () => api.events(query),
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

// Audit history is append-only and paged server-side: not 'live' (no SSE
// invalidation or snapshot polling), but every distinct page/filter is its
// own cache entry so paging never serves a stale first page forever.
export function useAudit(query: AuditListQuery = {}) {
  return useQuery({
    queryKey: ['audit', query],
    queryFn: () => api.audit(query),
    placeholderData: keepPreviousData,
  })
}

export function useConfigApprovals(status?: string) {
  return useQuery({
    queryKey: ['live', 'config-approvals', status ?? null],
    queryFn: () => api.configApprovals(status),
    ...live,
  })
}

export function useRolloutDetail(rolloutId: string | undefined) {
  return useQuery({
    queryKey: ['live', 'rollout-detail', rolloutId ?? null],
    queryFn: () => api.rollout(rolloutId!),
    enabled: Boolean(rolloutId),
    refetchInterval: ROLLOUT_DETAIL_INTERVAL_MS,
    placeholderData: keepPreviousData,
  })
}

// Version history for one Job: fetched through the shared query cache so a
// rollback can invalidate it alongside the job detail.
export function useJobVersions(jobId: string) {
  return useQuery({
    queryKey: ['job-versions', jobId],
    queryFn: () => api.jobVersions(jobId),
    placeholderData: keepPreviousData,
  })
}

// Rollback a Job to a stored version; the version list refetches so the
// restored entry reflects the new current version.
export function useRollbackJobUpgrade(jobId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (operation: string) => api.rollbackJobUpgrade(jobId, operation),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['job-versions', jobId] })
    },
  })
}
