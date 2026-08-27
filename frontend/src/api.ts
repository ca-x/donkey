import type {
  CacheEntry,
  ConvertOutput,
  DashboardData,
  Mapping,
  MappingInput,
  NodeInput,
  NodeView,
  RegistryRoute,
  RegistryRouteInput,
  RuntimeConfig,
  RuntimeSettingsExport,
  RegistryCredential,
  ImageJob,
  ImageJobInput,
  ImageSyncRule,
  ImageFileEntry,
  AuthConfig,
  AuthUser,
  HealthData,
  PullEventPage,
} from './types'
import { adminUrl } from './basePath'

interface ErrorEnvelope {
  error?: {
    code?: string
    message?: string
  }
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message)
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(adminUrl(`/api${path}`), {
    ...init,
    credentials: 'same-origin',
    headers: {
      Accept: 'application/json',
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers,
    },
  })
  if (!response.ok) {
    let body: ErrorEnvelope = {}
    try {
      body = (await response.json()) as ErrorEnvelope
    } catch {
      // A reverse proxy may return a non-JSON error page.
    }
    throw new ApiError(
      body.error?.message ?? `请求失败 (${response.status})`,
      response.status,
      body.error?.code,
    )
  }
  if (response.status === 204) return undefined as T
  return (await response.json()) as T
}

const json = (value: unknown) => JSON.stringify(value)

export const api = {
  health: () => request<HealthData>('/health'),
  authConfig: () => request<AuthConfig>('/auth/config'),
  me: () => request<AuthUser>('/auth/me'),
  login: (username: string, password: string) => request<AuthUser>('/auth/login', { method: 'POST', body: json({ username, password }) }),
  logout: () => request<void>('/auth/logout', { method: 'POST' }),
  updateProfile: (input: { display_name: string; username?: string; current_password?: string; new_password?: string }) => request<AuthUser>('/auth/profile', { method: 'PUT', body: json(input) }),
  dashboard: () => request<DashboardData>('/dashboard'),
  nodes: () => request<NodeView[]>('/nodes'),
  createNode: (input: NodeInput) =>
    request<NodeView>('/nodes', { method: 'POST', body: json(input) }),
  updateNode: (id: string, input: NodeInput) =>
    request<NodeView>(`/nodes/${id}`, { method: 'PUT', body: json(input) }),
  deleteNode: (id: string) => request<void>(`/nodes/${id}`, { method: 'DELETE' }),
  probeNode: (id: string) => request<NodeView>(`/nodes/${id}/probe`, { method: 'POST' }),
  registryRoutes: () => request<RegistryRoute[]>('/registry-routes'),
  createRegistryRoute: (input: RegistryRouteInput) =>
    request<RegistryRoute>('/registry-routes', { method: 'POST', body: json(input) }),
  updateRegistryRoute: (id: string, input: RegistryRouteInput) =>
    request<RegistryRoute>(`/registry-routes/${id}`, { method: 'PUT', body: json(input) }),
  deleteRegistryRoute: (id: string) => request<void>(`/registry-routes/${id}`, { method: 'DELETE' }),
  cache: (limit = 250) => request<CacheEntry[]>(`/cache?limit=${limit}`),
  deleteCache: (key: string) => request<void>(`/cache/${key}`, { method: 'DELETE' }),
  clearCache: () => request<{ freed_bytes: number }>('/cache/clear', { method: 'DELETE' }),
  pullEvents: (page = 1, pageSize = 50) => request<PullEventPage>(`/pull-events?page=${page}&page_size=${pageSize}`),
  clearPullEvents: () => request<{ deleted: number }>('/pull-events', { method: 'DELETE' }),
  mappings: () => request<Mapping[]>('/mappings'),
  createMapping: (input: MappingInput) =>
    request<Mapping>('/mappings', { method: 'POST', body: json(input) }),
  updateMapping: (id: string, input: MappingInput) =>
    request<Mapping>(`/mappings/${id}`, { method: 'PUT', body: json(input) }),
  deleteMapping: (id: string) => request<void>(`/mappings/${id}`, { method: 'DELETE' }),
  convert: (url: string) =>
    request<ConvertOutput>('/domainfold/convert', { method: 'POST', body: json({ url }) }),
  runtime: () => request<RuntimeConfig>('/runtime'),
  updateRuntime: (input: Omit<RuntimeConfig, 'admin_addr' | 'registry_addr' | 'tls_enabled' | 'private_upstreams' | 'cache_used_bytes' | 'cache_entries' | 'max_export_bytes' | 'export_ttl_seconds' | 'admin_external_tls' | 'admin_external_loopback' | 'registry_external_tls' | 'registry_auth_enabled'>) => request<RuntimeConfig>('/runtime', { method: 'PUT', body: json(input) }),
  exportRuntime: () => request<RuntimeSettingsExport>('/runtime/export'),
  importRuntime: (input: RuntimeSettingsExport) => request<RuntimeConfig>('/runtime/import', { method: 'POST', body: json(input) }),
  imageCredentials: () => request<RegistryCredential[]>('/image-tools/credentials'),
  createImageCredential: (input: { name: string; registry: string; auth_mode: string; username: string | null; secret: string }) => request<RegistryCredential>('/image-tools/credentials', { method: 'POST', body: json(input) }),
  updateImageCredential: (id: string, input: { name: string; registry: string; auth_mode: string; username: string | null; secret: string }) => request<RegistryCredential>(`/image-tools/credentials/${id}`, { method: 'PUT', body: json(input) }),
  deleteImageCredential: (id: string) => request<void>(`/image-tools/credentials/${id}`, { method: 'DELETE' }),
  imageJobs: () => request<ImageJob[]>('/image-tools/jobs?limit=200'),
  createImageJob: (input: ImageJobInput, idempotencyKey?: string) => request<ImageJob>('/image-tools/jobs', { method: 'POST', headers: idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : undefined, body: json(input) }),
  cancelImageJob: (id: string) => request<void>(`/image-tools/jobs/${id}`, { method: 'DELETE' }),
  purgeImageJob: (id: string) => request<void>(`/image-tools/jobs/${id}/purge`, { method: 'DELETE' }),
  retryImageJob: (id: string) => request<ImageJob>(`/image-tools/jobs/${id}/retry`, { method: 'POST' }),
  imageFiles: (id: string, path = '') => request<ImageFileEntry[]>(`/image-tools/jobs/${id}/files?path=${encodeURIComponent(path)}`),
  imageRules: () => request<ImageSyncRule[]>('/image-tools/sync-rules'),
  createImageRule: (input: Omit<ImageSyncRule, 'id' | 'last_digest' | 'last_run_at' | 'next_run_at'>) => request<ImageSyncRule>('/image-tools/sync-rules', { method: 'POST', body: json(input) }),
  runImageRule: (id: string) => request<ImageJob>(`/image-tools/sync-rules/${id}/run`, { method: 'POST' }),
  deleteImageRule: (id: string) => request<void>(`/image-tools/sync-rules/${id}`, { method: 'DELETE' }),
}

export function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1)
  return `${(value / 1024 ** index).toFixed(index > 1 ? 1 : 0)} ${units[index]}`
}

export function formatRate(value: number): string {
  return `${formatBytes(value)}/s`
}
