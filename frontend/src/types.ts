export type AuthMode = 'none' | 'basic' | 'bearer' | 'header'
export type RepositoryMode = 'docker_hub_library' | 'passthrough'

export interface AuthUser {
  id: string | null
  username: string
  display_name: string
  role: 'admin' | 'member'
  legacy: boolean
  local_password: boolean
}

export interface AuthConfig {
  local_enabled: boolean
  oidc_enabled: boolean
  oidc_name: string | null
}

export interface NodeMetric {
  node_id: string
  healthy: boolean
  latency_ms: number
  speed_bps: number
  success_rate: number
  current_bps: number
  total_bytes: number
  last_checked_at: string | null
  last_error: string | null
}

export interface NodeModel {
  id: string
  name: string
  url: string
  registry_route_id: string
  enabled: boolean
  priority: number
  cf_preferred: boolean
  connect_ip: string | null
  auth_mode: AuthMode
  auth_username: string | null
  auth_header: string | null
  created_at: string
  updated_at: string
}

export interface NodeView {
  node: NodeModel
  metric: NodeMetric
  score: number
  auth_configured: boolean
  route: RegistryRouteSummary
  max_concurrency: number
}

export interface NodeInput {
  name: string
  url: string
  registry_route_id: string
  enabled: boolean
  priority: number
  max_concurrency: number
  cf_preferred: boolean
  connect_ip: string | null
  auth_mode: AuthMode
  auth_username: string | null
  auth_header: string | null
  auth_secret: string | null
}

export interface RegistryRouteSummary {
  id: string
  key: string
  name: string
  canonical_registry: string
  path_prefix: string | null
  repository_mode: RepositoryMode
  enabled: boolean
}

export interface RegistryRoute extends RegistryRouteSummary {
  is_default: boolean
  created_at: string
  updated_at: string
}

export interface RegistryRouteInput {
  key: string
  name: string
  canonical_registry: string
  path_prefix: string | null
  repository_mode: RepositoryMode
  is_default: boolean
  enabled: boolean
}

export interface CacheEntry {
  key: string
  media_type: string
  size_bytes: number
  digest: string | null
  hit_count: number
  created_at: string
  last_accessed_at: string
}

export interface Mapping {
  id: string
  source_host: string
  upstream_base: string
  public_base: string
  enabled: boolean
  created_at: string
  updated_at: string
}

export interface MappingInput {
  source_host: string
  upstream_base: string
  public_base: string
  enabled: boolean
}

export interface DashboardData {
  nodes: NodeView[]
  cache_entries: number
  cache_bytes: number
  cache_hits: number
  healthy_nodes: number
  registry_requests: number
  registry_bytes: number
}

export interface HealthData {
  status: string
  version: string
}

export interface RuntimeConfig {
  admin_addr: string
  registry_addr: string
  tls_enabled: boolean
  private_upstreams: boolean
  chunk_size: number
  chunk_concurrency: number
  parallel_threshold: number
  resumable_threshold: number
  upstream_timeout_seconds: number
  stream_fallback_timeout_seconds: number
  partial_ttl_seconds: number
  health_interval_seconds: number
  scheduler_policy: 'balanced' | 'speed-first'
  max_cache_bytes: number
  cache_used_bytes: number
  cache_entries: number
  cache_policy: 'balanced' | 'lru' | 'lfu'
  cache_high_watermark: number
  cache_low_watermark: number
  cache_ttl_seconds: number | null
  max_export_bytes: number
  export_ttl_seconds: number
  admin_external_tls: boolean
  admin_external_loopback: boolean
  registry_external_tls: boolean
  registry_auth_enabled: boolean
}

export type RuntimeSettingsInput = Pick<RuntimeConfig, 'chunk_size' | 'chunk_concurrency' | 'parallel_threshold' | 'resumable_threshold' | 'scheduler_policy' | 'upstream_timeout_seconds' | 'stream_fallback_timeout_seconds' | 'partial_ttl_seconds' | 'max_cache_bytes' | 'cache_policy' | 'cache_high_watermark' | 'cache_low_watermark' | 'cache_ttl_seconds' | 'health_interval_seconds' | 'max_export_bytes' | 'export_ttl_seconds'>

export interface RuntimeSettingsExport {
  format: 'donkey-runtime-settings'
  version: 1
  settings: RuntimeSettingsInput | null
  registry_routes: RegistryRoute[]
  nodes: Array<{
    name: string
    url: string
    registry_route: string
    enabled: boolean
    priority: number
    max_concurrency: number
    cf_preferred: boolean
    connect_ip: string | null
    auth_mode: AuthMode
    auth_username: string | null
    auth_header: string | null
  }>
}

export interface ConvertOutput {
  original_url: string
  accelerated_url: string
  mapping_id: string
}

export interface RegistryCredential {
  id: string
  name: string
  registry: string
  auth_mode: 'basic' | 'bearer'
  username: string | null
  credential_configured: boolean
  generation: number
  updated_at: string
}

export interface ImageJob {
  id: string
  kind: 'export' | 'extract' | 'copy'
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'skipped'
  source_ref: string
  source_node_id: string | null
  source_credential_id: string | null
  destination_ref: string | null
  destination_credential_id: string | null
  platform_os: string
  platform_arch: string
  output_format: string | null
  resolved_digest: string | null
  index_digest: string | null
  stage: string
  progress_bytes: number
  total_bytes: number
  artifact_name: string | null
  error: string | null
  cancel_requested: boolean
  created_at: string
  updated_at: string
}

export interface ImageJobInput {
  kind: 'export' | 'extract' | 'copy'
  source_ref: string
  source_node_id: string | null
  source_credential_id: string | null
  destination_ref: string | null
  destination_credential_id: string | null
  platform_os: string
  platform_arch: string
  output_format: 'docker' | 'oci' | null
}

export interface ImageSyncRule {
  id: string
  name: string
  enabled: boolean
  source_ref: string
  source_node_id: string | null
  source_credential_id: string | null
  destination_ref: string
  destination_credential_id: string
  platform_os: string
  platform_arch: string
  cron: string
  timezone: string
  last_digest: string | null
  last_run_at: string | null
  next_run_at: string | null
}

export interface ImageFileEntry {
  path: string
  name: string
  kind: 'directory' | 'file' | 'symlink'
  size: number
}
