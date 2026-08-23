export type NodeKind = 'dockerhub' | 'ghcr' | 'registry'
export type AuthMode = 'none' | 'basic' | 'bearer' | 'header'

export interface AuthUser {
  id: string | null
  username: string
  display_name: string
  role: 'admin' | 'member'
  legacy: boolean
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
  kind: NodeKind
  route_prefix: string | null
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
}

export interface NodeInput {
  name: string
  url: string
  kind: NodeKind
  route_prefix: string | null
  enabled: boolean
  priority: number
  cf_preferred: boolean
  connect_ip: string | null
  auth_mode: AuthMode
  auth_username: string | null
  auth_header: string | null
  auth_secret: string | null
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
}

export interface RuntimeConfig {
  admin_addr: string
  registry_addr: string
  tls_enabled: boolean
  private_upstreams: boolean
  chunk_size: number
  chunk_concurrency: number
  parallel_threshold: number
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
