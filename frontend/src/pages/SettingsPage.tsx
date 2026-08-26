import { Box, Code, CopyButton, Group, Paper, SimpleGrid, Stack, Text, Title, Tooltip, ActionIcon } from '@mantine/core'
import { useQuery } from '@tanstack/react-query'
import { IconArchive, IconCheck, IconCopy, IconLock, IconRoute, IconSettings, IconShieldCheck, IconTerminal2 } from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { api, formatBytes } from '../api'
import { adminBasePath } from '../basePath'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'

export function SettingsPage() {
  const { t } = useTranslation()
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime })
  if (runtime.isLoading) return <LoadingState />
  if (runtime.error) return <ErrorState error={runtime.error} retry={() => void runtime.refetch()} />
  const config = runtime.data!
  const registryUrl = publicRegistryUrl(config)
  const registryHost = new URL(registryUrl).host
  const helperUrl = `${registryUrl}/helper`
  const daemon = JSON.stringify({ 'registry-mirrors': [registryUrl] }, null, 2)
  return <Stack gap={24}>
    <PageHeader title={t('settings.title')} description={t('settings.description')} />
    <SimpleGrid cols={{ base: 1, lg: 2 }}>
      <SettingsPanel icon={<IconRoute size={19} />} title={t('settings.endpoints')}><Setting label={t('settings.admin')} value={config.admin_addr} /><Setting label={t('settings.registry')} value={config.registry_addr} /><Setting label={t('settings.tls')} value={t(config.tls_enabled ? 'settings.enabled' : 'settings.disabled')} tone={config.tls_enabled ? 'green' : 'orange'} /></SettingsPanel>
      <SettingsPanel icon={<IconSettings size={19} />} title={t('settings.scheduler')}><Setting label={t('settings.schedulerPolicy')} value={t(config.scheduler_policy === 'speed-first' ? 'settings.speedFirstPolicy' : 'settings.balancedPolicy')} /><Setting label={t('settings.chunk')} value={formatBytes(config.chunk_size)} /><Setting label={t('settings.concurrency')} value={`${config.chunk_concurrency}`} /><Setting label={t('settings.threshold')} value={formatBytes(config.parallel_threshold)} /></SettingsPanel>
      <SettingsPanel icon={<IconShieldCheck size={19} />} title={t('settings.cache')}><Setting label={t('cache.capacity')} value={formatBytes(config.max_cache_bytes)} /><Setting label={t('cache.policy')} value={t(`cache.${config.cache_policy}`)} /><Setting label={t('cache.watermarks')} value={`${Math.round(config.cache_high_watermark * 100)}% → ${Math.round(config.cache_low_watermark * 100)}%`} /><Setting label={t('settings.private')} value={t(config.private_upstreams ? 'settings.allowed' : 'settings.denied')} tone={config.private_upstreams ? 'orange' : 'green'} /></SettingsPanel>
      <SettingsPanel icon={<IconArchive size={19} />} title={t('imageTools.title')}><Setting label={t('cache.capacity')} value={formatBytes(config.max_export_bytes)} /><Setting label={t('cache.ttl')} value={`${Math.round(config.export_ttl_seconds / 86400)}d`} /><Setting label={t('settings.adminTransport')} value={t(config.admin_external_tls ? 'settings.transportTls' : config.admin_external_loopback ? 'settings.transportLoopback' : 'settings.transportInsecure')} tone={config.admin_external_tls || config.admin_external_loopback ? 'green' : 'orange'} /></SettingsPanel>
      <SettingsPanel icon={<IconLock size={19} />} title={t('settings.dockerConfig')}><Text size="sm" c="dimmed">{t(config.registry_auth_enabled ? 'settings.composeHint' : 'settings.loginNotRequired')}</Text><CodeBlock value={daemon} />{config.registry_auth_enabled && <><Text size="xs" c="dimmed" mt="sm">{t('settings.command')}</Text><CodeBlock value={`docker login ${registryHost}`} /></>}</SettingsPanel>
    </SimpleGrid>
    <SettingsPanel icon={<IconRoute size={19} />} title={t('settings.registryUsageTitle')}>
      <Text size="sm" c="dimmed">{t('settings.registryUsageDescription')}</Text>
      <SimpleGrid cols={{ base: 1, md: 2 }}>
        <Command label="Docker Hub" value={`docker pull ${registryHost}/library/alpine:latest`} />
        <Command label="GitHub Container Registry" value={`docker pull ${registryHost}/ghcr/owner/image:tag`} />
      </SimpleGrid>
    </SettingsPanel>
    <SettingsPanel icon={<IconTerminal2 size={19} />} title={t('settings.helperTitle')}>
      <Text size="sm" c="dimmed">{t('settings.helperDescription')}</Text>
      <Text size="sm" c="dimmed">{t('settings.helperLimitations')}</Text>
      <SimpleGrid cols={{ base: 1, md: 2 }}>
        <Command label="Linux" value={`curl -fsSL ${helperUrl} | sudo sh -s -- configure`} />
        <Command label="macOS" value={`curl -fsSL ${helperUrl} | sh -s -- configure`} />
        <Command label="Windows PowerShell" value={`irm ${helperUrl}.win | iex`} />
        <Command label={t('settings.temporary')} value={`docker pull ${registryHost}/library/alpine:latest`} />
      </SimpleGrid>
    </SettingsPanel>
  </Stack>
}

function publicRegistryUrl(config: import('../types').RuntimeConfig): string {
  if (adminBasePath) return window.location.origin
  const url = new URL(window.location.origin)
  url.protocol = config.tls_enabled || config.registry_external_tls ? 'https:' : 'http:'
  const port = config.registry_addr.match(/:(\d+)$/)?.[1]
  if (port) url.port = port
  return url.origin
}

function SettingsPanel({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) { return <Paper className="panel settings-panel"><Group mb="lg" gap="sm"><Box c="dimmed" aria-hidden="true">{icon}</Box><Title order={2}>{title}</Title></Group><Stack gap="sm">{children}</Stack></Paper> }
function Setting({ label, value, tone = 'gray' }: { label: string; value: string; tone?: string }) { return <Group justify="space-between" gap="lg" wrap="nowrap"><Text size="sm" c="dimmed">{label}</Text><Text size="sm" fw={620} ff="monospace" c={tone === 'gray' ? undefined : `${tone}.6`} className="setting-value">{value}</Text></Group> }
function Command({ label, value }: { label: string; value: string }) { return <Box><Text size="xs" c="dimmed">{label}</Text><CodeBlock value={value} /></Box> }
function CodeBlock({ value }: { value: string }) { const { t } = useTranslation(); return <Box className="code-block"><Code block>{value}</Code><CopyButton value={value}>{({ copied, copy }) => <Tooltip label={t(copied ? 'common.copied' : 'common.copy')}><ActionIcon className="code-copy" variant="subtle" onClick={copy} aria-label={t('common.copy')}>{copied ? <IconCheck size={17} /> : <IconCopy size={17} />}</ActionIcon></Tooltip>}</CopyButton></Box> }
