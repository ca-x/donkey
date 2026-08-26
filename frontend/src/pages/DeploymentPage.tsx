import { ActionIcon, Box, Code, CopyButton, Group, Paper, SimpleGrid, Stack, Tabs, Text, Title, Tooltip } from '@mantine/core'
import { IconCheck, IconCopy, IconInfoCircle, IconLock, IconRoute, IconTerminal2 } from '@tabler/icons-react'
import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { adminBasePath } from '../basePath'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'

export function DeploymentPage() {
  const { t } = useTranslation()
  const [tab, setTab] = useState<string | null>('daemon')
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime })
  if (runtime.isLoading) return <LoadingState />
  if (runtime.error) return <ErrorState error={runtime.error} retry={() => void runtime.refetch()} />
  const config = runtime.data!
  const registryUrl = publicRegistryUrl(config)
  const registryHost = new URL(registryUrl).host
  const helperUrl = `${registryUrl}/helper`
  const daemon = JSON.stringify({ 'registry-mirrors': [registryUrl] }, null, 2)
  return <Stack gap={24}>
    <PageHeader title={t('deployment.title')} description={t('deployment.description')} />
    <SimpleGrid cols={{ base: 1, lg: 2 }}>
      <Panel icon={<IconRoute size={19} />} title={t('settings.endpoints')}><Setting label={t('settings.admin')} value={config.admin_addr} /><Setting label={t('settings.registry')} value={config.registry_addr} /><Setting label={t('settings.tls')} value={t(config.tls_enabled ? 'settings.enabled' : 'settings.disabled')} /></Panel>

    </SimpleGrid>
    <Tabs value={tab} onChange={setTab} variant="default"><Tabs.List><Tabs.Tab value="daemon">{t('settings.dockerConfig')}</Tabs.Tab><Tabs.Tab value="usage">{t('settings.registryUsageTitle')}</Tabs.Tab><Tabs.Tab value="helper">{t('settings.helperTitle')}</Tabs.Tab></Tabs.List><Tabs.Panel value="daemon" pt="lg"><Panel icon={<IconLock size={19} />} title={t('settings.dockerConfig')}><Text size="sm" c="dimmed">{t(config.registry_auth_enabled ? 'settings.composeHint' : 'settings.loginNotRequired')}</Text><CodeBlock value={daemon} /></Panel></Tabs.Panel><Tabs.Panel value="usage" pt="lg"><Panel icon={<IconInfoCircle size={19} />} title={t('settings.registryUsageTitle')}><Text size="sm" c="dimmed">{t('settings.registryUsageDescription')}</Text><SimpleGrid cols={{ base: 1, md: 2 }}><Command label="Docker Hub" value={`docker pull ${registryHost}/library/alpine:latest`} /><Command label="GitHub Container Registry" value={`docker pull ${registryHost}/ghcr/owner/image:tag`} /></SimpleGrid></Panel></Tabs.Panel><Tabs.Panel value="helper" pt="lg"><Panel icon={<IconTerminal2 size={19} />} title={t('settings.helperTitle')}><Text size="sm" c="dimmed">{t('settings.helperDescription')}</Text><Text size="sm" c="dimmed">{t('settings.helperLimitations')}</Text><HelperCommands helperUrl={helperUrl} /></Panel></Tabs.Panel></Tabs>
  </Stack>
}
function publicRegistryUrl(config: import('../types').RuntimeConfig): string { if (adminBasePath) return window.location.origin; const url = new URL(window.location.origin); url.protocol = config.tls_enabled || config.registry_external_tls ? 'https:' : 'http:'; const port = config.registry_addr.match(/:(\d+)$/)?.[1]; if (port) url.port = port; return url.origin }
function Panel({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) { return <Paper className="panel settings-panel"><Group mb="lg" gap="sm"><Box c="dimmed" aria-hidden="true">{icon}</Box><Title order={2}>{title}</Title></Group><Stack gap="sm">{children}</Stack></Paper> }
function Setting({ label, value }: { label: string; value: string }) { return <Group justify="space-between" gap="lg" wrap="nowrap"><Text size="sm" c="dimmed">{label}</Text><Text size="sm" fw={620} ff="monospace" className="setting-value">{value}</Text></Group> }
function Command({ label, value }: { label: string; value: string }) { return <Box><Text size="xs" c="dimmed">{label}</Text><CodeBlock value={value} /></Box> }
function CodeBlock({ value }: { value: string }) { const { t } = useTranslation(); return <Box className="code-block"><Code block>{value}</Code><CopyButton value={value}>{({ copied, copy }) => <Tooltip label={t(copied ? 'common.copied' : 'common.copy')}><ActionIcon className="code-copy" variant="subtle" onClick={copy} aria-label={t('common.copy')}>{copied ? <IconCheck size={17} /> : <IconCopy size={17} />}</ActionIcon></Tooltip>}</CopyButton></Box> }

function HelperCommands({ helperUrl }: { helperUrl: string }) { const [os, setOs] = useState<string | null>('linux'); return <Tabs value={os} onChange={setOs}><Tabs.List><Tabs.Tab value="linux">Linux</Tabs.Tab><Tabs.Tab value="mac">macOS</Tabs.Tab><Tabs.Tab value="windows">Windows</Tabs.Tab></Tabs.List><Tabs.Panel value="linux" pt="md"><Command label="Linux" value={`curl -fsSL ${helperUrl} | sudo sh -s -- configure`} /></Tabs.Panel><Tabs.Panel value="mac" pt="md"><Command label="macOS" value={`curl -fsSL ${helperUrl} | sh -s -- configure`} /></Tabs.Panel><Tabs.Panel value="windows" pt="md"><Command label="Windows PowerShell" value={`irm ${helperUrl}.win | iex`} /></Tabs.Panel></Tabs> }
