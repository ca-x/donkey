import { Box, Button, Checkbox, Collapse, FileInput, Group, Modal, NumberInput, Paper, PasswordInput, Select, SimpleGrid, Stack, Switch, Tabs, Text, TextInput, Title } from '@mantine/core'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useForm } from '@mantine/form'
import { IconAdjustments, IconDownload, IconRotateClockwise, IconSettings, IconUpload } from '@tabler/icons-react'
import { notifications } from '@mantine/notifications'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'
import { useAuth } from '../useAuth'

export function SettingsPage() {
  const { t } = useTranslation()
  const [tab, setTab] = useState<string | null>('runtime')
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime })
  if (runtime.isLoading) return <LoadingState />
  if (runtime.error) return <ErrorState error={runtime.error} retry={() => void runtime.refetch()} />
  const config = runtime.data!
  return <Stack gap={24}>
    <PageHeader title={t('settings.title')} description={t('settings.description')} />
    <Tabs value={tab} onChange={setTab} variant="default"><Tabs.List><Tabs.Tab value="runtime">{t('settings.runtimeTitle')}</Tabs.Tab><Tabs.Tab value="account">{t('settings.profileTitle')}</Tabs.Tab></Tabs.List><Tabs.Panel value="runtime" pt="lg"><RuntimeSettingsEditor config={config} /></Tabs.Panel><Tabs.Panel value="account" pt="lg"><AccountSettings /></Tabs.Panel></Tabs>
  </Stack>
}

function AccountSettings() {
  const { t } = useTranslation()
  const user = useAuth()
  const client = useQueryClient()
  const form = useForm({ initialValues: { display_name: user.display_name, username: user.username, current_password: '', new_password: '' } })
  const save = useMutation({ mutationFn: () => api.updateProfile({ display_name: form.values.display_name, username: user.local_password ? form.values.username : undefined, current_password: form.values.current_password || undefined, new_password: form.values.new_password || undefined }), onSuccess: (updated) => { client.setQueryData(['auth-me'], updated); form.setFieldValue('current_password', ''); form.setFieldValue('new_password', ''); notifications.show({ color: 'green', message: t('settings.profileSaved') }) } })
  return <SettingsPanel icon={<IconSettings size={19} />} title={t('settings.profileTitle')}><SimpleGrid cols={{ base: 1, md: 2 }}><TextInput label={t('settings.displayName')} required {...form.getInputProps('display_name')} />{user.local_password && <TextInput label={t('settings.loginName')} required {...form.getInputProps('username')} />}</SimpleGrid>{user.local_password && <SimpleGrid cols={{ base: 1, md: 2 }}><PasswordInput label={t('settings.currentPassword')} {...form.getInputProps('current_password')} /><PasswordInput label={t('settings.newPassword')} description={t('settings.passwordHint')} {...form.getInputProps('new_password')} /></SimpleGrid>}<Group justify="flex-end"><Button onClick={() => save.mutate()} loading={save.isPending}>{t('common.save')}</Button></Group></SettingsPanel>
}

function RuntimeSettingsEditor({ config }: { config: import('../types').RuntimeConfig }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [advanced, setAdvanced] = useState(false)
  const [importFile, setImportFile] = useState<File | null>(null)
  const [pendingImport, setPendingImport] = useState<import('../types').RuntimeSettingsExport | null>(null)
  const [pendingExport, setPendingExport] = useState<import('../types').RuntimeSettingsExport | null>(null)
  const [exportScope, setExportScope] = useState({ runtime: true, routes: true, nodes: true })
  const form = useForm({ initialValues: {
    chunk_size: config.chunk_size, chunk_concurrency: config.chunk_concurrency,
    adaptive_chunking_enabled: config.adaptive_chunking_enabled,
    parallel_threshold: config.parallel_threshold, resumable_threshold: config.resumable_threshold,
    scheduler_policy: config.scheduler_policy, upstream_timeout_seconds: config.upstream_timeout_seconds,
    stream_fallback_timeout_seconds: config.stream_fallback_timeout_seconds, max_cache_bytes: config.max_cache_bytes,
    partial_ttl_seconds: config.partial_ttl_seconds,
    cache_policy: config.cache_policy, cache_high_watermark: config.cache_high_watermark,
    cache_low_watermark: config.cache_low_watermark, cache_ttl_seconds: config.cache_ttl_seconds ?? 0,
    health_interval_seconds: config.health_interval_seconds, max_export_bytes: config.max_export_bytes, export_ttl_seconds: config.export_ttl_seconds,
    pull_logging_enabled: config.pull_logging_enabled,
    pull_log_retention_days: config.pull_log_retention_days, pull_log_max_entries: config.pull_log_max_entries,
  } })
  const save = useMutation({ mutationFn: () => api.updateRuntime(form.values), onSuccess: () => { void client.invalidateQueries({ queryKey: ['runtime'] }) } })
  const exportSettings = useMutation({ mutationFn: api.exportRuntime, onSuccess: setPendingExport })
  const downloadExport = () => { if (!pendingExport) return; const payload = { ...pendingExport, settings: exportScope.runtime ? pendingExport.settings : null, registry_routes: exportScope.routes ? pendingExport.registry_routes : [], nodes: exportScope.nodes ? pendingExport.nodes : [] }; const url = URL.createObjectURL(new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' })); const anchor = document.createElement('a'); anchor.href = url; anchor.download = 'donkey-config.json'; anchor.click(); URL.revokeObjectURL(url); setPendingExport(null) }
  const importSettings = useMutation({ mutationFn: api.importRuntime, onSuccess: () => { setImportFile(null); setPendingImport(null); void client.invalidateQueries({ queryKey: ['runtime'] }); void client.invalidateQueries({ queryKey: ['nodes'] }); void client.invalidateQueries({ queryKey: ['registry-routes'] }); notifications.show({ color: 'green', message: t('settings.imported') }) } })
  const handleImport = async () => { if (!importFile) return; try { const parsed = JSON.parse(await importFile.text()) as import('../types').RuntimeSettingsExport; if (parsed.format !== 'donkey-runtime-settings' || parsed.version !== 1 || !Array.isArray(parsed.nodes) || !Array.isArray(parsed.registry_routes)) throw new Error(t('settings.invalidImport')); setPendingImport(parsed) } catch (error) { notifications.show({ color: 'red', message: error instanceof Error ? error.message : t('settings.invalidImport') }) } }
  const reset = () => form.setValues({
    chunk_size: 2 * 1024 * 1024, chunk_concurrency: 8, parallel_threshold: 8 * 1024 * 1024,
    resumable_threshold: 8 * 1024 * 1024, scheduler_policy: 'balanced', upstream_timeout_seconds: 30,
    stream_fallback_timeout_seconds: 10, partial_ttl_seconds: 3600, max_cache_bytes: 50 * 1024 ** 3,
    cache_policy: 'balanced', cache_high_watermark: 0.9, cache_low_watermark: 0.8,
    cache_ttl_seconds: 0, health_interval_seconds: 60, max_export_bytes: 20 * 1024 ** 3, export_ttl_seconds: 7 * 86400,
    pull_logging_enabled: true,
    adaptive_chunking_enabled: true,
    pull_log_retention_days: 30, pull_log_max_entries: 10000,
  })
  return <SettingsPanel icon={<IconSettings size={19} />} title={t('settings.runtimeTitle')}>
    <Text size="sm" c="dimmed">{t('settings.runtimeDescription')}</Text>
    <Paper className="settings-summary" withBorder><Group gap="sm"><IconAdjustments size={18} /><Box><Text size="sm" fw={650}>{t('settings.recommendedMode')}</Text><Text size="xs" c="dimmed">{t('settings.recommendedDescription')}</Text></Box></Group><Text size="xs" c="dimmed" mt="sm">{t('settings.effectiveSummary', { policy: form.values.scheduler_policy === 'speed-first' ? t('settings.speedFirstPolicy') : t('settings.balancedPolicy'), chunk: form.values.adaptive_chunking_enabled ? `${t('pulls.adaptiveChunking')} 2–8 MiB` : formatSettingBytes(form.values.chunk_size), resumable: formatSettingBytes(form.values.resumable_threshold) })}</Text></Paper>
    <Switch label={t('pulls.adaptiveChunking')} description={t('pulls.adaptiveChunkingDescription')} {...form.getInputProps('adaptive_chunking_enabled', { type: 'checkbox' })} />
    <SimpleGrid cols={{ base: 1, md: 2 }}>
      <NumberInput label={t('settings.concurrency')} min={1} max={64} {...form.getInputProps('chunk_concurrency')} />
      <Select label={t('settings.schedulerPolicy')} data={[{ value: 'balanced', label: t('settings.balancedPolicy') }, { value: 'speed-first', label: t('settings.speedFirstPolicy') }]} {...form.getInputProps('scheduler_policy')} />
      <NumberInput label={t('settings.upstreamTimeout')} min={1} max={3600} suffix=" s" {...form.getInputProps('upstream_timeout_seconds')} />
      <UnitInput label={t('cache.capacity')} value={form.values.max_cache_bytes} onChange={(value) => form.setFieldValue('max_cache_bytes', value)} />
      <Select label={t('cache.policy')} data={[{ value: 'balanced', label: t('cache.balanced') }, { value: 'lru', label: t('cache.lru') }, { value: 'lfu', label: t('cache.lfu') }]} {...form.getInputProps('cache_policy')} />
    </SimpleGrid>
    <Switch label={t('pulls.loggingEnabled')} description={t('pulls.loggingDescription')} {...form.getInputProps('pull_logging_enabled', { type: 'checkbox' })} />
    <Group justify="space-between"><Switch checked={advanced} onChange={(event) => setAdvanced(event.currentTarget.checked)} label={t('settings.advancedSettings')} /><Button variant="subtle" leftSection={<IconRotateClockwise size={16} />} onClick={reset}>{t('settings.resetDefaults')}</Button></Group>
    <Collapse expanded={advanced}><SimpleGrid cols={{ base: 1, md: 2 }}>{!form.values.adaptive_chunking_enabled && <UnitInput label={t('settings.chunk')} value={form.values.chunk_size} onChange={(value) => form.setFieldValue('chunk_size', value)} />}<UnitInput label={t('settings.threshold')} value={form.values.parallel_threshold} onChange={(value) => form.setFieldValue('parallel_threshold', value)} /><UnitInput label={t('settings.resumableThreshold')} value={form.values.resumable_threshold} onChange={(value) => form.setFieldValue('resumable_threshold', value)} /><NumberInput label={t('settings.streamFallbackTimeout')} min={1} max={3600} suffix=" s" {...form.getInputProps('stream_fallback_timeout_seconds')} /><NumberInput label={t('settings.partialTtl')} min={60} max={604800} suffix=" s" {...form.getInputProps('partial_ttl_seconds')} /><NumberInput label={t('settings.healthInterval')} min={1} max={86400} suffix=" s" {...form.getInputProps('health_interval_seconds')} /><NumberInput label={t('settings.cacheHighWatermark')} min={0.5} max={1} step={0.01} {...form.getInputProps('cache_high_watermark')} /><NumberInput label={t('settings.cacheLowWatermark')} min={0.1} max={0.99} step={0.01} {...form.getInputProps('cache_low_watermark')} /><NumberInput label={t('cache.ttl')} min={0} suffix=" s" {...form.getInputProps('cache_ttl_seconds')} /><NumberInput label={t('pulls.retentionDays')} description={t('pulls.retentionDaysDescription')} min={1} max={3650} suffix={` ${t('pulls.days')}`} {...form.getInputProps('pull_log_retention_days')} /><NumberInput label={t('pulls.maxEntries')} description={t('pulls.maxEntriesDescription')} min={100} max={1000000} step={100} {...form.getInputProps('pull_log_max_entries')} /></SimpleGrid></Collapse>
    <Group justify="space-between" align="flex-end"><Group align="flex-end"><FileInput label={t('settings.importSettings')} placeholder={t('settings.chooseFile')} value={importFile} onChange={setImportFile} accept="application/json" clearable /><Button variant="default" leftSection={<IconUpload size={16} />} disabled={!importFile} onClick={() => void handleImport()}>{t('settings.previewImport')}</Button><Button variant="default" leftSection={<IconDownload size={16} />} loading={exportSettings.isPending} onClick={() => exportSettings.mutate()}>{t('settings.export')}</Button></Group><Button onClick={() => save.mutate()} loading={save.isPending}>{t('common.save')}</Button></Group>
    <Modal opened={pendingImport !== null} onClose={() => setPendingImport(null)} title={t('settings.importPreview')} centered><Stack><Text size="sm">{t('settings.importCounts', { routes: pendingImport?.registry_routes.length ?? 0, nodes: pendingImport?.nodes.length ?? 0 })}</Text>{pendingImport?.nodes.some((node) => node.auth_mode !== 'none') && <Text size="sm" c="orange">{t('settings.importCredentialsWarning')}</Text>}<Group justify="flex-end"><Button variant="default" onClick={() => setPendingImport(null)}>{t('common.cancel')}</Button><Button loading={importSettings.isPending} onClick={() => pendingImport && importSettings.mutate(pendingImport)}>{t('settings.confirmImport')}</Button></Group></Stack></Modal>
    <Modal opened={pendingExport !== null} onClose={() => setPendingExport(null)} title={t('settings.exportOptions')} centered><Stack><Text size="sm" c="dimmed">{t('settings.exportDescription')}</Text><Checkbox label={t('settings.exportRuntime')} checked={exportScope.runtime} onChange={(event) => setExportScope((scope) => ({ ...scope, runtime: event.currentTarget.checked }))} /><Checkbox label={t('settings.exportRoutes')} checked={exportScope.routes} onChange={(event) => setExportScope((scope) => ({ ...scope, routes: event.currentTarget.checked }))} /><Checkbox label={t('settings.exportNodes')} checked={exportScope.nodes} onChange={(event) => setExportScope((scope) => ({ ...scope, nodes: event.currentTarget.checked }))} /><Group justify="flex-end"><Button variant="default" onClick={() => setPendingExport(null)}>{t('common.cancel')}</Button><Button onClick={downloadExport}>{t('settings.confirmExport')}</Button></Group></Stack></Modal>
    {save.isSuccess && <Text size="xs" c="green">{t('settings.runtimeSaved')}</Text>}
  </SettingsPanel>
}

function formatSettingBytes(value: number) { if (value >= 1024 ** 3) return `${Math.round(value / 1024 ** 3)} GB`; return `${Math.round(value / 1024 ** 2)} MB` }

function UnitInput({ label, value, onChange }: { label: string; value: number; onChange: (value: number) => void }) {
  const units = [{ label: 'KB', multiplier: 1024 }, { label: 'MB', multiplier: 1024 ** 2 }, { label: 'GB', multiplier: 1024 ** 3 }]
  const selected = [...units].reverse().find((unit) => value >= unit.multiplier) ?? units[0]!
  return <Group align="flex-end" gap="xs" wrap="nowrap"><NumberInput label={label} min={1} value={Math.max(1, Math.round(value / selected!.multiplier))} onChange={(next) => { const numeric = typeof next === 'number' ? next : Number(next); if (Number.isFinite(numeric)) onChange(Math.round(numeric * selected!.multiplier)) }} style={{ flex: 1 }} /><Select aria-label={`${label} unit`} data={units.map((unit) => unit.label)} value={selected!.label} onChange={(next) => { const unit = units.find((item) => item.label === next) ?? selected!; onChange(Math.round((value / selected!.multiplier) * unit.multiplier)) }} w={84} /></Group>
}

function SettingsPanel({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) { return <Paper className="panel settings-panel"><Group mb="lg" gap="sm"><Box c="dimmed" aria-hidden="true">{icon}</Box><Title order={2}>{title}</Title></Group><Stack gap="sm">{children}</Stack></Paper> }
