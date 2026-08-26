import { Box, Button, Group, NumberInput, Paper, Select, SimpleGrid, Stack, Text, Title } from '@mantine/core'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useForm } from '@mantine/form'
import { IconSettings } from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { PageHeader } from '../components/PageHeader'
import { ErrorState, LoadingState } from '../components/States'

export function SettingsPage() {
  const { t } = useTranslation()
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime })
  if (runtime.isLoading) return <LoadingState />
  if (runtime.error) return <ErrorState error={runtime.error} retry={() => void runtime.refetch()} />
  const config = runtime.data!
  return <Stack gap={24}>
    <PageHeader title={t('settings.title')} description={t('settings.description')} />
    <RuntimeSettingsEditor config={config} />
  </Stack>
}

function RuntimeSettingsEditor({ config }: { config: import('../types').RuntimeConfig }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const form = useForm({ initialValues: {
    chunk_size: config.chunk_size, chunk_concurrency: config.chunk_concurrency,
    parallel_threshold: config.parallel_threshold, resumable_threshold: config.resumable_threshold,
    scheduler_policy: config.scheduler_policy, upstream_timeout_seconds: config.upstream_timeout_seconds,
    stream_fallback_timeout_seconds: config.stream_fallback_timeout_seconds, max_cache_bytes: config.max_cache_bytes,
    partial_ttl_seconds: config.partial_ttl_seconds,
    cache_policy: config.cache_policy, cache_high_watermark: config.cache_high_watermark,
    cache_low_watermark: config.cache_low_watermark, cache_ttl_seconds: config.cache_ttl_seconds ?? 0,
    health_interval_seconds: config.health_interval_seconds,
  } })
  const save = useMutation({ mutationFn: () => api.updateRuntime(form.values), onSuccess: () => { void client.invalidateQueries({ queryKey: ['runtime'] }) } })
  return <SettingsPanel icon={<IconSettings size={19} />} title={t('settings.runtimeTitle')}>
    <Text size="sm" c="dimmed">{t('settings.runtimeDescription')}</Text>
    <SimpleGrid cols={{ base: 1, md: 2 }}>
      <UnitInput label={t('settings.chunk')} value={form.values.chunk_size} onChange={(value) => form.setFieldValue('chunk_size', value)} />
      <NumberInput label={t('settings.concurrency')} min={1} max={64} {...form.getInputProps('chunk_concurrency')} />
      <UnitInput label={t('settings.threshold')} value={form.values.parallel_threshold} onChange={(value) => form.setFieldValue('parallel_threshold', value)} />
      <UnitInput label={t('settings.resumableThreshold')} value={form.values.resumable_threshold} onChange={(value) => form.setFieldValue('resumable_threshold', value)} />
      <Select label={t('settings.schedulerPolicy')} data={[{ value: 'balanced', label: t('settings.balancedPolicy') }, { value: 'speed-first', label: t('settings.speedFirstPolicy') }]} {...form.getInputProps('scheduler_policy')} />
      <NumberInput label={t('settings.upstreamTimeout')} min={1} max={3600} suffix=" s" {...form.getInputProps('upstream_timeout_seconds')} />
      <NumberInput label={t('settings.streamFallbackTimeout')} min={1} max={3600} suffix=" s" {...form.getInputProps('stream_fallback_timeout_seconds')} />
      <NumberInput label={t('settings.partialTtl')} min={60} max={604800} suffix=" s" {...form.getInputProps('partial_ttl_seconds')} />
      <NumberInput label={t('settings.healthInterval')} min={1} max={86400} suffix=" s" {...form.getInputProps('health_interval_seconds')} />
      <UnitInput label={t('cache.capacity')} value={form.values.max_cache_bytes} onChange={(value) => form.setFieldValue('max_cache_bytes', value)} />
      <Select label={t('cache.policy')} data={[{ value: 'balanced', label: t('cache.balanced') }, { value: 'lru', label: t('cache.lru') }, { value: 'lfu', label: t('cache.lfu') }]} {...form.getInputProps('cache_policy')} />
      <NumberInput label={t('cache.watermarks')} min={0.1} max={1} step={0.01} {...form.getInputProps('cache_high_watermark')} />
      <NumberInput label={t('settings.cacheLowWatermark')} min={0.1} max={0.99} step={0.01} {...form.getInputProps('cache_low_watermark')} />
      <NumberInput label={t('cache.ttl')} min={0} suffix=" s" {...form.getInputProps('cache_ttl_seconds')} />
    </SimpleGrid>
    <Group justify="flex-end"><Button onClick={() => save.mutate()} loading={save.isPending}>{t('common.save')}</Button></Group>
    {save.isSuccess && <Text size="xs" c="green">{t('settings.runtimeSaved')}</Text>}
  </SettingsPanel>
}

function UnitInput({ label, value, onChange }: { label: string; value: number; onChange: (value: number) => void }) {
  const units = [{ label: 'KB', multiplier: 1024 }, { label: 'MB', multiplier: 1024 ** 2 }, { label: 'GB', multiplier: 1024 ** 3 }]
  const selected = units.reduce((best, unit) => Math.abs(value / unit.multiplier - 1) < Math.abs(value / best.multiplier - 1) ? unit : best, units[0]!)
  return <Group align="flex-end" gap="xs" wrap="nowrap"><NumberInput label={label} min={1} value={Math.max(1, Math.round(value / selected!.multiplier))} onChange={(next) => { const numeric = typeof next === 'number' ? next : Number(next); if (Number.isFinite(numeric)) onChange(Math.round(numeric * selected!.multiplier)) }} style={{ flex: 1 }} /><Select aria-label={`${label} unit`} data={units.map((unit) => unit.label)} value={selected!.label} onChange={(next) => { const unit = units.find((item) => item.label === next) ?? selected!; onChange(Math.round((value / selected!.multiplier) * unit.multiplier)) }} w={84} /></Group>
}

function SettingsPanel({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) { return <Paper className="panel settings-panel"><Group mb="lg" gap="sm"><Box c="dimmed" aria-hidden="true">{icon}</Box><Title order={2}>{title}</Title></Group><Stack gap="sm">{children}</Stack></Paper> }
