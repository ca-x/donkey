import {
  ActionIcon,
  Box,
  Button,
  Group,
  Paper,
  Progress,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Tooltip,
} from '@mantine/core'
import { useState } from 'react'
import { notifications } from '@mantine/notifications'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { IconClock, IconDatabase, IconFlame, IconTrash } from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { api, formatBytes } from '../api'
import { MetricCard } from '../components/MetricCard'
import { NamedConfirmDialog } from '../components/NamedConfirmDialog'
import { PageHeader } from '../components/PageHeader'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import type { CacheEntry } from '../types'
import { useAuth } from '../useAuth'

export function CachePage() {
  const { t, i18n } = useTranslation()
  const canWrite = useAuth().role === 'admin'
  const client = useQueryClient()
  const [pendingDelete, setPendingDelete] = useState<CacheEntry | null>(null)
  const [clearRequested, setClearRequested] = useState(false)
  const cache = useQuery({ queryKey: ['cache'], queryFn: () => api.cache(500) })
  const runtime = useQuery({ queryKey: ['runtime'], queryFn: api.runtime })
  const remove = useMutation({
    mutationFn: api.deleteCache,
    onSuccess: () => {
      setPendingDelete(null)
      void client.invalidateQueries({ queryKey: ['cache'] })
      void client.invalidateQueries({ queryKey: ['dashboard'] })
      notifications.show({ color: 'green', message: t('cache.removed') })
    },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('cache.removeFailed'), message: error.message }),
  })
  const clear = useMutation({ mutationFn: api.clearCache, onSuccess: () => { setClearRequested(false); void client.invalidateQueries({ queryKey: ['cache'] }); void client.invalidateQueries({ queryKey: ['dashboard'] }); notifications.show({ color: 'green', message: t('cache.cleared') }) }, onError: (error: Error) => notifications.show({ color: 'red', title: t('cache.removeFailed'), message: error.message }) })
  if (cache.isLoading || runtime.isLoading) return <LoadingState />
  if (cache.error || runtime.error) return <ErrorState error={(cache.error ?? runtime.error)!} retry={() => { void cache.refetch(); void runtime.refetch() }} />

  const entries = cache.data!
  const config = runtime.data!
  const used = config.cache_used_bytes
  const percent = config.max_cache_bytes > 0 ? Math.min(100, used / config.max_cache_bytes * 100) : 0
  const dateFormatter = new Intl.DateTimeFormat(i18n.resolvedLanguage === 'zh' ? 'zh-CN' : 'en', { dateStyle: 'medium', timeStyle: 'short' })
  const policy = t(`cache.${config.cache_policy}`)

  return (
    <Stack gap={24}>
      <PageHeader title={t('cache.title')} description={t('cache.description')} action={canWrite ? <Button color="red" variant="light" leftSection={<IconTrash size={17} />} onClick={() => setClearRequested(true)}>{t('cache.clearAll')}</Button> : undefined} />
      <SimpleGrid cols={{ base: 1, xs: 2, lg: 4 }}>
        <MetricCard label={t('cache.used')} value={formatBytes(used)} detail={`${percent.toFixed(1)}%`} icon={<IconDatabase size={18} />} />
        <MetricCard label={t('cache.capacity')} value={formatBytes(config.max_cache_bytes)} detail={`${config.cache_entries} ${t('cache.objects')}`} icon={<IconDatabase size={18} />} />
        <MetricCard label={t('cache.policy')} value={policy} detail={`${Math.round(config.cache_high_watermark * 100)}% → ${Math.round(config.cache_low_watermark * 100)}%`} icon={<IconFlame size={18} />} />
        <MetricCard label={t('cache.ttl')} value={config.cache_ttl_seconds ? formatDuration(config.cache_ttl_seconds) : t('cache.forever')} detail={t('cache.watermarks')} icon={<IconClock size={18} />} />
      </SimpleGrid>
      <Paper className="panel cache-usage-panel">
        <Group justify="space-between" mb={8}><Text size="sm" fw={650}>{t('cache.used')}</Text><Text size="sm" c="dimmed">{formatBytes(used)} / {formatBytes(config.max_cache_bytes)}</Text></Group>
        <Progress value={percent} size="sm" color={percent >= config.cache_high_watermark * 100 ? 'orange' : 'blue'} aria-label={`${percent.toFixed(1)}%`} />
      </Paper>

      {entries.length === 0 ? <EmptyState title={t('cache.emptyTitle')} description={t('cache.emptyDesc')} /> : (
        <Paper className="panel cache-list-panel">
          <Table.ScrollContainer minWidth={780} className="desktop-cache-table">
            <Table verticalSpacing="sm" horizontalSpacing="md" highlightOnHover>
              <Table.Thead><Table.Tr><Table.Th>{t('cache.digest')}</Table.Th><Table.Th>{t('cache.size')}</Table.Th><Table.Th>{t('cache.hits')}</Table.Th><Table.Th>{t('cache.lastAccess')}</Table.Th><Table.Th aria-label={t('common.delete')} /></Table.Tr></Table.Thead>
              <Table.Tbody>{entries.map((entry) => <CacheRow key={entry.key} entry={entry} date={(value) => dateFormatter.format(new Date(value))} remove={canWrite ? () => setPendingDelete(entry) : undefined} removing={remove.isPending && remove.variables === entry.key} />)}</Table.Tbody>
            </Table>
          </Table.ScrollContainer>
          <Stack className="mobile-cache-list" gap="sm">
            {entries.map((entry) => <CacheCard key={entry.key} entry={entry} date={(value) => dateFormatter.format(new Date(value))} remove={canWrite ? () => setPendingDelete(entry) : undefined} removing={remove.isPending && remove.variables === entry.key} />)}
          </Stack>
        </Paper>
      )}
      <NamedConfirmDialog
        opened={pendingDelete !== null}
        title={t('cache.confirmDeleteTitle')}
        name={pendingDelete?.digest ?? pendingDelete?.key ?? ''}
        consequence={t('cache.confirmDeleteMessage')}
        confirmLabel={t('cache.remove')}
        loading={remove.isPending}
        onCancel={() => setPendingDelete(null)}
        onConfirm={() => pendingDelete && remove.mutate(pendingDelete.key)}
      />
      <NamedConfirmDialog opened={clearRequested} title={t('cache.clearAllTitle')} name={t('cache.clearAllName')} consequence={t('cache.clearAllMessage')} confirmLabel={t('cache.clearAll')} loading={clear.isPending} onCancel={() => setClearRequested(false)} onConfirm={() => clear.mutate()} />
    </Stack>
  )
}

function CacheRow({ entry, date, remove, removing }: { entry: CacheEntry; date: (value: string) => string; remove?: () => void; removing: boolean }) {
  const { t } = useTranslation()
  return <Table.Tr><Table.Td><Box maw={380}><Text ff="monospace" size="xs" truncate>{entry.digest ?? entry.key}</Text><Text size="xs" c="dimmed" truncate mt={3}>{entry.media_type}</Text></Box></Table.Td><Table.Td><Text fw={620}>{formatBytes(entry.size_bytes)}</Text></Table.Td><Table.Td><Text size="sm" fw={620}>{entry.hit_count}</Text></Table.Td><Table.Td><Text size="sm">{date(entry.last_accessed_at)}</Text></Table.Td><Table.Td>{remove && <Tooltip label={t('cache.remove')}><ActionIcon variant="subtle" color="red" aria-label={t('cache.remove')} loading={removing} onClick={remove}><IconTrash size={17} /></ActionIcon></Tooltip>}</Table.Td></Table.Tr>
}

function CacheCard({ entry, date, remove, removing }: { entry: CacheEntry; date: (value: string) => string; remove?: () => void; removing: boolean }) {
  const { t } = useTranslation()
  return <Paper className="cache-mobile-card"><Group justify="space-between" wrap="nowrap"><Box className="cache-mobile-copy"><Text ff="monospace" size="xs" truncate>{entry.digest ?? entry.key}</Text><Text size="xs" c="dimmed">{formatBytes(entry.size_bytes)} · {entry.hit_count} {t('cache.hits')}</Text></Box>{remove && <ActionIcon variant="subtle" color="red" aria-label={t('cache.remove')} loading={removing} onClick={remove}><IconTrash size={17} /></ActionIcon>}</Group><Text size="xs" c="dimmed" mt="sm">{t('cache.lastAccess')}: {date(entry.last_accessed_at)}</Text></Paper>
}

function formatDuration(seconds: number) {
  if (seconds % 86400 === 0) return `${seconds / 86400}d`
  if (seconds % 3600 === 0) return `${seconds / 3600}h`
  return `${seconds}s`
}
