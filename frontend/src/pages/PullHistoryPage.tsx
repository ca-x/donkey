import { Badge, Box, Button, Group, Pagination, Paper, Stack, Table, Text } from '@mantine/core'
import { notifications } from '@mantine/notifications'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { IconHistory, IconTrash } from '@tabler/icons-react'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { NamedConfirmDialog } from '../components/NamedConfirmDialog'
import { PageHeader } from '../components/PageHeader'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import type { PullEvent } from '../types'
import { useAuth } from '../useAuth'

export function PullHistoryPage() {
  const { t, i18n } = useTranslation()
  const canWrite = useAuth().role === 'admin'
  const client = useQueryClient()
  const [page, setPage] = useState(1)
  const [clearRequested, setClearRequested] = useState(false)
  const events = useQuery({ queryKey: ['pull-events', page], queryFn: () => api.pullEvents(page, 50), refetchInterval: 15_000 })
  const routes = useQuery({ queryKey: ['registry-routes'], queryFn: api.registryRoutes })
  const clear = useMutation({
    mutationFn: api.clearPullEvents,
    onSuccess: () => {
      setClearRequested(false)
      setPage(1)
      void client.invalidateQueries({ queryKey: ['pull-events'] })
      notifications.show({ color: 'green', message: t('pulls.cleared') })
    },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('pulls.clearFailed'), message: error.message }),
  })
  if (events.isLoading || routes.isLoading) return <LoadingState />
  if (events.error || routes.error) return <ErrorState error={(events.error ?? routes.error)!} retry={() => { void events.refetch(); void routes.refetch() }} />

  const routeNames = new Map(routes.data!.map((route) => [route.id, route.name]))
  const date = new Intl.DateTimeFormat(i18n.resolvedLanguage === 'zh' ? 'zh-CN' : 'en', { dateStyle: 'medium', timeStyle: 'medium' })
  const history = events.data!.items
  const totalPages = Math.max(1, Math.ceil(events.data!.total / events.data!.page_size))
  return <Stack gap={24}>
    <PageHeader
      title={t('pulls.title')}
      description={t('pulls.description')}
      action={canWrite && history.length > 0 ? <Button color="red" variant="light" leftSection={<IconTrash size={17} />} onClick={() => setClearRequested(true)}>{t('pulls.clear')}</Button> : undefined}
    />
    {history.length === 0 ? <EmptyState title={t('pulls.emptyTitle')} description={t('pulls.emptyDescription')} /> : <Paper className="panel">
      <Table.ScrollContainer minWidth={820} className="desktop-cache-table">
        <Table verticalSpacing="sm" horizontalSpacing="md" highlightOnHover>
          <Table.Thead><Table.Tr><Table.Th>{t('pulls.repository')}</Table.Th><Table.Th>{t('pulls.reference')}</Table.Th><Table.Th>{t('pulls.route')}</Table.Th><Table.Th>{t('pulls.digest')}</Table.Th><Table.Th>{t('pulls.time')}</Table.Th><Table.Th>{t('pulls.status')}</Table.Th></Table.Tr></Table.Thead>
          <Table.Tbody>{history.map((event) => <PullRow key={event.id} event={event} route={routeNames.get(event.registry_route_id) ?? event.registry_route_id} date={date} />)}</Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      <Stack className="mobile-cache-list" gap="sm">
        {history.map((event) => <PullCard key={event.id} event={event} route={routeNames.get(event.registry_route_id) ?? event.registry_route_id} date={date} />)}
      </Stack>
      {totalPages > 1 && <Group justify="center" mt="lg"><Pagination value={page} onChange={setPage} total={totalPages} withEdges /></Group>}
    </Paper>}
    <NamedConfirmDialog opened={clearRequested} title={t('pulls.clearTitle')} name={t('pulls.clearName')} consequence={t('pulls.clearMessage')} confirmLabel={t('pulls.clear')} loading={clear.isPending} onCancel={() => setClearRequested(false)} onConfirm={() => clear.mutate()} />
  </Stack>
}

function PullRow({ event, route, date }: { event: PullEvent; route: string; date: Intl.DateTimeFormat }) {
  return <Table.Tr>
    <Table.Td><Text fw={650}>{event.repository}</Text></Table.Td>
    <Table.Td><Badge variant="light" color="gray">{event.reference}</Badge></Table.Td>
    <Table.Td><Text size="sm">{route}</Text></Table.Td>
    <Table.Td><Text ff="monospace" size="xs" maw={260} truncate>{event.resolved_digest ?? '—'}</Text></Table.Td>
    <Table.Td><Text size="sm">{date.format(new Date(event.created_at))}</Text></Table.Td>
    <Table.Td><Badge variant="light" color={event.status_code < 400 ? 'green' : 'red'}>{event.status_code}</Badge></Table.Td>
  </Table.Tr>
}

function PullCard({ event, route, date }: { event: PullEvent; route: string; date: Intl.DateTimeFormat }) {
  return <Paper className="cache-mobile-card">
    <Group justify="space-between" align="flex-start" wrap="nowrap"><Box><Text fw={650}>{event.repository}</Text><Text size="xs" c="dimmed" mt={2}>{route}</Text></Box><Badge variant="light" color="gray">{event.reference}</Badge></Group>
    <Group gap="xs" mt="sm"><IconHistory size={15} aria-hidden="true" /><Text size="xs" c="dimmed">{date.format(new Date(event.created_at))}</Text><Badge ml="auto" size="sm" variant="light" color={event.status_code < 400 ? 'green' : 'red'}>{event.status_code}</Badge></Group>
  </Paper>
}
