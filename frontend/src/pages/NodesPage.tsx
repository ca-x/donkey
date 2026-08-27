import { useState } from 'react'
import {
  ActionIcon,
  Box,
  Button,
  Divider,
  Group,
  Modal,
  NumberInput,
  Paper,
  PasswordInput,
  Select,
  SimpleGrid,
  Stack,
  Switch,
  Tabs,
  Text,
  TextInput,
  Tooltip,
} from '@mantine/core'
import { useForm } from '@mantine/form'
import { notifications } from '@mantine/notifications'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  IconBolt,
  IconCloud,
  IconEdit,
  IconPlus,
  IconRefresh,
  IconRoute,
  IconServer,
  IconTrash,
} from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { api, formatBytes, formatRate } from '../api'
import { NamedConfirmDialog } from '../components/NamedConfirmDialog'
import { PageHeader } from '../components/PageHeader'
import { RegistryRoutesDialog } from '../components/RegistryRoutesDialog'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import type { AuthMode, NodeInput, NodeView, RegistryRoute } from '../types'
import { useAuth } from '../useAuth'

function defaultRouteId(routes: RegistryRoute[]) {
  return routes.find((route) => route.is_default && route.enabled)?.id
    ?? routes.find((route) => route.enabled)?.id
    ?? ''
}

function nodeInitialValues(registryRouteId: string): NodeInput {
  return {
    name: '',
    url: '',
    registry_route_id: registryRouteId,
    enabled: true,
    priority: 100,
    max_concurrency: 8,
    cf_preferred: false,
    connect_ip: null,
    auth_mode: 'none',
    auth_username: null,
    auth_header: null,
    auth_secret: null,
  }
}

export function NodesPage() {
  const { t } = useTranslation()
  const canWrite = useAuth().role === 'admin'
  const [tab, setTab] = useState<string | null>('all')
  const [routesOpened, setRoutesOpened] = useState(false)
  const [dialog, setDialog] = useState<{ opened: boolean; value: NodeView | null; routeId?: string; revision: number }>({ opened: false, value: null, revision: 0 })
  const nodes = useQuery({ queryKey: ['nodes'], queryFn: api.nodes, refetchInterval: 20_000 })
  const routes = useQuery({ queryKey: ['registry-routes'], queryFn: api.registryRoutes })
  const routeList = routes.data ?? []
  const filtered = (nodes.data ?? []).filter((item) => tab === 'all' || item.node.registry_route_id === tab)
  const loading = nodes.isLoading || routes.isLoading
  const loadError = nodes.error ?? routes.error
  const openDialog = (value: NodeView | null, routeId?: string) => setDialog((current) => ({ opened: true, value, routeId, revision: current.revision + 1 }))
  const closeDialog = () => setDialog((current) => ({ ...current, opened: false }))
  const retry = () => {
    void nodes.refetch()
    void routes.refetch()
  }

  return (
    <Stack gap={24}>
      <PageHeader
        title={t('nodes.title')}
        description={t('nodes.description')}
        action={canWrite ? (
          <Group gap="sm">
            <Button variant="default" leftSection={<IconRoute size={18} />} onClick={() => setRoutesOpened(true)}>{t('nodes.manageRoutes')}</Button>
            <Button leftSection={<IconPlus size={18} />} onClick={() => openDialog(null, tab !== 'all' ? tab ?? undefined : undefined)} disabled={routeList.length === 0} className="pressable">{t('nodes.add')}</Button>
          </Group>
        ) : undefined}
      />
      {!loading && !loadError ? (
        <Tabs value={tab} onChange={setTab} variant="default" className="node-tabs">
          <Tabs.List aria-label={t('nodes.routeFilter')}>
            <Tabs.Tab value="all">{t('nodes.all')}</Tabs.Tab>
            {routeList.map((route) => <Tabs.Tab value={route.id} key={route.id}>{route.name}</Tabs.Tab>)}
          </Tabs.List>
        </Tabs>
      ) : null}
      {loading ? <LoadingState label={t('nodes.reading')} /> : null}
      {loadError ? <ErrorState error={loadError} retry={retry} /> : null}
      {!loading && !loadError && filtered.length === 0 ? (
        <EmptyState title={t('nodes.emptyTitle')} description={t('nodes.emptyDesc')} action={canWrite ? <Button variant="light" onClick={() => openDialog(null, tab !== 'all' ? tab ?? undefined : undefined)}>{t('nodes.add')}</Button> : undefined} />
      ) : null}
      <SimpleGrid cols={{ base: 1, xl: 2 }} spacing="md">
        {filtered.map((item) => <NodeCard key={item.node.id} item={item} canWrite={canWrite} edit={() => openDialog(item)} />)}
      </SimpleGrid>
      {canWrite && routeList.length > 0 ? <NodeDialog key={dialog.revision} opened={dialog.opened} value={dialog.value} initialRouteId={dialog.routeId} routes={routeList} close={closeDialog} /> : null}
      {canWrite ? <RegistryRoutesDialog opened={routesOpened} routes={routeList} close={() => setRoutesOpened(false)} /> : null}
    </Stack>
  )
}

function NodeCard({ item, canWrite, edit }: { item: NodeView; canWrite: boolean; edit: () => void }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const probe = useMutation({
    mutationFn: () => api.probeNode(item.node.id),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['nodes'] })
      void client.invalidateQueries({ queryKey: ['dashboard'] })
      notifications.show({ color: 'green', title: t('nodes.measured'), message: t('nodes.measuredMessage', { name: item.node.name }) })
    },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('nodes.measureFailed'), message: error.message }),
  })
  const healthy = item.node.enabled && item.metric.healthy
  return (
    <Paper className="node-card">
      <Group justify="space-between" align="flex-start" wrap="nowrap">
        <Group gap="sm" wrap="nowrap" className="node-title-group">
          <Box c={healthy ? 'green.5' : 'red.5'} className="state-icon" aria-hidden="true">
            {item.node.cf_preferred ? <IconCloud size={21} /> : <IconServer size={21} />}
          </Box>
          <Box className="node-copy">
            <Group gap={7} wrap="wrap">
              <Text fw={680} truncate>{item.node.name}</Text>
              <Text size="xs" fw={620} c={healthy ? 'green.5' : 'red.5'}>{t(healthy ? 'common.online' : item.node.enabled ? 'common.error' : 'common.disabled')}</Text>
              {!item.route.enabled && <Text size="xs" fw={620} c="orange.5">{t('nodes.routeDisabled')}</Text>}
              {item.node.cf_preferred && <Text size="xs" c="dimmed">CFIP</Text>}
              {item.auth_configured && <Text size="xs" c="dimmed">{t('common.configured')}</Text>}
            </Group>
            <Text size="xs" c="dimmed" truncate mt={4}>
              {t('nodes.logicalRegistry')}: {item.route.name} · {item.route.canonical_registry}{item.route.path_prefix ? ` · /${item.route.path_prefix}` : ` · ${t('nodes.rootNamespace')}`}
            </Text>
            <Text size="xs" c="dimmed" truncate mt={2}>{t('nodes.mirrorEndpoint')}: {item.node.url}</Text>
          </Box>
        </Group>
        {canWrite && <Group gap={4} wrap="nowrap">
          <Tooltip label={t('nodes.probe')}><ActionIcon variant="subtle" aria-label={`${t('nodes.probe')} ${item.node.name}`} loading={probe.isPending} onClick={() => probe.mutate()} className="pressable"><IconRefresh size={18} /></ActionIcon></Tooltip>
          <Tooltip label={t('nodes.edit')}><ActionIcon variant="subtle" aria-label={`${t('nodes.edit')} ${item.node.name}`} onClick={edit} className="pressable"><IconEdit size={18} /></ActionIcon></Tooltip>
        </Group>}
      </Group>
      <div className="node-metrics">
        <Metric label={t('nodes.currentRate')} value={formatRate(item.metric.current_bps)} />
        <Metric label={t('nodes.measuredRate')} value={formatRate(item.metric.speed_bps)} />
        <Metric label={t('nodes.latency')} value={`${item.metric.latency_ms} ms`} />
        <Metric label={t('nodes.downloaded')} value={formatBytes(item.metric.total_bytes)} />
        <Metric label={t('nodes.maxConcurrency')} value={`${item.max_concurrency}`} />
      </div>
      <Group justify="space-between" mt="md" gap="sm">
        <Text size="xs" c={item.metric.last_error ? 'red.3' : 'dimmed'} lineClamp={1}>
          {item.metric.last_error ?? t('nodes.successRate', { value: (item.metric.success_rate * 100).toFixed(0), score: item.score.toFixed(2) })}
        </Text>
        <Text size="xs" c="dimmed">P{item.node.priority}</Text>
      </Group>
    </Paper>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return <Box><Text size="xs" c="dimmed">{label}</Text><Text fw={650} mt={3}>{value}</Text></Box>
}

function NodeDialog({ opened, value, initialRouteId, routes, close }: { opened: boolean; value: NodeView | null; initialRouteId?: string; routes: RegistryRoute[]; close: () => void }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [confirmDelete, setConfirmDelete] = useState(false)
  const editing = value
  const form = useForm<NodeInput>({
    mode: 'controlled',
    initialValues: editing ? {
      name: editing.node.name,
      url: editing.node.url,
      registry_route_id: editing.node.registry_route_id,
      enabled: editing.node.enabled,
      priority: editing.node.priority,
      max_concurrency: editing.max_concurrency,
      cf_preferred: editing.node.cf_preferred,
      connect_ip: editing.node.connect_ip,
      auth_mode: editing.node.auth_mode,
      auth_username: editing.node.auth_username,
      auth_header: editing.node.auth_header,
      auth_secret: null,
    } : nodeInitialValues(initialRouteId ?? defaultRouteId(routes)),
    validate: {
      name: (v) => v.trim().length === 0 || v.length > 80 ? t('nodes.validationName') : null,
      url: (v) => /^https?:\/\//.test(v) ? null : t('nodes.validationUrl'),
      registry_route_id: (v) => routes.some((route) => route.id === v && (route.enabled || route.id === editing?.node.registry_route_id)) ? null : t('nodes.validationRoute'),
      auth_username: (v, values) => values.auth_mode === 'basic' && !v?.trim() ? t('nodes.validationUsername') : null,
      auth_header: (v, values) => values.auth_mode === 'header' && !v?.trim() ? t('nodes.validationHeader') : null,
      auth_secret: (v, values) => !editing && values.auth_mode !== 'none' && !v ? t('nodes.validationSecret') : null,
    },
  })

  const save = useMutation({
    mutationFn: (input: NodeInput) => editing ? api.updateNode(editing.node.id, input) : api.createNode(input),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['nodes'] })
      void client.invalidateQueries({ queryKey: ['dashboard'] })
      notifications.show({ color: 'green', title: t(editing ? 'nodes.updated' : 'nodes.created'), message: t('nodes.savedMessage') })
      close()
    },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('nodes.saveFailed'), message: error.message }),
  })
  const remove = useMutation({
    mutationFn: () => api.deleteNode(editing!.node.id),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['nodes'] })
      notifications.show({ color: 'green', title: t('nodes.deleted'), message: editing!.node.name })
      setConfirmDelete(false)
      close()
    },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('nodes.deleteFailed'), message: error.message }),
  })

  const routeOptions = routes.map((route) => ({
    value: route.id,
    label: `${route.name} · ${route.canonical_registry}${route.enabled ? '' : ` · ${t('common.disabled')}`}`,
    disabled: !route.enabled && route.id !== editing?.node.registry_route_id,
  }))
  const requestClose = () => {
    if (!save.isPending) close()
  }

  return (
    <Modal.Stack>
      <Modal stackId="node-editor" opened={opened} onClose={requestClose} title={t(editing ? 'nodes.editTitle' : 'nodes.createTitle')} size="lg" centered withCloseButton={!save.isPending} closeOnClickOutside={!save.isPending} closeOnEscape={!save.isPending} classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }} transitionProps={{ transition: 'pop', duration: 220, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}>
        <form aria-busy={save.isPending} onSubmit={form.onSubmit((values) => save.mutate(values))}>
          <fieldset className="pending-form" disabled={save.isPending}>
            <Stack gap="md">
            <SimpleGrid cols={{ base: 1, sm: 2 }}>
              <TextInput label={t('nodes.name')} required {...form.getInputProps('name')} />
              <Select label={t('nodes.registryNamespace')} data={routeOptions} searchable required {...form.getInputProps('registry_route_id')} />
            </SimpleGrid>
            <TextInput label={t('nodes.upstream')} description={t('nodes.upstreamDesc')} required {...form.getInputProps('url')} />
            <SimpleGrid cols={{ base: 1, sm: 2 }}>
              <NumberInput label={t('nodes.priority')} min={0} max={1000} {...form.getInputProps('priority')} />
              <NumberInput label={t('nodes.maxConcurrency')} description={t('concurrencyHelp.nodeDescription')} min={1} max={64} {...form.getInputProps('max_concurrency')} />
              <TextInput label={t('nodes.connectIp')} {...form.getInputProps('connect_ip')} />
            </SimpleGrid>
            <Group gap="xl">
              <Switch label={t('nodes.enableNode')} {...form.getInputProps('enabled', { type: 'checkbox' })} />
              <Switch label={t('nodes.cfip')} {...form.getInputProps('cf_preferred', { type: 'checkbox' })} />
            </Group>
            <Divider label={t('nodes.upstreamAuth')} labelPosition="left" />
            <Select
              label={t('nodes.authMode')}
              data={[{ value: 'none', label: t('nodes.authNone') }, { value: 'basic', label: t('nodes.authBasic') }, { value: 'bearer', label: t('nodes.authBearer') }, { value: 'header', label: t('nodes.authHeader') }]}
              {...form.getInputProps('auth_mode')}
              onChange={(value) => {
                const mode = (value ?? 'none') as AuthMode
                form.setFieldValue('auth_mode', mode)
                if (mode === 'basic' && form.values.url.includes('1ms.run') && !form.values.auth_username) form.setFieldValue('auth_username', '1ms')
              }}
            />
            {form.values.auth_mode === 'basic' && <TextInput label={t('nodes.username')} description={form.values.url.includes('1ms.run') ? t('nodes.oneMsUser') : undefined} {...form.getInputProps('auth_username')} />}
            {form.values.auth_mode === 'header' && <TextInput label={t('nodes.headerName')} {...form.getInputProps('auth_header')} />}
            {form.values.auth_mode !== 'none' && <PasswordInput label={t(editing?.auth_configured ? 'nodes.newSecret' : 'nodes.secret')} description={t('nodes.secretDesc')} autoComplete="new-password" {...form.getInputProps('auth_secret')} />}
            <Group justify="space-between" mt="sm">
              {editing ? <Button type="button" color="red" variant="subtle" leftSection={<IconTrash size={17} />} onClick={() => setConfirmDelete(true)}>{t('common.delete')}</Button> : <span />}
              <Group>
                <Button type="button" variant="default" disabled={save.isPending} onClick={requestClose}>{t('common.cancel')}</Button>
                <Button type="submit" leftSection={<IconBolt size={17} />} loading={save.isPending} className="pressable">{t('nodes.saveNode')}</Button>
              </Group>
            </Group>
            </Stack>
          </fieldset>
        </form>
      </Modal>
      {editing && <NamedConfirmDialog stackId="node-delete-confirm" opened={confirmDelete} title={t('nodes.confirmDeleteTitle')} name={editing.node.name} consequence={t('nodes.confirmDeleteMessage')} loading={remove.isPending} onCancel={() => setConfirmDelete(false)} onConfirm={() => remove.mutate()} />}
    </Modal.Stack>
  )
}
