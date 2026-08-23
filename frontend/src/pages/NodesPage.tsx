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
  IconServer,
  IconTrash,
} from '@tabler/icons-react'
import { api, formatBytes, formatRate } from '../api'
import { useTranslation } from 'react-i18next'
import { PageHeader } from '../components/PageHeader'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import type { AuthMode, NodeInput, NodeKind, NodeView } from '../types'
import { useAuth } from '../useAuth'

const tabKinds: Array<{ value: 'all' | NodeKind; label: string }> = [
  { value: 'all', label: 'nodes.all' },
  { value: 'dockerhub', label: 'nodes.dockerhub' },
  { value: 'ghcr', label: 'nodes.ghcr' },
  { value: 'registry', label: 'nodes.registry' },
]

const initialValues: NodeInput = {
  name: '',
  url: 'https://',
  kind: 'dockerhub',
  route_prefix: null,
  enabled: true,
  priority: 100,
  cf_preferred: false,
  connect_ip: null,
  auth_mode: 'none',
  auth_username: null,
  auth_header: null,
  auth_secret: null,
}

export function NodesPage() {
  const { t } = useTranslation()
  const canWrite = useAuth().role === 'admin'
  const [tab, setTab] = useState<string | null>('all')
  const [editing, setEditing] = useState<NodeView | null | undefined>(undefined)
  const nodes = useQuery({ queryKey: ['nodes'], queryFn: api.nodes, refetchInterval: 20_000 })
  const filtered = (nodes.data ?? []).filter((item) => tab === 'all' || item.node.kind === tab)

  return (
    <Stack gap={24}>
      <PageHeader
        title={t('nodes.title')}
        description={t('nodes.description')}
        action={canWrite ? <Button leftSection={<IconPlus size={18} />} onClick={() => setEditing(null)} className="pressable">{t('nodes.add')}</Button> : undefined}
      />
      <Tabs value={tab} onChange={setTab} variant="default" className="node-tabs">
        <Tabs.List>
          {tabKinds.map((item) => <Tabs.Tab value={item.value} key={item.value}>{t(item.label)}</Tabs.Tab>)}
        </Tabs.List>
      </Tabs>
      {nodes.isLoading ? <LoadingState label={t('nodes.reading')} /> : null}
      {nodes.error ? <ErrorState error={nodes.error} retry={() => void nodes.refetch()} /> : null}
      {!nodes.isLoading && !nodes.error && filtered.length === 0 ? (
        <EmptyState title={t('nodes.emptyTitle')} description={t('nodes.emptyDesc')} action={canWrite ? <Button variant="light" onClick={() => setEditing(null)}>{t('nodes.add')}</Button> : undefined} />
      ) : null}
      <SimpleGrid cols={{ base: 1, xl: 2 }} spacing="md">
        {filtered.map((item) => <NodeCard key={item.node.id} item={item} canWrite={canWrite} edit={() => setEditing(item)} />)}
      </SimpleGrid>
      {canWrite && <NodeDialog value={editing} close={() => setEditing(undefined)} />}
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
              {item.node.cf_preferred && <Text size="xs" c="dimmed">CFIP</Text>}
              {item.node.route_prefix && <Text size="xs" c="dimmed">/{item.node.route_prefix}</Text>}
              {item.auth_configured && <Text size="xs" c="dimmed">{t('common.configured')}</Text>}
            </Group>
            <Text size="xs" c="dimmed" truncate mt={3}>{item.node.url}</Text>
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

function NodeDialog({ value, close }: { value: NodeView | null | undefined; close: () => void }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const opened = value !== undefined
  const editing = value ?? null
  const form = useForm<NodeInput>({
    mode: 'controlled',
    initialValues,
    validate: {
      name: (v) => v.trim().length === 0 ? t('nodes.validationName') : v.length > 80 ? t('nodes.validationName') : null,
      url: (v) => /^https?:\/\//.test(v) ? null : t('nodes.validationUrl'),
      auth_username: (v, values) => values.auth_mode === 'basic' && !v?.trim() ? t('nodes.validationUsername') : null,
      auth_header: (v, values) => values.auth_mode === 'header' && !v?.trim() ? t('nodes.validationHeader') : null,
      auth_secret: (v, values) => !editing && values.auth_mode !== 'none' && !v ? t('nodes.validationSecret') : null,
    },
  })

  const resetFor = (node: NodeView | null) => {
    form.setValues(node ? {
      name: node.node.name,
      url: node.node.url,
      kind: node.node.kind,
      route_prefix: node.node.route_prefix,
      enabled: node.node.enabled,
      priority: node.node.priority,
      cf_preferred: node.node.cf_preferred,
      connect_ip: node.node.connect_ip,
      auth_mode: node.node.auth_mode,
      auth_username: node.node.auth_username,
      auth_header: node.node.auth_header,
      auth_secret: null,
    } : initialValues)
    form.resetDirty()
  }

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
      close()
    },
    onError: (error: Error) => notifications.show({ color: 'red', title: t('nodes.deleteFailed'), message: error.message }),
  })

  return (
    <Modal
      opened={opened}
      onClose={close}
      onEnterTransitionEnd={() => resetFor(editing)}
      title={t(editing ? 'nodes.editTitle' : 'nodes.createTitle')}
      size="lg"
      centered
      classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }}
      transitionProps={{ transition: 'pop', duration: 220, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}
    >
      <form onSubmit={form.onSubmit((values) => save.mutate(values))}>
        <Stack gap="md">
          <SimpleGrid cols={{ base: 1, sm: 2 }}>
            <TextInput label={t('nodes.name')} placeholder={t('nodes.namePlaceholder')} required {...form.getInputProps('name')} />
            <Select label={t('nodes.type')} data={[{ value: 'dockerhub', label: t('nodes.dockerhub') }, { value: 'ghcr', label: t('nodes.ghcr') }, { value: 'registry', label: t('nodes.registry') }]} {...form.getInputProps('kind')} />
          </SimpleGrid>
          <TextInput label={t('nodes.upstream')} description={t('nodes.upstreamDesc')} placeholder="https://docker.1ms.run" required {...form.getInputProps('url')} />
          <TextInput label={t('nodes.routePrefix')} description={t('nodes.routePrefixDesc')} placeholder={form.values.kind === 'ghcr' ? 'ghcr' : t('nodes.optional')} {...form.getInputProps('route_prefix')} />
          <SimpleGrid cols={{ base: 1, sm: 2 }}>
            <NumberInput label={t('nodes.priority')} description={t('nodes.priorityDesc')} min={0} max={1000} {...form.getInputProps('priority')} />
            <TextInput label={t('nodes.connectIp')} description={t('nodes.connectIpDesc')} placeholder={t('nodes.optional')} {...form.getInputProps('connect_ip')} />
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
          {form.values.auth_mode === 'header' && <TextInput label={t('nodes.headerName')} placeholder="X-Registry-Token" {...form.getInputProps('auth_header')} />}
          {form.values.auth_mode !== 'none' && <PasswordInput label={t(editing?.auth_configured ? 'nodes.newSecret' : 'nodes.secret')} description={t('nodes.secretDesc')} autoComplete="new-password" {...form.getInputProps('auth_secret')} />}
          <Group justify="space-between" mt="sm">
            {editing ? <Button type="button" color="red" variant="subtle" leftSection={<IconTrash size={17} />} loading={remove.isPending} onClick={() => remove.mutate()}>{t('common.delete')}</Button> : <span />}
            <Group>
              <Button type="button" variant="default" onClick={close}>{t('common.cancel')}</Button>
              <Button type="submit" leftSection={<IconBolt size={17} />} loading={save.isPending} className="pressable">{t('nodes.saveNode')}</Button>
            </Group>
          </Group>
        </Stack>
      </form>
    </Modal>
  )
}
