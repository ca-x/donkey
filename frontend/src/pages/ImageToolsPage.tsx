import { useState } from 'react'
import {
  ActionIcon,
  Box,
  Button,
  Checkbox,
  Group,
  Modal,
  Paper,
  PasswordInput,
  Progress,
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
  IconArchive,
  IconArrowUp,
  IconCopy,
  IconDownload,
  IconFile,
  IconFolder,
  IconKey,
  IconPlayerPlay,
  IconRefresh,
  IconRoute,
  IconSearch,
  IconTrash,
  IconX,
} from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { api, formatBytes } from '../api'
import { PageHeader } from '../components/PageHeader'
import { NamedConfirmDialog } from '../components/NamedConfirmDialog'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import type {
  ImageJob,
  ImageJobInput,
  ImageSyncRule,
  NodeView,
  RegistryCredential,
} from '../types'
import { useAuth } from '../useAuth'

const baseJob: ImageJobInput = {
  kind: 'extract',
  source_ref: 'docker.io/library/alpine:latest',
  source_node_id: null,
  source_credential_id: null,
  destination_ref: null,
  destination_credential_id: null,
  platform_os: 'linux',
  platform_arch: 'amd64',
  output_format: null,
}

export function ImageToolsPage() {
  const { t } = useTranslation()
  const canWrite = useAuth().role === 'admin'
  const [tab, setTab] = useState<string | null>('content')
  const [credentialOpen, setCredentialOpen] = useState(false)
  const jobs = useQuery({
    queryKey: ['image-jobs'],
    queryFn: api.imageJobs,
    refetchInterval: (query) => query.state.data?.some((job) => ['pending', 'running'].includes(job.status)) ? 2_000 : 30_000,
  })
  const credentials = useQuery({ queryKey: ['image-credentials'], queryFn: api.imageCredentials })
  const rules = useQuery({ queryKey: ['image-rules'], queryFn: api.imageRules, refetchInterval: 15_000 })
  const nodes = useQuery({ queryKey: ['nodes'], queryFn: api.nodes })
  if (jobs.isLoading || credentials.isLoading || rules.isLoading || nodes.isLoading) return <LoadingState />
  const error = jobs.error ?? credentials.error ?? rules.error ?? nodes.error
  if (error) return <ErrorState error={error} retry={() => { void jobs.refetch(); void credentials.refetch(); void rules.refetch(); void nodes.refetch() }} />

  return <Stack gap={24}>
    <PageHeader
      title={t('imageTools.title')}
      description={t('imageTools.description')}
      action={canWrite ? <Button variant="light" leftSection={<IconKey size={17} />} onClick={() => setCredentialOpen(true)}>{t('imageTools.credentials')}</Button> : undefined}
    />
    <Tabs value={tab} onChange={setTab} variant="default" className="node-tabs image-tools-tabs">
      <Tabs.List>
        <Tabs.Tab value="content" leftSection={<IconSearch size={15} />}>{t('imageTools.content')}</Tabs.Tab>
        <Tabs.Tab value="export" disabled={!canWrite} leftSection={<IconArchive size={15} />}>{t('imageTools.export')}</Tabs.Tab>
        <Tabs.Tab value="copy" disabled={!canWrite} leftSection={<IconCopy size={15} />}>{t('imageTools.copy')}</Tabs.Tab>
        <Tabs.Tab value="schedules" disabled={!canWrite} leftSection={<IconRefresh size={15} />}>{t('imageTools.schedules')}</Tabs.Tab>
        <Tabs.Tab value="tasks" leftSection={<IconRoute size={15} />}>{t('imageTools.tasks')}</Tabs.Tab>
      </Tabs.List>
    </Tabs>
    {tab === 'content' && <ContentPanel jobs={jobs.data!} nodes={nodes.data!} credentials={credentials.data!} canWrite={canWrite} />}
    {tab === 'export' && <JobPanel kind="export" nodes={nodes.data!} credentials={credentials.data!} />}
    {tab === 'copy' && <CopyPanel nodes={nodes.data!} credentials={credentials.data!} openCredentials={() => setCredentialOpen(true)} />}
    {tab === 'schedules' && <SchedulesPanel rules={rules.data!} nodes={nodes.data!} credentials={credentials.data!} />}
    {tab === 'tasks' && <TasksPanel jobs={jobs.data!} canWrite={canWrite} />}
    {canWrite && <CredentialDialog opened={credentialOpen} close={() => setCredentialOpen(false)} credentials={credentials.data!} />}
  </Stack>
}

function sourceNodeOptions(nodes: NodeView[], direct: string) {
  return [{ value: '', label: direct }, ...nodes.map((node) => ({ value: node.node.id, label: `${node.node.name} · ${node.route.name} · ${node.node.url}` }))]
}

function credentialOptions(credentials: RegistryCredential[], empty: string) {
  return [{ value: '', label: empty }, ...credentials.map((credential) => ({ value: credential.id, label: `${credential.name} · ${credential.registry}` }))]
}

function JobPanel({ kind, nodes, credentials }: { kind: 'export' | 'extract'; nodes: NodeView[]; credentials: RegistryCredential[] }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const form = useForm<ImageJobInput>({ mode: 'controlled', initialValues: { ...baseJob, kind, output_format: kind === 'export' ? 'docker' : null } })
  const create = useMutation({ mutationFn: (input: ImageJobInput) => api.createImageJob(input), onSuccess: () => { void client.invalidateQueries({ queryKey: ['image-jobs'] }); notifications.show({ color: 'green', message: t('imageTools.queued') }) }, onError: notifyError(t) })
  return <Paper className="panel image-tool-flow"><form onSubmit={form.onSubmit((value) => create.mutate(value))}><Stack gap="md"><ImageSourceFields form={form} nodes={nodes} credentials={credentials} />{kind === 'export' && <Select label={t('imageTools.outputFormat')} data={[{ value: 'docker', label: t('imageTools.dockerArchive') }, { value: 'oci', label: t('imageTools.ociArchive') }]} {...form.getInputProps('output_format')} />}<Group justify="flex-end"><Button type="submit" loading={create.isPending} leftSection={kind === 'export' ? <IconArchive size={17} /> : <IconSearch size={17} />}>{t(kind === 'export' ? 'imageTools.startExport' : 'imageTools.startExtract')}</Button></Group></Stack></form></Paper>
}

function ContentPanel({ jobs, nodes, credentials, canWrite }: { jobs: ImageJob[]; nodes: NodeView[]; credentials: RegistryCredential[]; canWrite: boolean }) {
  const { t } = useTranslation()
  const completed = jobs.filter((job) => job.kind === 'extract' && job.status === 'completed')
  const [jobId, setJobId] = useState(completed[0]?.id ?? '')
  const [path, setPath] = useState('')
  const files = useQuery({ queryKey: ['image-files', jobId, path], queryFn: () => api.imageFiles(jobId, path), enabled: Boolean(jobId) })
  return <Stack gap="md">{canWrite && <JobPanel kind="extract" nodes={nodes} credentials={credentials} />}{completed.length > 0 && <Paper className="panel"><Group justify="space-between" mb="md"><Select value={jobId} onChange={(value) => { setJobId(value ?? ''); setPath('') }} data={completed.map((job) => ({ value: job.id, label: `${job.source_ref} · ${job.resolved_digest?.slice(0, 20) ?? job.id}` }))} /><Button variant="subtle" leftSection={<IconArrowUp size={16} />} disabled={!path} onClick={() => setPath(path.split('/').slice(0, -1).join('/'))}>{t('imageTools.parent')}</Button></Group>{files.isLoading ? <LoadingState /> : files.data?.length ? <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }}>{files.data.map((file) => <Paper key={file.path} className="image-file-card" component={file.kind === 'directory' ? 'button' : 'div'} onClick={() => file.kind === 'directory' && setPath(file.path)}><Group wrap="nowrap"><Box c="dimmed" aria-hidden="true">{file.kind === 'directory' ? <IconFolder size={18} /> : <IconFile size={18} />}</Box><Box className="mapping-copy"><Text size="sm" fw={620} truncate>{file.name}</Text><Text size="xs" c="dimmed">{file.kind} · {formatBytes(file.size)}</Text></Box>{file.kind === 'file' && <ActionIcon component="a" href={`/api/image-tools/jobs/${jobId}/file?path=${encodeURIComponent(file.path)}`} aria-label={t('imageTools.fileDownload')}><IconDownload size={16} /></ActionIcon>}</Group></Paper>)}</SimpleGrid> : <Text c="dimmed" ta="center" py="xl">{t('imageTools.emptyFiles')}</Text>}</Paper>}</Stack>
}

function ImageSourceFields({ form, nodes, credentials }: { form: ReturnType<typeof useForm<ImageJobInput>>; nodes: NodeView[]; credentials: RegistryCredential[] }) {
  const { t } = useTranslation()
  return <><TextInput required label={t('imageTools.source')} {...form.getInputProps('source_ref')} /><SimpleGrid cols={{ base: 1, sm: 3 }}><Select label={t('imageTools.pullVia')} description={t('imageTools.sourceRouteHint')} data={sourceNodeOptions(nodes, t('imageTools.direct'))} value={form.values.source_node_id ?? ''} onChange={(value) => { form.setFieldValue('source_node_id', value || null); if (value) form.setFieldValue('source_credential_id', null) }} /><Select label={t('imageTools.sourceCredential')} data={credentialOptions(credentials, t('imageTools.sourceAuthNone'))} value={form.values.source_credential_id ?? ''} onChange={(value) => { form.setFieldValue('source_credential_id', value || null); if (value) form.setFieldValue('source_node_id', null) }} /><Select label={t('imageTools.platform')} data={[{ value: 'amd64', label: 'linux/amd64' }, { value: 'arm64', label: 'linux/arm64' }, { value: 'arm', label: 'linux/arm' }, { value: '386', label: 'linux/386' }]} value={form.values.platform_arch} onChange={(value) => form.setFieldValue('platform_arch', value ?? 'amd64')} /></SimpleGrid></>
}

function CopyPanel({ nodes, credentials, openCredentials }: { nodes: NodeView[]; credentials: RegistryCredential[]; openCredentials: () => void }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [confirmed, setConfirmed] = useState(false)
  const form = useForm<ImageJobInput>({ mode: 'controlled', initialValues: { ...baseJob, kind: 'copy', destination_ref: 'registry.example.com/team/alpine:latest' } })
  const create = useMutation({ mutationFn: (input: ImageJobInput) => api.createImageJob(input), onSuccess: () => { void client.invalidateQueries({ queryKey: ['image-jobs'] }); notifications.show({ color: 'green', message: t('imageTools.queued') }); setConfirmed(false) }, onError: notifyError(t) })
  if (credentials.length === 0) return <EmptyState title={t('imageTools.credentialRequired')} description={t('imageTools.secretHint')} action={<Button onClick={openCredentials}>{t('imageTools.addCredential')}</Button>} />
  return <Paper className="panel image-tool-flow"><form onSubmit={form.onSubmit((value) => { if (confirmed) create.mutate(value) })}><Stack gap="md"><ImageSourceFields form={form} nodes={nodes} credentials={credentials} /><TextInput required label={t('imageTools.destination')} {...form.getInputProps('destination_ref')} /><Select required label={t('imageTools.destinationCredential')} data={credentialOptions(credentials, t('imageTools.credentialRequired')).slice(1)} value={form.values.destination_credential_id ?? ''} onChange={(value) => form.setFieldValue('destination_credential_id', value || null)} /><Checkbox checked={confirmed} onChange={(event) => setConfirmed(event.currentTarget.checked)} label={t('imageTools.confirmCopy')} /><Group justify="flex-end"><Button type="submit" disabled={!confirmed || !form.values.destination_credential_id} loading={create.isPending} leftSection={<IconCopy size={17} />}>{t('imageTools.startCopy')}</Button></Group></Stack></form></Paper>
}

function SchedulesPanel({ rules, nodes, credentials }: { rules: ImageSyncRule[]; nodes: NodeView[]; credentials: RegistryCredential[] }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [pendingDelete, setPendingDelete] = useState<ImageSyncRule | null>(null)
  const form = useForm({ mode: 'controlled', initialValues: { name: '', enabled: true, source_ref: 'docker.io/library/alpine:latest', source_node_id: null as string | null, source_credential_id: null as string | null, destination_ref: '', destination_credential_id: '', platform_os: 'linux', platform_arch: 'amd64', cron: '0 0 */6 * * *', timezone: 'UTC' } })
  const create = useMutation({ mutationFn: api.createImageRule, onSuccess: () => { void client.invalidateQueries({ queryKey: ['image-rules'] }); notifications.show({ color: 'green', message: t('imageTools.queued') }) }, onError: notifyError(t) })
  const run = useMutation({ mutationFn: api.runImageRule, onSuccess: () => { void client.invalidateQueries({ queryKey: ['image-jobs'] }); notifications.show({ color: 'green', message: t('imageTools.queued') }) }, onError: notifyError(t) })
  const remove = useMutation({ mutationFn: api.deleteImageRule, onSuccess: () => { setPendingDelete(null); void client.invalidateQueries({ queryKey: ['image-rules'] }) }, onError: notifyError(t) })
  return <Stack gap="md"><Paper className="panel"><form onSubmit={form.onSubmit((value) => create.mutate(value))}><Stack><SimpleGrid cols={{ base: 1, sm: 2 }}><TextInput required label={t('imageTools.ruleName')} {...form.getInputProps('name')} /><TextInput required label={t('imageTools.source')} {...form.getInputProps('source_ref')} /><Select label={t('imageTools.pullVia')} description={t('imageTools.sourceRouteHint')} data={sourceNodeOptions(nodes, t('imageTools.direct'))} value={form.values.source_node_id ?? ''} onChange={(value) => { form.setFieldValue('source_node_id', value || null); if (value) form.setFieldValue('source_credential_id', null) }} /><Select label={t('imageTools.sourceCredential')} data={credentialOptions(credentials, t('imageTools.sourceAuthNone'))} value={form.values.source_credential_id ?? ''} onChange={(value) => { form.setFieldValue('source_credential_id', value || null); if (value) form.setFieldValue('source_node_id', null) }} /><TextInput required label={t('imageTools.destination')} {...form.getInputProps('destination_ref')} /><Select required label={t('imageTools.destinationCredential')} data={credentialOptions(credentials, t('imageTools.credentialRequired')).slice(1)} {...form.getInputProps('destination_credential_id')} /><TextInput required label={t('imageTools.cron')} {...form.getInputProps('cron')} /><TextInput required label={t('imageTools.timezone')} {...form.getInputProps('timezone')} /><Switch label={t('common.enabled')} {...form.getInputProps('enabled', { type: 'checkbox' })} /></SimpleGrid><Group justify="flex-end"><Button type="submit" disabled={!form.values.destination_credential_id} loading={create.isPending}>{t('imageTools.createRule')}</Button></Group></Stack></form></Paper>{rules.length === 0 ? <EmptyState title={t('imageTools.noRules')} description={t('imageTools.cron')} /> : rules.map((rule) => <Paper key={rule.id} className="panel"><Group justify="space-between" wrap="nowrap"><Box className="mapping-copy"><Group gap="xs"><Text fw={680}>{rule.name}</Text><Text size="xs" fw={620} c={rule.enabled ? 'green.5' : 'dimmed'}>{rule.enabled ? t('common.enabled') : t('common.disabled')}</Text></Group><Text size="sm" c="dimmed" mt={5}>{rule.source_ref} → {rule.destination_ref}</Text><Text size="xs" c="dimmed" mt={4}>{rule.cron} · {rule.timezone} · {t('imageTools.nextRun')}: {rule.next_run_at ?? '—'}</Text></Box><Group gap={4}><Tooltip label={t('imageTools.runNow')}><ActionIcon aria-label={`${t('imageTools.runNow')} ${rule.name}`} loading={run.isPending && run.variables === rule.id} onClick={() => run.mutate(rule.id)}><IconPlayerPlay size={17} /></ActionIcon></Tooltip><Tooltip label={t('common.delete')}><ActionIcon color="red" variant="subtle" aria-label={`${t('common.delete')} ${rule.name}`} loading={remove.isPending && remove.variables === rule.id} onClick={() => setPendingDelete(rule)}><IconTrash size={17} /></ActionIcon></Tooltip></Group></Group></Paper>)}<NamedConfirmDialog opened={pendingDelete !== null} title={t('imageTools.confirmDeleteRuleTitle')} name={pendingDelete?.name ?? ''} consequence={t('imageTools.confirmDeleteRuleMessage')} loading={remove.isPending} onCancel={() => setPendingDelete(null)} onConfirm={() => pendingDelete && remove.mutate(pendingDelete.id)} /></Stack>
}

function TasksPanel({ jobs, canWrite }: { jobs: ImageJob[]; canWrite: boolean }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const cancel = useMutation({ mutationFn: api.cancelImageJob, onSuccess: () => void client.invalidateQueries({ queryKey: ['image-jobs'] }), onError: notifyError(t) })
  const retry = useMutation({ mutationFn: api.retryImageJob, onSuccess: () => void client.invalidateQueries({ queryKey: ['image-jobs'] }), onError: notifyError(t) })
  if (jobs.length === 0) return <EmptyState title={t('imageTools.noTasks')} description={t('imageTools.description')} />
  return <Stack gap="sm">{jobs.map((job) => <JobCard key={job.id} job={job} cancel={canWrite ? () => cancel.mutate(job.id) : undefined} retry={canWrite ? () => retry.mutate(job.id) : undefined} canceling={cancel.isPending && cancel.variables === job.id} retrying={retry.isPending && retry.variables === job.id} />)}</Stack>
}

function JobCard({ job, cancel, retry, canceling, retrying }: { job: ImageJob; cancel?: () => void; retry?: () => void; canceling: boolean; retrying: boolean }) {
  const { t } = useTranslation()
  const progress = job.total_bytes > 0 ? Math.min(100, job.progress_bytes / job.total_bytes * 100) : 0
  const color = job.status === 'completed' ? 'green.5' : job.status === 'failed' ? 'red.5' : job.status === 'running' ? 'blue.5' : 'dimmed'
  return <Paper className="panel image-job-card" data-status={job.status}><Group justify="space-between" align="flex-start" wrap="nowrap"><Box className="mapping-copy"><Group gap="xs"><Text size="xs" fw={680} c={color}>{job.status}</Text><Text size="xs" c="dimmed">{job.kind}</Text><Text fw={680} truncate>{job.source_ref}</Text></Group>{job.destination_ref && <Text size="sm" c="dimmed" mt={5}>→ {job.destination_ref}</Text>}<Text size="xs" c="dimmed" mt={5}>{t('imageTools.stage')}: {job.stage} · {job.resolved_digest ?? '—'}</Text></Box><Group gap={3}>{job.status === 'completed' && job.artifact_name && <Tooltip label={t('imageTools.download')}><ActionIcon component="a" href={`/api/image-tools/jobs/${job.id}/artifact`} aria-label={`${t('imageTools.download')} ${job.source_ref}`}><IconDownload size={17} /></ActionIcon></Tooltip>}{job.status === 'failed' && retry && <Tooltip label={t('imageTools.retry')}><ActionIcon aria-label={`${t('imageTools.retry')} ${job.source_ref}`} loading={retrying} onClick={retry}><IconRefresh size={17} /></ActionIcon></Tooltip>}{['pending', 'running'].includes(job.status) && cancel && <Tooltip label={t('imageTools.cancel')}><ActionIcon color="red" variant="subtle" aria-label={`${t('imageTools.cancel')} ${job.source_ref}`} loading={canceling} onClick={cancel}><IconX size={17} /></ActionIcon></Tooltip>}</Group></Group>{job.status === 'running' && <Progress value={progress} mt="md" animated aria-label={`${progress.toFixed(0)}%`} />} {job.error && <Text size="sm" c="red.4" mt="sm">{job.error}</Text>}</Paper>
}

function CredentialDialog({ opened, close, credentials }: { opened: boolean; close: () => void; credentials: RegistryCredential[] }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [pendingDelete, setPendingDelete] = useState<RegistryCredential | null>(null)
  const form = useForm({ mode: 'controlled', initialValues: { name: '', registry: '', auth_mode: 'basic', username: '', secret: '' } })
  const create = useMutation({ mutationFn: api.createImageCredential, onSuccess: () => { void client.invalidateQueries({ queryKey: ['image-credentials'] }); notifications.show({ color: 'green', message: t('imageTools.credentialSaved') }); form.reset() }, onError: notifyError(t) })
  const remove = useMutation({ mutationFn: api.deleteImageCredential, onSuccess: () => { setPendingDelete(null); void client.invalidateQueries({ queryKey: ['image-credentials'] }) }, onError: notifyError(t) })
  return <Modal.Stack><Modal stackId="credential-editor" opened={opened} onClose={close} title={t('imageTools.credentials')} size="lg" centered classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }} transitionProps={{ transition: 'pop', duration: 220, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}><Stack><form onSubmit={form.onSubmit((value) => create.mutate({ ...value, username: value.auth_mode === 'basic' ? value.username : null }))}><Stack><SimpleGrid cols={{ base: 1, sm: 2 }}><TextInput required label={t('imageTools.credentialName')} {...form.getInputProps('name')} /><TextInput required label={t('imageTools.registry')} placeholder="registry.cn-hangzhou.aliyuncs.com" {...form.getInputProps('registry')} /><Select label={t('imageTools.authMode')} data={[{ value: 'basic', label: 'Basic' }, { value: 'bearer', label: 'Bearer / Access Token' }]} {...form.getInputProps('auth_mode')} />{form.values.auth_mode === 'basic' && <TextInput required label={t('imageTools.username')} {...form.getInputProps('username')} />}</SimpleGrid><PasswordInput required label={t('imageTools.secret')} description={t('imageTools.secretHint')} autoComplete="new-password" {...form.getInputProps('secret')} /><Group justify="flex-end"><Button type="submit" loading={create.isPending}>{t('imageTools.addCredential')}</Button></Group></Stack></form>{credentials.map((credential) => <Paper key={credential.id} className="credential-row"><Group justify="space-between" wrap="nowrap"><Box className="mapping-copy"><Text fw={650}>{credential.name}</Text><Text size="xs" c="dimmed">{credential.registry} · {credential.auth_mode} · {credential.username ?? 'token'}</Text></Box><ActionIcon color="red" variant="subtle" aria-label={`${t('common.delete')} ${credential.name}`} loading={remove.isPending && remove.variables === credential.id} onClick={() => setPendingDelete(credential)}><IconTrash size={17} /></ActionIcon></Group></Paper>)}</Stack></Modal><NamedConfirmDialog stackId="credential-delete-confirm" opened={pendingDelete !== null} title={t('imageTools.confirmDeleteCredentialTitle')} name={pendingDelete?.name ?? ''} consequence={t('imageTools.confirmDeleteCredentialMessage')} loading={remove.isPending} onCancel={() => setPendingDelete(null)} onConfirm={() => pendingDelete && remove.mutate(pendingDelete.id)} /></Modal.Stack>
}

function notifyError(t: ReturnType<typeof useTranslation>['t']) {
  return (error: Error) => notifications.show({ color: 'red', title: t('imageTools.failed'), message: error.message })
}
