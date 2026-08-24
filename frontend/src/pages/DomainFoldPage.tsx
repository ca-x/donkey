import { useState } from 'react'
import { ActionIcon, Box, Button, CopyButton, Group, Modal, Paper, SimpleGrid, Stack, Switch, Text, TextInput, Tooltip } from '@mantine/core'
import { useForm } from '@mantine/form'
import { notifications } from '@mantine/notifications'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { IconArrowRight, IconCheck, IconCopy, IconExternalLink, IconLink, IconPlus, IconTrash } from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { api } from '../api'
import { PageHeader } from '../components/PageHeader'
import { NamedConfirmDialog } from '../components/NamedConfirmDialog'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import type { Mapping, MappingInput } from '../types'
import { useAuth } from '../useAuth'

export function DomainFoldPage() {
  const { t } = useTranslation()
  const canWrite = useAuth().role === 'admin'
  const [url, setUrl] = useState('')
  const [dialog, setDialog] = useState<{ opened: boolean; value: Mapping | null; revision: number }>({ opened: false, value: null, revision: 0 })
  const mappings = useQuery({ queryKey: ['mappings'], queryFn: api.mappings })
  const convert = useMutation({ mutationFn: api.convert, onError: (error: Error) => notifications.show({ color: 'red', title: t('domain.convertFailed'), message: error.message }) })
  const openDialog = (value: Mapping | null) => setDialog((current) => ({ opened: true, value, revision: current.revision + 1 }))
  const closeDialog = () => setDialog((current) => ({ ...current, opened: false }))
  if (mappings.isLoading) return <LoadingState />
  if (mappings.error) return <ErrorState error={mappings.error} retry={() => void mappings.refetch()} />
  return <Stack gap={24}>
    <PageHeader title={t('domain.title')} description={t('domain.description')} />
    <Paper className="panel convert-panel">
      <Text fw={680}>{t('domain.convertTitle')}</Text>
      <Group align="flex-end" mt="md" gap="sm" wrap="nowrap" className="convert-form">
        <TextInput className="convert-input" label={t('domain.original')} value={url} onChange={(event) => setUrl(event.currentTarget.value)} placeholder="https://github.com/org/repo/releases/download/v1/file.tar.gz" />
        <Button leftSection={<IconArrowRight size={17} />} loading={convert.isPending} disabled={!url} onClick={() => convert.mutate(url)}>{t('domain.convert')}</Button>
      </Group>
      {convert.data && <Box className="conversion-result" mt="md"><Text size="xs" c="dimmed" mb={5}>{t('domain.result')}</Text><Group wrap="nowrap"><Text ff="monospace" size="sm" truncate className="conversion-url">{convert.data.accelerated_url}</Text><CopyButton value={convert.data.accelerated_url}>{({ copied, copy }) => <ActionIcon onClick={copy} color={copied ? 'green' : 'blue'} aria-label={t('common.copy')}>{copied ? <IconCheck size={17} /> : <IconCopy size={17} />}</ActionIcon>}</CopyButton><ActionIcon component="a" href={convert.data.accelerated_url} target="_blank" rel="noreferrer" variant="subtle" aria-label={t('domain.open')}><IconExternalLink size={17} /></ActionIcon></Group></Box>}
    </Paper>
    <Group justify="space-between"><Text fw={680} size="lg">{t('domain.mappings')}</Text>{canWrite && <Button variant="light" leftSection={<IconPlus size={17} />} onClick={() => openDialog(null)}>{t('domain.add')}</Button>}</Group>
    {mappings.data!.length === 0 ? <EmptyState title={t('domain.emptyTitle')} description={t('domain.emptyDesc')} /> : <SimpleGrid cols={{ base: 1, lg: 2 }}>{mappings.data!.map((mapping) => <Paper key={mapping.id} component={canWrite ? 'button' : 'div'} type={canWrite ? 'button' : undefined} aria-label={canWrite ? `${t('common.edit')} ${mapping.source_host}` : undefined} className={`mapping-card${canWrite ? ' pressable' : ''}`} onClick={canWrite ? () => openDialog(mapping) : undefined}><Group wrap="nowrap" align="flex-start"><Box c="dimmed" mt={2} aria-hidden="true"><IconLink size={19} /></Box><Box className="mapping-copy"><Group gap={7}><Text fw={680}>{mapping.source_host}</Text><Text size="xs" c={mapping.enabled ? 'green.5' : 'dimmed'}>{t(mapping.enabled ? 'common.enabled' : 'common.disabled')}</Text></Group><Text size="xs" c="dimmed" mt={6} truncate>{mapping.upstream_base}</Text><Text size="xs" c="blue.5" mt={3} truncate>→ {mapping.public_base}</Text></Box></Group></Paper>)}</SimpleGrid>}
    {canWrite && <MappingDialog key={dialog.revision} opened={dialog.opened} value={dialog.value} close={closeDialog} />}
  </Stack>
}

function MappingDialog({ opened, value, close }: { opened: boolean; value: Mapping | null; close: () => void }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const [confirmDelete, setConfirmDelete] = useState(false)
  const editing = value
  const form = useForm<MappingInput>({ mode: 'controlled', initialValues: editing ? { source_host: editing.source_host, upstream_base: editing.upstream_base, public_base: editing.public_base, enabled: editing.enabled } : { source_host: '', upstream_base: 'https://', public_base: 'https://', enabled: true } })
  const save = useMutation({ mutationFn: (input: MappingInput) => editing ? api.updateMapping(editing.id, input) : api.createMapping(input), onSuccess: () => { void client.invalidateQueries({ queryKey: ['mappings'] }); notifications.show({ color: 'green', message: t(editing ? 'domain.updated' : 'domain.created') }); close() }, onError: (error: Error) => notifications.show({ color: 'red', title: t('domain.saveFailed'), message: error.message }) })
  const remove = useMutation({ mutationFn: () => api.deleteMapping(editing!.id), onSuccess: () => { void client.invalidateQueries({ queryKey: ['mappings'] }); notifications.show({ color: 'green', message: t('domain.deleted') }); setConfirmDelete(false); close() }, onError: (error: Error) => notifications.show({ color: 'red', title: t('domain.deleteFailed'), message: error.message }) })
  return <Modal opened={opened} onClose={close} title={t(editing ? 'common.edit' : 'domain.add')} centered classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }} transitionProps={{ transition: 'pop', duration: 220, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}><form onSubmit={form.onSubmit((input) => save.mutate(input))}><Stack><TextInput required label={t('domain.sourceHost')} placeholder="github.com" {...form.getInputProps('source_host')} /><TextInput required label={t('domain.upstreamBase')} placeholder="https://github.com/" {...form.getInputProps('upstream_base')} /><TextInput required label={t('domain.publicBase')} placeholder="https://gh.example:5443/" {...form.getInputProps('public_base')} /><Switch label={t('common.enabled')} {...form.getInputProps('enabled', { type: 'checkbox' })} /><Group justify="space-between" mt="sm">{editing ? <Tooltip label={t('common.delete')}><ActionIcon color="red" variant="subtle" aria-label={`${t('common.delete')} ${editing.source_host}`} onClick={() => setConfirmDelete(true)}><IconTrash size={18} /></ActionIcon></Tooltip> : <span />}<Group><Button variant="default" onClick={close}>{t('common.cancel')}</Button><Button type="submit" loading={save.isPending}>{t('common.save')}</Button></Group></Group></Stack></form>{editing && <NamedConfirmDialog opened={confirmDelete} title={t('domain.confirmDeleteTitle')} name={editing.source_host} consequence={t('domain.confirmDeleteMessage')} loading={remove.isPending} onCancel={() => setConfirmDelete(false)} onConfirm={() => remove.mutate()} />}</Modal>
}
