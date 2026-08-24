import { useEffect, useState } from 'react'
import { ActionIcon, Box, Button, Group, Modal, Select, SimpleGrid, Stack, Switch, Text, TextInput, Tooltip } from '@mantine/core'
import { useForm } from '@mantine/form'
import { notifications } from '@mantine/notifications'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { IconEdit, IconPlus, IconTrash } from '@tabler/icons-react'
import { useTranslation } from 'react-i18next'
import { ApiError, api } from '../api'
import type { RegistryRoute, RegistryRouteInput } from '../types'
import { NamedConfirmDialog } from './NamedConfirmDialog'

const builtInRouteKeys = new Set(['dockerhub', 'ghcr'])

function isBuiltInRoute(route: RegistryRoute) {
  return builtInRouteKeys.has(route.key)
}

export function RegistryRoutesDialog({ opened, routes, close }: { opened: boolean; routes: RegistryRoute[]; close: () => void }) {
  const { t } = useTranslation()
  const [editorSaving, setEditorSaving] = useState(false)
  const [editor, setEditor] = useState<{ opened: boolean; value: RegistryRoute | null; revision: number }>({ opened: false, value: null, revision: 0 })
  const openEditor = (value: RegistryRoute | null) => {
    if (editorSaving) return
    setEditor((current) => ({ opened: true, value, revision: current.revision + 1 }))
  }
  const closeEditor = () => {
    if (editorSaving) return
    setEditor((current) => ({ ...current, opened: false }))
  }
  const completeEditor = () => {
    setEditorSaving(false)
    setEditor((current) => ({ ...current, opened: false }))
  }
  const closeManager = () => {
    if (!editorSaving) close()
  }

  return (
    <Modal.Stack>
      <Modal stackId="registry-routes" opened={opened} onClose={closeManager} title={t('nodes.routesTitle')} size="lg" centered withCloseButton={!editorSaving} closeOnClickOutside={!editorSaving} closeOnEscape={!editorSaving} classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }} transitionProps={{ transition: 'pop', duration: 200, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}>
        <Stack gap="md">
          <Group justify="space-between" align="flex-start">
            <Text size="sm" c="dimmed" className="route-manager-description">{t('nodes.routesDescription')}</Text>
            <Button size="sm" leftSection={<IconPlus size={17} />} disabled={editorSaving} onClick={() => openEditor(null)}>{t('nodes.addRoute')}</Button>
          </Group>
          <Stack gap={0} className="registry-route-list">
            {routes.map((route) => (
              <Group key={route.id} justify="space-between" wrap="nowrap" className="registry-route-row">
                <Box className="route-copy">
                  <Group gap="xs" wrap="wrap">
                    <Text size="sm" fw={680}>{route.name}</Text>
                    <Text size="xs" fw={620} c={route.enabled ? 'green.5' : 'dimmed'}>{t(route.enabled ? 'common.enabled' : 'common.disabled')}</Text>
                    {route.is_default && <Text size="xs" c="dimmed">{t('nodes.defaultRoute')}</Text>}
                    {isBuiltInRoute(route) && <Text size="xs" c="dimmed">{t('nodes.builtIn')}</Text>}
                  </Group>
                  <Text size="xs" c="dimmed" mt={3}>{route.canonical_registry} · {route.path_prefix ? `/${route.path_prefix}` : t('nodes.rootNamespace')} · {t(`nodes.mode.${route.repository_mode}`)}</Text>
                </Box>
                <Tooltip label={t('common.edit')}><ActionIcon variant="subtle" aria-label={`${t('common.edit')} ${route.name}`} disabled={editorSaving} onClick={() => openEditor(route)}><IconEdit size={18} /></ActionIcon></Tooltip>
              </Group>
            ))}
          </Stack>
        </Stack>
      </Modal>
      <RegistryRouteEditor key={editor.revision} opened={editor.opened} value={editor.value} routes={routes} close={closeEditor} complete={completeEditor} setSaving={setEditorSaving} />
    </Modal.Stack>
  )
}

type RegistryRouteFormValues = Omit<RegistryRouteInput, 'path_prefix'> & { path_prefix: string }

function routeInitialValues(route: RegistryRoute | null): RegistryRouteFormValues {
  return route ? {
    key: route.key,
    name: route.name,
    canonical_registry: route.canonical_registry,
    path_prefix: route.path_prefix ?? '',
    repository_mode: route.repository_mode,
    is_default: route.is_default,
    enabled: route.enabled,
  } : {
    key: '',
    name: '',
    canonical_registry: '',
    path_prefix: '',
    repository_mode: 'passthrough',
    is_default: false,
    enabled: true,
  }
}

function validRouteIdentifier(value: string) {
  return /^[a-z0-9][a-z0-9_-]{0,31}$/.test(normalizeRouteIdentifier(value))
}

function normalizeRouteIdentifier(value: string) {
  return value.trim().toLowerCase()
}

function validCanonicalRegistry(value: string) {
  const authority = value.trim().toLowerCase()
  if (!authority || authority.length > 261 || /[\s/?#@]/.test(authority)) return false
  let port = ''
  if (authority.startsWith('[')) {
    const close = authority.indexOf(']')
    if (close < 2) return false
    const host = authority.slice(0, close + 1)
    if (authority.length > close + 1) {
      if (authority[close + 1] !== ':') return false
      port = authority.slice(close + 2)
    }
    try {
      if (!new URL(`https://${host}`).hostname) return false
    } catch {
      return false
    }
  } else {
    const separator = authority.indexOf(':')
    const host = separator === -1 ? authority : authority.slice(0, separator)
    port = separator === -1 ? '' : authority.slice(separator + 1)
    if (port.includes(':')) return false
    if (!host || host.length > 253 || !host.split('.').every((label) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label))) return false
  }
  return !port || (/^\d{1,5}$/.test(port) && Number(port) > 0 && Number(port) <= 65535)
}

function RegistryRouteEditor({ opened, value, routes, close, complete, setSaving }: { opened: boolean; value: RegistryRoute | null; routes: RegistryRoute[]; close: () => void; complete: () => void; setSaving: (saving: boolean) => void }) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const editing = value
  const builtIn = editing ? isBuiltInRoute(editing) : false
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const form = useForm<RegistryRouteFormValues>({
    mode: 'controlled',
    initialValues: routeInitialValues(editing),
    validate: {
      key: (v) => {
        if (!validRouteIdentifier(v)) return t('nodes.validationRouteKey')
        const normalized = normalizeRouteIdentifier(v)
        return routes.some((route) => route.id !== editing?.id && normalizeRouteIdentifier(route.key) === normalized) ? t('routeFeedback.keyInUse') : null
      },
      name: (v) => v.trim().length > 0 && v.trim().length <= 80 ? null : t('nodes.validationRouteName'),
      canonical_registry: (v) => validCanonicalRegistry(v) ? null : t('nodes.validationCanonicalRegistry'),
      path_prefix: (v, values) => {
        if (values.is_default) return v.trim() ? t('nodes.validationDefaultPrefix') : null
        if (!validRouteIdentifier(v)) return t('nodes.validationPathPrefix')
        const normalized = normalizeRouteIdentifier(v)
        return routes.some((route) => route.id !== editing?.id && route.path_prefix !== null && normalizeRouteIdentifier(route.path_prefix) === normalized) ? t('routeFeedback.prefixInUse') : null
      },
      repository_mode: (v) => ['docker_hub_library', 'passthrough'].includes(v) ? null : t('nodes.validationRepositoryMode'),
      is_default: (v) => v && routes.some((route) => route.is_default && route.id !== editing?.id) ? t('nodes.validationDefaultConflict') : null,
    },
  })

  const save = useMutation({
    mutationFn: (values: RegistryRouteFormValues) => {
      const input: RegistryRouteInput = {
        ...values,
        key: normalizeRouteIdentifier(values.key),
        name: values.name.trim(),
        canonical_registry: values.canonical_registry.trim().toLowerCase(),
        path_prefix: normalizeRouteIdentifier(values.path_prefix) || null,
      }
      return editing ? api.updateRegistryRoute(editing.id, input) : api.createRegistryRoute(input)
    },
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['registry-routes'] })
      void client.invalidateQueries({ queryKey: ['nodes'] })
      void client.invalidateQueries({ queryKey: ['dashboard'] })
      notifications.show({ color: 'green', title: t(editing ? 'nodes.routeUpdated' : 'nodes.routeCreated'), message: t('nodes.routeSavedMessage') })
      complete()
    },
    onError: (error: Error) => {
      const message = error instanceof ApiError && error.status === 409 ? t('routeFeedback.conflict') : error.message
      notifications.show({ color: 'red', title: t('nodes.routeSaveFailed'), message })
    },
  })
  useEffect(() => setSaving(save.isPending), [save.isPending, setSaving])
  const remove = useMutation({
    mutationFn: () => api.deleteRegistryRoute(editing!.id),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['registry-routes'] })
      void client.invalidateQueries({ queryKey: ['nodes'] })
      setConfirmDelete(false)
      notifications.show({ color: 'green', title: t('nodes.routeDeleted'), message: editing!.name })
      close()
    },
    onError: (error: Error) => {
      const message = error instanceof ApiError && error.status === 409 ? t('nodes.routeInUse') : error.message
      setDeleteError(message)
      notifications.show({ color: 'red', title: t('nodes.routeDeleteFailed'), message })
    },
  })

  const openDelete = () => {
    setDeleteError(null)
    setConfirmDelete(true)
  }
  const requestClose = () => {
    if (!save.isPending) close()
  }

  return (
    <>
      <Modal stackId="registry-route-editor" opened={opened} onClose={requestClose} title={t(editing ? 'nodes.editRouteTitle' : 'nodes.createRouteTitle')} size="md" centered withCloseButton={!save.isPending} closeOnClickOutside={!save.isPending} closeOnEscape={!save.isPending} classNames={{ content: 'polished-modal', overlay: 'polished-overlay' }} transitionProps={{ transition: 'pop', duration: 200, timingFunction: 'cubic-bezier(0.23, 1, 0.32, 1)' }}>
        <form aria-busy={save.isPending} onSubmit={form.onSubmit((values) => save.mutate(values))}>
          <fieldset className="pending-form" disabled={save.isPending}>
            <Stack gap="md">
            <SimpleGrid cols={{ base: 1, sm: 2 }}>
              <TextInput label={t('nodes.routeKey')} description={t('nodes.routeKeyDesc')} readOnly={builtIn} required {...form.getInputProps('key')} />
              <TextInput label={t('nodes.routeName')} required {...form.getInputProps('name')} />
            </SimpleGrid>
            <TextInput label={t('nodes.canonicalRegistry')} description={t('nodes.canonicalRegistryDesc')} placeholder="registry.example.com:5000" required {...form.getInputProps('canonical_registry')} />
            <TextInput label={t('nodes.pathPrefix')} description={form.values.is_default ? t('nodes.defaultPrefixDesc') : t('nodes.pathPrefixDesc')} placeholder="team" required={!form.values.is_default} {...form.getInputProps('path_prefix')} />
            <Select label={t('nodes.repositoryMode')} description={t('nodes.repositoryModeDesc')} data={[{ value: 'passthrough', label: t('nodes.mode.passthrough') }, { value: 'docker_hub_library', label: t('nodes.mode.docker_hub_library') }]} required {...form.getInputProps('repository_mode')} />
            <Group gap="xl">
              <Switch label={t('nodes.defaultRoute')} description={t('nodes.defaultRouteDesc')} {...form.getInputProps('is_default', { type: 'checkbox' })} />
              <Switch label={t('nodes.enableRoute')} {...form.getInputProps('enabled', { type: 'checkbox' })} />
            </Group>
            <Group justify="space-between" mt="sm">
              {editing && !builtIn ? <Button type="button" color="red" variant="subtle" leftSection={<IconTrash size={17} />} onClick={openDelete}>{t('common.delete')}</Button> : <span />}
              <Group>
                <Button type="button" variant="default" disabled={save.isPending} onClick={requestClose}>{t('common.cancel')}</Button>
                <Button type="submit" loading={save.isPending}>{t('common.save')}</Button>
              </Group>
            </Group>
            </Stack>
          </fieldset>
        </form>
      </Modal>
      {editing && !builtIn ? <NamedConfirmDialog
        stackId="registry-route-delete-confirm"
        opened={confirmDelete}
        title={t('nodes.confirmDeleteRouteTitle')}
        name={editing.name}
        consequence={t('nodes.confirmDeleteRouteMessage')}
        loading={remove.isPending}
        error={deleteError}
        onCancel={() => {
          setDeleteError(null)
          setConfirmDelete(false)
        }}
        onConfirm={() => remove.mutate()}
      /> : null}
    </>
  )
}
