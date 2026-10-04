import { asRecord, asString } from './DiscordSourceCapture'
import { type DiscordInteraction, type Env, validSnowflake } from './DiscordSourceProtocol'
import {
  authenticatedEndpoint,
  InboxError,
  inboxErrorResponse,
  interactionUserId,
  requestBody,
} from './DiscordInbox'
import { CORS_HEADERS, json, sha256Hex } from './DiscordWorkerHttp'

const TTL = 7 * 24 * 60 * 60 * 1_000
const PAGE = 20
const MAX_SIZE = 4 * 1024 ** 3
const STATES = [
  'queued',
  'downloading',
  'importing',
  'waiting_version',
  'imported',
  'failed',
  'cancelled',
] as const
interface Attachment {
  url: string
  name: string
  size: number
  identity: string
}
interface ResourceJob {
  id: string
  library_id: string
  channel_id: string
  message_id: string
  url: string
  name: string
  size: number
  state: (typeof STATES)[number]
  error: string | null
  created_at: number
  updated_at: number
  expires_at: number
}

/** Exact HTTPS allowlist: no user-controlled proxy target, login cookies, or redirects. */
function attachment(value: unknown, size = 0): Attachment | undefined {
  if (typeof value !== 'string' || value.length > 4_096) return undefined
  try {
    const url = new URL(value.replace(/[),.;!?\]}>。，；！？]+$/u, ''))
    if (
      url.protocol !== 'https:' ||
      url.port ||
      url.username ||
      url.password ||
      !['cdn.discordapp.com', 'media.discordapp.net'].includes(url.hostname) ||
      !/^\/attachments\/\d+\/\d+\/[^/]+$/u.test(url.pathname)
    )
      return undefined
    const name = decodeURIComponent(url.pathname.split('/').at(-1)!)
      .replace(/[\\/:*?"<>|\p{Cc}]/gu, '_')
      .slice(0, 240)
    if (!/\.(png|json|zip|txt|srlchat|webp|jpe?g)$/iu.test(name)) return undefined
    url.hash = ''
    const identity = new URL(url)
    for (const key of ['ex', 'is', 'hm']) identity.searchParams.delete(key)
    // The same immutable Discord attachment has the same identity on both CDN hosts.
    identity.hostname = 'cdn.discordapp.com'
    return {
      url: url.toString(),
      name,
      size: Number.isSafeInteger(size) && size > 0 ? size : 0,
      identity: identity.toString(),
    }
  } catch {
    return undefined
  }
}

function messageAttachments(message: Record<string, unknown>): Attachment[] {
  const found = new Map<string, Attachment>()
  for (const item of Array.isArray(message.attachments) ? message.attachments : []) {
    const object = asRecord(item)
    const file = attachment(object?.url, typeof object?.size === 'number' ? object.size : 0)
    if (file) found.set(file.identity, file)
  }
  for (const match of asString(message.content).matchAll(/https:\/\/[^\s<>"']+/giu)) {
    const file = attachment(match[0])
    if (file && !found.has(file.identity)) found.set(file.identity, file)
  }
  return [...found.values()]
}

export function extractPastedResourceLinks(content: string): string[] {
  return messageAttachments({ content }).map((file) => file.url)
}

export async function createResourceJobs(
  interaction: DiscordInteraction,
  env: Env,
): Promise<string> {
  const userId = interactionUserId(interaction)
  const messageId = interaction.data?.target_id
  const message = messageId ? interaction.data?.resolved?.messages?.[messageId] : undefined
  const channelId = asString(message?.channel_id) || interaction.channel_id
  const direct = interaction.data?.type === 1 && interaction.data.name === '下载直链'
  if (
    !userId ||
    (!direct && (!message || !validSnowflake(messageId) || !validSnowflake(channelId)))
  )
    throw new InboxError(400, 'Discord 未提供完整的目标消息')
  const link = interaction.data?.options?.find(
    (option) => option.name === '链接' && option.type === 3,
  )?.value
  const file = direct && typeof link === 'string' ? attachment(link.trim()) : undefined
  const files = direct ? (file ? [file] : []) : messageAttachments(message!)
  if (!files.length)
    throw new InboxError(
      400,
      '没有找到可导入的 Discord 附件直链。支持 PNG、JSON、ZIP、TXT、聊天文件和图片；保存正文请用“保存帖子到SRL（云端暂存）”。',
    )
  if (files.length > 20 || files.some((file) => file.size > MAX_SIZE))
    throw new InboxError(400, '一次最多下载 20 个文件，单文件最多 4 GiB')
  const result = await queueResourceFiles(
    interaction,
    env,
    files,
    direct ? '' : channelId!,
    direct ? '' : messageId!,
  )
  return result.message
}

export async function queuePastedResourceLinks(
  interaction: DiscordInteraction,
  env: Env,
  content: string,
): Promise<{ count: number; message: string }> {
  const files = extractPastedResourceLinks(content).flatMap((url) => {
    const file = attachment(url)
    return file ? [file] : []
  })
  if (!files.length) return { count: 0, message: '' }
  if (files.length > 20 || files.some((file) => file.size > MAX_SIZE))
    throw new InboxError(400, '正文中的附件超过限制：一次最多 20 个文件，单文件最多 4 GiB')
  return queueResourceFiles(interaction, env, files, '', '')
}

async function queueResourceFiles(
  interaction: DiscordInteraction,
  env: Env,
  files: Attachment[],
  channelId: string,
  messageId: string,
): Promise<{ count: number; message: string }> {
  const userId = interactionUserId(interaction)
  if (!userId) throw new InboxError(400, '无法确认执行命令的 Discord 用户')
  const target = await env.DB.prepare(
    'SELECT library_id, name FROM inbox_endpoints WHERE discord_user_id = ? AND is_default = 1 AND revoked_at IS NULL',
  )
    .bind(userId)
    .first<{ library_id: string; name: string }>()
  if (!target)
    throw new InboxError(
      409,
      '先在资源库生成配对码并执行 /绑定资源库，再下载资源。原来的 APK 直接分享和网页粘贴仍可使用。',
    )
  const now = Date.now()
  const fingerprints: string[] = []
  const statements: D1PreparedStatement[] = []
  for (const file of files) {
    const fingerprint = await sha256Hex(file.identity)
    fingerprints.push(fingerprint)
    statements.push(
      env.DB.prepare(
        `INSERT INTO inbox_resources
       (id, library_id, fingerprint, channel_id, message_id, url, name, size, created_at, updated_at, expires_at)
       SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
       WHERE EXISTS (SELECT 1 FROM inbox_endpoints WHERE library_id = ? AND discord_user_id = ? AND is_default = 1 AND revoked_at IS NULL)
       ON CONFLICT(library_id, fingerprint) DO UPDATE SET
         url = CASE WHEN inbox_resources.state = 'imported' AND inbox_resources.expires_at > excluded.created_at THEN '' ELSE excluded.url END,
         channel_id = CASE WHEN inbox_resources.state = 'imported' AND inbox_resources.expires_at > excluded.created_at THEN '' WHEN excluded.message_id = '' THEN inbox_resources.channel_id ELSE excluded.channel_id END,
         message_id = CASE WHEN inbox_resources.state = 'imported' AND inbox_resources.expires_at > excluded.created_at THEN '' WHEN excluded.message_id = '' THEN inbox_resources.message_id ELSE excluded.message_id END,
         state = CASE WHEN inbox_resources.state IN ('failed','cancelled') OR inbox_resources.expires_at <= excluded.created_at THEN 'queued' ELSE inbox_resources.state END,
         error = CASE WHEN inbox_resources.state IN ('failed','cancelled') THEN NULL ELSE inbox_resources.error END,
         updated_at = excluded.updated_at, expires_at = excluded.expires_at`,
      ).bind(
        crypto.randomUUID(),
        target.library_id,
        fingerprint,
        channelId,
        messageId,
        file.url,
        file.name,
        file.size,
        now,
        now,
        now + TTL,
        target.library_id,
        userId,
      ),
    )
  }
  const result = await env.DB.batch(statements)
  if (result.some((item) => !item.meta.changes))
    throw new InboxError(409, '配对目标已改变，请重新执行命令')
  const saved = await env.DB.prepare(
    `SELECT state FROM inbox_resources WHERE library_id = ? AND fingerprint IN (${fingerprints.map(() => '?').join(',')})`,
  )
    .bind(target.library_id, ...fingerprints)
    .all<{ state: string }>()
  return {
    count: files.length,
    message: saved.results.every((row) => row.state === 'imported')
      ? `这 ${files.length} 个文件已导入「${target.name}」，没有重复创建资源。`
      : `已将 ${files.length} 个文件加入「${target.name}」的资源下载队列。回到资源库后自动领取，进度在“资源下载”中查看；云端任务保留 7 天。`,
  }
}

function summary(job: ResourceJob) {
  return {
    id: job.id,
    libraryId: job.library_id,
    name: job.name,
    size: job.size,
    state: job.state,
    error: job.error,
    createdAt: job.created_at,
    updatedAt: job.updated_at,
    expiresAt: job.expires_at,
  }
}

async function freshAttachment(job: ResourceJob, env: Env): Promise<Attachment> {
  const original = attachment(job.url, job.size)
  if (!original) throw new InboxError(422, '附件直链无效，请重新发送')
  const expiry = new URL(job.url).searchParams.get('ex')
  if (
    !expiry ||
    (/^[a-f\d]+$/iu.test(expiry) && Number.parseInt(expiry, 16) * 1_000 > Date.now() + 60_000)
  )
    return original
  if (!validSnowflake(job.channel_id) || !validSnowflake(job.message_id))
    throw new InboxError(
      410,
      '直链已过期，请重新复制有效下载链接并执行 /下载直链 或 /粘贴收件 重试。',
    )
  // Only reread the explicitly selected message; never scan other comments.
  const response = await fetch(
    `https://discord.com/api/v10/channels/${job.channel_id}/messages/${job.message_id}`,
    {
      headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
      redirect: 'manual',
    },
  )
  if (!response.ok) {
    await response.body?.cancel()
    throw new InboxError(
      410,
      '附件直链已过期，Bot 无法刷新。请回 Discord 对原消息重新执行“下载资源到SRL（云端暂存）”。',
    )
  }
  const message = asRecord(await response.json())
  const fresh =
    message && messageAttachments(message).find((file) => file.identity === original.identity)
  if (!fresh) throw new InboxError(410, '原消息中没有这份附件，请重新发送资源')
  const renewedExpiry = new URL(fresh.url).searchParams.get('ex')
  if (
    renewedExpiry &&
    (!/^[a-f\d]+$/iu.test(renewedExpiry) ||
      Number.parseInt(renewedExpiry, 16) * 1_000 <= Date.now())
  )
    throw new InboxError(410, '原消息中的直链也已过期，请重新分享有效附件链接')
  return fresh
}

export async function handleResourceRequest(request: Request, env: Env): Promise<Response> {
  let stage = 'authenticate'
  try {
    const target = await authenticatedEndpoint(request, env)
    stage = 'read_task'
    const path = new URL(request.url).pathname
    const now = Date.now()
    if (path === '/inbox/resources' && request.method === 'GET') {
      const after = new URL(request.url).searchParams.get('after')
      const cursor = after === null ? undefined : /^(\d{1,13}):([a-f\d-]{36})$/u.exec(after)
      if (after !== null && !cursor) throw new InboxError(400, 'invalid_resource_cursor')
      const rows = await env.DB.prepare(
        `SELECT * FROM inbox_resources WHERE library_id = ? AND expires_at > ?
         AND (created_at > ? OR (created_at = ? AND id > ?))
         AND state NOT IN ('imported','failed','cancelled') ORDER BY created_at, id LIMIT ?`,
      )
        .bind(
          target.library_id,
          now,
          Number(cursor?.[1] ?? 0),
          Number(cursor?.[1] ?? 0),
          cursor?.[2] ?? '',
          PAGE + 1,
        )
        .all<ResourceJob>()
      const recent = await env.DB.prepare(
        `SELECT * FROM inbox_resources WHERE library_id = ? AND expires_at > ?
         AND state IN ('imported','failed','cancelled','waiting_version') ORDER BY updated_at DESC, id LIMIT ?`,
      )
        .bind(target.library_id, now, PAGE)
        .all<ResourceJob>()
      return json({
        jobs: rows.results.slice(0, PAGE).map(summary),
        recent: recent.results.map(summary),
        hasMore: rows.results.length > PAGE,
      })
    }
    const match = /^\/inbox\/resources\/([a-f\d-]{36})(?:\/(ack|file))?$/u.exec(path)
    if (!match) return json({ error: 'not_found' }, { status: 404 })
    const job = await env.DB.prepare(
      'SELECT * FROM inbox_resources WHERE id = ? AND library_id = ? AND expires_at > ?',
    )
      .bind(match[1], target.library_id, now)
      .first<ResourceJob>()
    if (!job) throw new InboxError(404, 'resource_task_not_found_or_expired')
    if (request.method === 'POST' && match[2] === 'ack') {
      stage = 'acknowledge'
      const body = await requestBody(request)
      if (!(STATES as readonly unknown[]).includes(body.state))
        throw new InboxError(400, 'invalid_state')
      const error = typeof body.error === 'string' ? body.error.slice(0, 300) : null
      // A stale download/error callback cannot turn an already committed import into failure.
      await env.DB.prepare(
        `UPDATE inbox_resources SET
          state = CASE WHEN state = 'imported' THEN state ELSE ? END,
          error = CASE WHEN state = 'imported' OR ? = 'imported' THEN NULL ELSE ? END,
          url = CASE WHEN state = 'imported' OR ? = 'imported' THEN '' ELSE url END,
          channel_id = CASE WHEN state = 'imported' OR ? = 'imported' THEN '' ELSE channel_id END,
          message_id = CASE WHEN state = 'imported' OR ? = 'imported' THEN '' ELSE message_id END,
          updated_at = ?
        WHERE id = ? AND library_id = ? AND expires_at > ?`,
      )
        .bind(
          body.state,
          body.state,
          error,
          body.state,
          body.state,
          body.state,
          now,
          job.id,
          target.library_id,
          now,
        )
        .run()
      return json({ ok: true })
    }
    if (request.method !== 'GET') return json({ error: 'method_not_allowed' }, { status: 405 })
    if (job.state === 'imported')
      return json({ error: 'resource_already_imported', state: job.state }, { status: 409 })
    stage = 'resolve_attachment'
    const file = await freshAttachment(job, env)
    if (!match[2]) return json({ ...summary(job), url: file.url })
    stage = 'download_attachment'
    const response = await fetch(file.url, {
      redirect: 'manual',
      headers: { 'Accept-Encoding': 'identity' },
    })
    if (!response.ok || !response.body) {
      await response.body?.cancel()
      throw new InboxError(502, '附件下载失败，请重试或重新发送直链')
    }
    if (response.headers.get('content-type')?.toLowerCase().startsWith('text/html')) {
      await response.body.cancel()
      throw new InboxError(422, '链接返回网页，未作为资源导入')
    }
    const declared = response.headers.get('content-length')
    if (declared && Number(declared) > MAX_SIZE) {
      await response.body.cancel()
      throw new InboxError(413, '附件超过 4 GiB')
    }
    const headers = new Headers(CORS_HEADERS)
    headers.set('Cache-Control', 'no-store')
    headers.set('Content-Type', response.headers.get('content-type') || 'application/octet-stream')
    if (declared) headers.set('Content-Length', declared)
    // Pass the CDN stream through without materializing binary contents in D1 or Worker memory.
    return new Response(response.body, { headers })
  } catch (error) {
    if (!(error instanceof InboxError)) {
      let message = error instanceof Error ? error.message : 'Unknown error'
      const credentials = [
        env.DISCORD_BOT_TOKEN,
        request.headers.get('Authorization')?.replace(/^Bearer\s+/iu, ''),
      ]
      for (const credential of credentials)
        if (credential) message = message.replaceAll(credential, '[credential]')
      message = message
        .replace(/https?:\/\/[^\s"'<>]+/giu, '[url]')
        .replace(/[\w.+-]+@[\w.-]+\.[a-z]{2,}/giu, '[email]')
        .slice(0, 240)
      console.error('Discord resource request failed', {
        stage,
        name: error instanceof Error ? error.name : 'UnknownError',
        message,
      })
    }
    return inboxErrorResponse(error)
  }
}

export async function cleanupResources(env: Env, now: number): Promise<void> {
  await env.DB.prepare('DELETE FROM inbox_resources WHERE expires_at <= ?').bind(now).run()
  await env.DB.prepare(
    "UPDATE inbox_resources SET url = '', channel_id = '', message_id = '', error = NULL WHERE state = 'imported' AND (url <> '' OR channel_id <> '' OR message_id <> '' OR error IS NOT NULL)",
  ).run()
}
