interface Env {
  DB: D1Database
  DISCORD_APPLICATION_ID: string
  DISCORD_PUBLIC_KEY: string
  DISCORD_BOT_TOKEN: string
  HANDOFF_TTL_SECONDS?: string
}

interface DiscordInteraction {
  type: number
  token?: string
  guild_id?: string
  channel_id?: string
  channel?: Record<string, unknown>
  data?: {
    type?: number
    target_id?: string
    resolved?: {
      messages?: Record<string, Record<string, unknown>>
    }
  }
}

type DiscordSourceRemoteScanCursor =
  | {
      lastSeenMessageId: string
      pendingBeforeMessageId?: never
      pendingHighWaterMessageId?: never
    }
  | {
      lastSeenMessageId: string
      pendingBeforeMessageId: string
      pendingHighWaterMessageId: string
    }

interface DiscordSourceReadRequest {
  guildId?: string
  channelId: string
  threadId?: string
  starterMessageId?: string
  savedMessageIds: string[]
  scanMessageIds: string[]
  scanCursor?: DiscordSourceRemoteScanCursor
}

interface DiscordSavedMessageCheckRequest {
  guildId?: string
  channelId: string
  threadId?: string
  starterMessageId?: string
  messageIds: string[]
}

interface DiscordCaptureContext {
  guildId?: string
  guildName?: string
  channelId: string
  channelName?: string
  threadId?: string
  starterMessageId: string
  title?: string
  forumTags: string[]
}

interface ThreadParentMetadata {
  channelName?: string
  forumTags: string[]
}

interface DiscordGuildMetadata {
  name?: string
  access: 'available' | 'unavailable' | 'unknown'
}

type DiscordSourceReadFailure =
  | { state: 'unavailable'; reason: 'not_found'; stage: 'channel' | 'starter' }
  | {
      state: 'uncheckable'
      reason: 'bot_access' | 'forbidden' | 'read_failed'
      stage: 'channel' | 'starter' | 'messages' | 'saved_messages'
    }

const COMMAND_NAME = '保存到资源库'
const INLINE_HANDOFF_MAX_BYTES = 1_800_000
const HANDOFF_CHUNK_CHARACTERS = 250_000
const HANDOFF_CHUNK_BATCH_SIZE = 20
const HANDOFF_CHUNK_MARKER = /^srl-chunks-v1:(\d+)$/u
const THREAD_CHANNEL_TYPES = new Set([10, 11, 12])
const DISCORD_SNOWFLAKE_PATTERN = /^\d{5,32}$/u
const MAX_THREAD_PAGES = 3
const MAX_SAVED_MESSAGE_IDS = 24
const MAX_HEALTH_MESSAGE_IDS = 12
const HEALTH_CHECK_CONCURRENCY = 2
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Authorization, Content-Type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
}

class DiscordRateLimitError extends Error {
  readonly retryAfterMs?: number

  constructor(operation: string, retryAfterMs?: number) {
    super(`${operation} rate limited`)
    this.name = 'DiscordRateLimitError'
    this.retryAfterMs = retryAfterMs
  }
}

function json(value: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers)
  headers.set('Content-Type', 'application/json; charset=utf-8')
  for (const [key, item] of Object.entries(CORS_HEADERS)) headers.set(key, item)
  return new Response(JSON.stringify(value), { ...init, headers })
}

function html(value: string, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers)
  headers.set('Content-Type', 'text/html; charset=utf-8')
  return new Response(value, { ...init, headers })
}

function escapeHtml(value: string): string {
  return value.replace(
    /[&<>"']/gu,
    (character) =>
      ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character] ??
      character,
  )
}

function hexToBytes(value: string): Uint8Array<ArrayBuffer> {
  if (!/^[0-9a-f]+$/iu.test(value) || value.length % 2 !== 0) throw new Error('invalid hex')
  const bytes = new Uint8Array(value.length / 2)
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16)
  }
  return bytes
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, '-').replace(/\//gu, '_').replace(/=+$/gu, '')
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))
  return bytesToHex(new Uint8Array(digest))
}

async function verifyDiscordRequest(request: Request, rawBody: string, publicKeyHex: string) {
  const signature = request.headers.get('X-Signature-Ed25519')
  const timestamp = request.headers.get('X-Signature-Timestamp')
  if (!signature || !timestamp) return false
  try {
    const key = await crypto.subtle.importKey(
      'raw',
      hexToBytes(publicKeyHex.trim()),
      { name: 'Ed25519' },
      false,
      ['verify'],
    )
    const message = new TextEncoder().encode(`${timestamp}${rawBody}`)
    return crypto.subtle.verify('Ed25519', key, hexToBytes(signature), message)
  } catch {
    return false
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function asString(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function asNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function readForumTags(channel?: Record<string, unknown>): string[] {
  if (!channel) return []
  const applied = Array.isArray(channel.applied_tags)
    ? channel.applied_tags.filter((value): value is string => typeof value === 'string')
    : []
  const available = Array.isArray(channel.available_tags)
    ? channel.available_tags.flatMap((value) => {
        const tag = asRecord(value)
        const id = asString(tag?.id)
        const name = asString(tag?.name)
        return id && name ? [{ id, name }] : []
      })
    : []
  const names = new Map(available.map((tag) => [tag.id, tag.name]))
  return applied.flatMap((id) => {
    const name = names.get(id)
    return name ? [name] : []
  })
}

function normalizeAttachments(value: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) return []
  return value.flatMap((item) => {
    const attachment = asRecord(item)
    if (!attachment) return []
    const id = asString(attachment.id)
    const name = asString(attachment.filename)
    const url = asString(attachment.url)
    if (!id || !name || !url) return []
    return [
      {
        id,
        name,
        size: asNumber(attachment.size) ?? 0,
        url,
        ...(asString(attachment.proxy_url) ? { proxyUrl: asString(attachment.proxy_url) } : {}),
        ...(asString(attachment.content_type)
          ? { contentType: asString(attachment.content_type) }
          : {}),
        ...(asNumber(attachment.width) !== undefined ? { width: asNumber(attachment.width) } : {}),
        ...(asNumber(attachment.height) !== undefined
          ? { height: asNumber(attachment.height) }
          : {}),
      },
    ]
  })
}

const MAX_TEXT_ATTACHMENT_BYTES = 1_000_000
const MAX_TEXT_ATTACHMENTS_PER_CAPTURE = 2
const MAX_TEXT_ATTACHMENT_BYTES_PER_READ = 2_000_000

function hasTextAttachments(capture: Record<string, unknown>): boolean {
  return Array.isArray(capture.attachments) && capture.attachments.some((item) => {
    const attachment = asRecord(item)
    return asString(attachment?.name).toLowerCase().endsWith('.txt')
  })
}

function textAttachmentUrl(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  try {
    const url = new URL(value)
    return url.protocol === 'https:' &&
      url.hostname === 'cdn.discordapp.com' &&
      !url.port &&
      !url.username &&
      !url.password
      ? url.toString()
      : undefined
  } catch {
    return undefined
  }
}

async function readTextAttachment(
  url: string,
  declaredSize: number,
  maxBytes: number,
): Promise<string | undefined> {
  if (declaredSize > maxBytes) return undefined
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 5_000)
  try {
    const response = await fetch(url, {
      method: 'GET',
      cache: 'no-store',
      redirect: 'error',
      signal: controller.signal,
    })
    if (!response.ok || !response.body) return undefined
    const contentLength = Number(response.headers.get('content-length'))
    if (Number.isFinite(contentLength) && contentLength > maxBytes) {
      await response.body.cancel()
      return undefined
    }
    const reader = response.body.getReader()
    const chunks: Uint8Array[] = []
    let totalBytes = 0
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      totalBytes += value.byteLength
      if (totalBytes > maxBytes) {
        await reader.cancel()
        return undefined
      }
      chunks.push(value)
    }
    const bytes = new Uint8Array(totalBytes)
    let offset = 0
    for (const chunk of chunks) {
      bytes.set(chunk, offset)
      offset += chunk.byteLength
    }
    let text = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    if (text.charCodeAt(0) === 0xfeff) text = text.slice(1)
    return text.includes('\0') ? undefined : text
  } catch {
    return undefined
  } finally {
    clearTimeout(timeout)
  }
}

async function addTextAttachmentContent(
  capture: Record<string, unknown>,
  budget: { remainingBytes: number } = { remainingBytes: MAX_TEXT_ATTACHMENT_BYTES_PER_READ },
): Promise<Record<string, unknown>> {
  const attachments = Array.isArray(capture.attachments)
    ? capture.attachments.map((value) => asRecord(value) ?? {})
    : []
  let extractedCount = 0
  const enriched: Array<Record<string, unknown>> = []
  for (const attachment of attachments) {
    const name = asString(attachment.name)
    const size = asNumber(attachment.size) ?? 0
    const url = name.toLowerCase().endsWith('.txt') ? textAttachmentUrl(attachment.url) : undefined
    const maxBytes = Math.min(MAX_TEXT_ATTACHMENT_BYTES, budget.remainingBytes)
    if (url && maxBytes > 0 && extractedCount < MAX_TEXT_ATTACHMENTS_PER_CAPTURE) {
      const textContent = await readTextAttachment(url, size, maxBytes)
      if (textContent !== undefined) {
        budget.remainingBytes -= new TextEncoder().encode(textContent).byteLength
        enriched.push({ ...attachment, textContent })
        extractedCount += 1
        continue
      }
    }
    enriched.push(attachment)
  }
  return { ...capture, attachments: enriched }
}

function buildCapture(interaction: DiscordInteraction): Record<string, unknown> {
  const channelId = interaction.channel_id ?? ''
  const targetId = interaction.data?.target_id ?? ''
  const message = interaction.data?.resolved?.messages?.[targetId]
  if (!channelId || !targetId || !message) throw new Error('Discord 没有提供被选中的消息')

  const author = asRecord(message.author)
  const authorId = asString(author?.id)
  const authorName =
    asString(author?.global_name) || asString(author?.username) || asString(message.author_name)
  if (!authorId || !authorName) throw new Error('无法读取消息作者')
  const authorBot = author?.bot === true || Boolean(asString(message.webhook_id))

  const channel = interaction.channel
  const channelType = asNumber(channel?.type)
  const threadId =
    channelType !== undefined && THREAD_CHANNEL_TYPES.has(channelType) ? channelId : undefined
  const starterMessageId = threadId ?? targetId
  const isStarter = targetId === starterMessageId
  const guildPath = interaction.guild_id ?? '@me'
  const canonicalUrl = `https://discord.com/channels/${encodeURIComponent(guildPath)}/${encodeURIComponent(channelId)}/${encodeURIComponent(targetId)}`
  const currentChannelName = asString(channel?.name) || undefined

  return {
    ...(interaction.guild_id ? { guildId: interaction.guild_id } : {}),
    channelId,
    ...(!threadId && currentChannelName ? { channelName: currentChannelName } : {}),
    ...(threadId ? { threadId } : {}),
    starterMessageId,
    isStarter,
    messageId: targetId,
    canonicalUrl,
    authorId,
    authorName,
    authorBot,
    content: typeof message.content === 'string' ? message.content : '',
    timestamp: asString(message.timestamp) || new Date().toISOString(),
    ...(asString(message.edited_timestamp)
      ? { editedTimestamp: asString(message.edited_timestamp) }
      : {}),
    ...(threadId && currentChannelName ? { title: currentChannelName } : {}),
    forumTags: readForumTags(channel),
    embeds: Array.isArray(message.embeds)
      ? message.embeds.filter((value) => Boolean(asRecord(value)))
      : [],
    attachments: normalizeAttachments(message.attachments),
  }
}

function handoffTtlSeconds(env: Env): number {
  const configured = Number(env.HANDOFF_TTL_SECONDS)
  return Number.isFinite(configured)
    ? Math.min(1_800, Math.max(300, Math.round(configured)))
    : 1_200
}

async function cleanupExpired(env: Env): Promise<void> {
  const now = Date.now()
  await env.DB.batch([
    env.DB.prepare(
      `DELETE FROM handoffs
       WHERE token_hash LIKE '%:chunk:%'
         AND (
           expires_at <= ? OR
           substr(token_hash, 1, instr(token_hash, ':chunk:') - 1) IN (
             SELECT token_hash FROM handoffs
             WHERE token_hash NOT LIKE '%:chunk:%'
               AND (expires_at <= ? OR (consumed_at IS NOT NULL AND consumed_at <= ?))
           )
         )`,
    ).bind(now, now, now - 60_000),
    env.DB.prepare(
      'DELETE FROM handoffs WHERE expires_at <= ? OR (consumed_at IS NOT NULL AND consumed_at <= ?)',
    ).bind(now, now - 60_000),
  ])
}

function splitHandoffPayload(payload: string): string[] {
  const chunks: string[] = []
  for (let start = 0; start < payload.length; ) {
    let end = Math.min(start + HANDOFF_CHUNK_CHARACTERS, payload.length)
    if (end < payload.length) {
      const last = payload.charCodeAt(end - 1)
      if (last >= 0xd800 && last <= 0xdbff) end -= 1
    }
    chunks.push(payload.slice(start, end))
    start = end
  }
  return chunks
}

function handoffChunkKey(tokenHash: string, index: number): string {
  return `${tokenHash}:chunk:${index}`
}

async function createHandoff(env: Env, payload: unknown): Promise<string> {
  const token = bytesToBase64Url(crypto.getRandomValues(new Uint8Array(32)))
  const tokenHash = await sha256Hex(token)
  const now = Date.now()
  const expiresAt = now + handoffTtlSeconds(env) * 1_000
  const payloadText = JSON.stringify(payload)
  if (new TextEncoder().encode(payloadText).byteLength <= INLINE_HANDOFF_MAX_BYTES) {
    await env.DB.prepare(
      'INSERT INTO handoffs (token_hash, payload, created_at, expires_at, consumed_at) VALUES (?, ?, ?, ?, NULL)',
    )
      .bind(tokenHash, payloadText, now, expiresAt)
      .run()
    return token
  }

  const chunks = splitHandoffPayload(payloadText)
  await env.DB.prepare(
    'INSERT INTO handoffs (token_hash, payload, created_at, expires_at, consumed_at) VALUES (?, ?, ?, ?, NULL)',
  )
    .bind(tokenHash, `srl-chunks-v1:${chunks.length}`, now, expiresAt)
    .run()
  for (let offset = 0; offset < chunks.length; offset += HANDOFF_CHUNK_BATCH_SIZE) {
    const statements = chunks
      .slice(offset, offset + HANDOFF_CHUNK_BATCH_SIZE)
      .map((chunk, index) =>
        env.DB.prepare(
          'INSERT INTO handoffs (token_hash, payload, created_at, expires_at, consumed_at) VALUES (?, ?, ?, ?, NULL)',
        ).bind(handoffChunkKey(tokenHash, offset + index), chunk, now, expiresAt),
      )
    await env.DB.batch(statements)
  }
  return token
}

async function readHandoffPayload(
  env: Env,
  tokenHash: string,
  storedPayload: string,
): Promise<string | undefined> {
  const marker = HANDOFF_CHUNK_MARKER.exec(storedPayload)
  if (!marker) return storedPayload
  const chunkCount = Number(marker[1])
  if (!Number.isSafeInteger(chunkCount) || chunkCount < 1) return undefined

  const chunks: string[] = []
  for (let offset = 0; offset < chunkCount; offset += HANDOFF_CHUNK_BATCH_SIZE) {
    const count = Math.min(HANDOFF_CHUNK_BATCH_SIZE, chunkCount - offset)
    const results = await env.DB.batch(
      Array.from({ length: count }, (_, index) =>
        env.DB.prepare('SELECT payload FROM handoffs WHERE token_hash = ? LIMIT 1').bind(
          handoffChunkKey(tokenHash, offset + index),
        ),
      ),
    )
    for (const result of results) {
      const row = result.results?.[0] as { payload?: unknown } | undefined
      if (typeof row?.payload !== 'string') return undefined
      chunks.push(row.payload)
    }
  }
  return chunks.join('')
}

async function consumeHandoff(env: Env, token: string): Promise<Response> {
  if (!/^[A-Za-z0-9_-]{30,160}$/u.test(token))
    return json({ error: 'invalid_token' }, { status: 400 })
  const tokenHash = await sha256Hex(token)
  const now = Date.now()
  const row = await env.DB.prepare(
    'SELECT payload FROM handoffs WHERE token_hash = ? AND consumed_at IS NULL AND expires_at > ? LIMIT 1',
  )
    .bind(tokenHash, now)
    .first<{ payload: string }>()
  if (!row) return json({ error: 'handoff_not_found_or_expired' }, { status: 404 })

  const payloadText = await readHandoffPayload(env, tokenHash, row.payload)
  if (payloadText === undefined) return json({ error: 'handoff_payload_invalid' }, { status: 500 })
  let capture: unknown
  try {
    capture = JSON.parse(payloadText)
  } catch {
    return json({ error: 'handoff_payload_invalid' }, { status: 500 })
  }

  const claimed = await env.DB.prepare(
    'UPDATE handoffs SET consumed_at = ? WHERE token_hash = ? AND consumed_at IS NULL AND expires_at > ?',
  )
    .bind(now, tokenHash, now)
    .run()
  if ((claimed.meta.changes ?? 0) !== 1)
    return json({ error: 'handoff_already_consumed' }, { status: 409 })

  return json({ capture })
}

function discordApplicationCommandsUrl(env: Env): string {
  return `https://discord.com/api/v10/applications/${encodeURIComponent(env.DISCORD_APPLICATION_ID)}/commands`
}

function discordVariableStatus(env: Env) {
  return {
    applicationId: Boolean(env.DISCORD_APPLICATION_ID?.trim()),
    publicKey: Boolean(env.DISCORD_PUBLIC_KEY?.trim()),
    botToken: Boolean(env.DISCORD_BOT_TOKEN?.trim()),
  }
}

async function readCurrentBotApplication(env: Env): Promise<Record<string, unknown>> {
  const response = await fetch('https://discord.com/api/v10/oauth2/applications/@me', {
    headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
  })
  const application = asRecord(await discordJson(response, 'Discord bot application check'))
  if (!application) throw new Error('Discord bot application response invalid')
  return application
}

async function registerMessageCommand(env: Env): Promise<void> {
  if (!env.DISCORD_APPLICATION_ID || !env.DISCORD_BOT_TOKEN) {
    throw new Error('Discord Application ID / Bot Token 未配置')
  }
  const response = await fetch(discordApplicationCommandsUrl(env), {
    method: 'POST',
    headers: {
      Authorization: `Bot ${env.DISCORD_BOT_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: COMMAND_NAME,
      type: 3,
      integration_types: [1],
      contexts: [0, 1, 2],
    }),
  })
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 800)
    throw new Error(`Discord command registration failed: ${response.status} ${detail}`)
  }
}

async function readMessageCommandStatus(env: Env): Promise<boolean> {
  if (!env.DISCORD_APPLICATION_ID || !env.DISCORD_BOT_TOKEN) {
    throw new Error('Discord Application ID / Bot Token 未配置')
  }
  const response = await fetch(discordApplicationCommandsUrl(env), {
    method: 'GET',
    headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
  })
  const payload = await discordJson(response, 'Discord command status read')
  if (!Array.isArray(payload)) throw new Error('Discord command status response invalid')
  return payload.some((item) => {
    const command = asRecord(item)
    return asString(command?.name) === COMMAND_NAME && asNumber(command?.type) === 3
  })
}

async function updateDeferredInteraction(
  interaction: DiscordInteraction,
  env: Env,
  content: string,
  openUrl?: string,
): Promise<void> {
  const token = interaction.token
  if (!token) return
  const response = await fetch(
    `https://discord.com/api/v10/webhooks/${encodeURIComponent(env.DISCORD_APPLICATION_ID)}/${encodeURIComponent(token)}/messages/@original`,
    {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        content,
        ...(openUrl
          ? {
              components: [
                {
                  type: 1,
                  components: [{ type: 2, style: 5, label: '打开资源库', url: openUrl }],
                },
              ],
            }
          : { components: [] }),
      }),
    },
  )
  if (!response.ok) console.error('Discord deferred response update failed', response.status)
}

async function finishDeferredMessageCommand(
  interaction: DiscordInteraction,
  env: Env,
  requestUrl: string,
  capture: Record<string, unknown>,
): Promise<void> {
  try {
    const enrichedCapture = await addTextAttachmentContent(capture)
    const attachments = Array.isArray(enrichedCapture.attachments) ? enrichedCapture.attachments : []
    const textAttachmentCount = attachments.filter((item) => {
      const attachment = asRecord(item)
      return asString(attachment?.name).toLowerCase().endsWith('.txt')
    }).length
    const extractedCount = attachments.filter((item) => {
      const attachment = asRecord(item)
      return typeof attachment?.textContent === 'string'
    }).length
    const token = await createHandoff(env, enrichedCapture)
    const openUrl = `${new URL(requestUrl).origin}/open/${encodeURIComponent(token)}`
    const content =
      textAttachmentCount > extractedCount
        ? `消息已保存。已读取 ${extractedCount} 个 .txt 附件；其余附件过大或暂时无法读取，仍可从附件链接打开。`
        : `消息和 ${extractedCount} 个 .txt 附件文字已保存。打开资源库后再选择关联到哪个资源。`
    await updateDeferredInteraction(interaction, env, content, openUrl)
  } catch (error) {
    console.error('Discord message handoff failed', error)
    await updateDeferredInteraction(interaction, env, '保存失败，请稍后重试。')
  }
}

function authorizedSetup(request: Request, env: Env): boolean {
  const header = request.headers.get('Authorization') ?? ''
  return Boolean(env.DISCORD_BOT_TOKEN) && header === `Bearer ${env.DISCORD_BOT_TOKEN}`
}

function validSnowflake(value: string | undefined): value is string {
  return Boolean(value && DISCORD_SNOWFLAKE_PATTERN.test(value))
}

function compareSnowflakes(left: string, right: string): number {
  const a = BigInt(left)
  const b = BigInt(right)
  return a < b ? -1 : a > b ? 1 : 0
}

function parseScanCursor(value: unknown): DiscordSourceRemoteScanCursor | undefined {
  const record = asRecord(value)
  if (!record) return undefined
  const lastSeenMessageId = asString(record.lastSeenMessageId).trim() || undefined
  const pendingBeforeMessageId = asString(record.pendingBeforeMessageId).trim() || undefined
  const pendingHighWaterMessageId = asString(record.pendingHighWaterMessageId).trim() || undefined
  if (lastSeenMessageId && !validSnowflake(lastSeenMessageId)) return undefined
  if (pendingBeforeMessageId && !validSnowflake(pendingBeforeMessageId)) return undefined
  if (pendingHighWaterMessageId && !validSnowflake(pendingHighWaterMessageId)) return undefined
  if (pendingBeforeMessageId || pendingHighWaterMessageId) {
    if (!lastSeenMessageId || !pendingBeforeMessageId || !pendingHighWaterMessageId)
      return undefined
  }
  if (!lastSeenMessageId && !pendingBeforeMessageId && !pendingHighWaterMessageId) return undefined
  if (lastSeenMessageId && pendingBeforeMessageId && pendingHighWaterMessageId) {
    return { lastSeenMessageId, pendingBeforeMessageId, pendingHighWaterMessageId }
  }
  return lastSeenMessageId ? { lastSeenMessageId } : undefined
}

function parseSourceReadRequest(value: unknown): DiscordSourceReadRequest | undefined {
  const record = asRecord(value)
  if (!record) return undefined
  const channelId = asString(record.channelId).trim()
  const guildId = asString(record.guildId).trim() || undefined
  const threadId = asString(record.threadId).trim() || undefined
  const starterMessageId = asString(record.starterMessageId).trim() || undefined
  if (!validSnowflake(channelId)) return undefined
  if (guildId && !validSnowflake(guildId)) return undefined
  if (threadId && !validSnowflake(threadId)) return undefined
  if (starterMessageId && !validSnowflake(starterMessageId)) return undefined
  const savedMessageIds = Array.isArray(record.savedMessageIds)
    ? Array.from(
        new Set(
          record.savedMessageIds
            .filter((item): item is string => typeof item === 'string')
            .map((item) => item.trim())
            .filter((item) => DISCORD_SNOWFLAKE_PATTERN.test(item)),
        ),
      ).slice(0, MAX_SAVED_MESSAGE_IDS)
    : []
  const scanMessageIds = Array.isArray(record.scanMessageIds)
    ? Array.from(
        new Set(
          record.scanMessageIds
            .filter((item): item is string => typeof item === 'string')
            .map((item) => item.trim())
            .filter((item) => DISCORD_SNOWFLAKE_PATTERN.test(item)),
        ),
      ).slice(0, MAX_HEALTH_MESSAGE_IDS)
    : []
  const scanCursor = parseScanCursor(record.scanCursor)
  return {
    guildId,
    channelId,
    threadId,
    starterMessageId,
    savedMessageIds,
    scanMessageIds,
    scanCursor,
  }
}

function parseSavedMessageCheckRequest(
  value: unknown,
): DiscordSavedMessageCheckRequest | undefined {
  const record = asRecord(value)
  if (!record) return undefined
  const channelId = asString(record.channelId).trim()
  const guildId = asString(record.guildId).trim() || undefined
  const threadId = asString(record.threadId).trim() || undefined
  const starterMessageId = asString(record.starterMessageId).trim() || undefined
  if (!validSnowflake(channelId)) return undefined
  if (guildId && !validSnowflake(guildId)) return undefined
  if (threadId && !validSnowflake(threadId)) return undefined
  if (starterMessageId && !validSnowflake(starterMessageId)) return undefined
  const messageIds = Array.isArray(record.messageIds)
    ? Array.from(
        new Set(
          record.messageIds
            .filter((item): item is string => typeof item === 'string')
            .map((item) => item.trim())
            .filter((item) => DISCORD_SNOWFLAKE_PATTERN.test(item)),
        ),
      ).slice(0, MAX_HEALTH_MESSAGE_IDS)
    : []
  if (!messageIds.length) return undefined
  return { guildId, channelId, threadId, starterMessageId, messageIds }
}

async function discordApi(env: Env, path: string): Promise<Response> {
  return fetch(`https://discord.com/api/v10${path}`, {
    method: 'GET',
    headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
  })
}

function retryAfterMs(response: Response): number | undefined {
  const header =
    response.headers.get('Retry-After') ?? response.headers.get('X-RateLimit-Reset-After')
  if (!header) return undefined
  const value = Number(header)
  if (!Number.isFinite(value) || value < 0) return undefined
  return Math.round(value * 1_000)
}

async function discordJson(response: Response, operation: string): Promise<unknown> {
  if (response.status === 429) throw new DiscordRateLimitError(operation, retryAfterMs(response))
  if (!response.ok) {
    const detail = (await response.text()).slice(0, 500)
    throw new Error(`${operation} failed: ${response.status} ${detail}`)
  }
  return response.json()
}

async function readGuildMetadata(
  env: Env,
  guildId: string | undefined,
): Promise<DiscordGuildMetadata> {
  if (!validSnowflake(guildId)) return { access: 'unavailable' }
  const response = await discordApi(env, `/guilds/${encodeURIComponent(guildId)}`)
  if (response.status === 403 || response.status === 404) return { access: 'unavailable' }
  if (!response.ok) return { access: 'unknown' }
  const guild = asRecord(await response.json())
  const name = asString(guild?.name)
  return {
    access: 'available',
    ...(name ? { name } : {}),
  }
}

async function readThreadParentMetadata(
  env: Env,
  thread: Record<string, unknown>,
): Promise<ThreadParentMetadata> {
  const parentId = asString(thread.parent_id)
  if (!validSnowflake(parentId)) return { forumTags: [] }
  const response = await discordApi(env, `/channels/${encodeURIComponent(parentId)}`)
  if (!response.ok) return { forumTags: [] }
  const parent = asRecord(await response.json())
  if (!parent) return { forumTags: [] }
  const applied = Array.isArray(thread.applied_tags)
    ? thread.applied_tags.filter((item): item is string => typeof item === 'string')
    : []
  return {
    channelName: asString(parent.name) || undefined,
    forumTags: readForumTags({ ...parent, applied_tags: applied }),
  }
}

function buildCaptureFromMessage(
  message: Record<string, unknown>,
  context: DiscordCaptureContext,
): Record<string, unknown> | undefined {
  const messageId = asString(message.id)
  if (!validSnowflake(messageId)) return undefined
  const author = asRecord(message.author)
  const authorId = asString(author?.id)
  const authorName =
    asString(author?.global_name) || asString(author?.username) || asString(message.author_name)
  if (!validSnowflake(authorId) || !authorName) return undefined
  const authorBot = author?.bot === true || Boolean(asString(message.webhook_id))
  const guildPath = context.guildId ?? '@me'
  const canonicalUrl = `https://discord.com/channels/${encodeURIComponent(guildPath)}/${encodeURIComponent(context.channelId)}/${encodeURIComponent(messageId)}`
  const isStarter = messageId === context.starterMessageId
  return {
    ...(context.guildId ? { guildId: context.guildId } : {}),
    ...(context.guildName ? { guildName: context.guildName } : {}),
    channelId: context.channelId,
    ...(context.channelName ? { channelName: context.channelName } : {}),
    ...(context.threadId ? { threadId: context.threadId } : {}),
    starterMessageId: context.starterMessageId,
    isStarter,
    messageId,
    canonicalUrl,
    authorId,
    authorName,
    authorBot,
    content: typeof message.content === 'string' ? message.content : '',
    timestamp: asString(message.timestamp) || new Date().toISOString(),
    ...(asString(message.edited_timestamp)
      ? { editedTimestamp: asString(message.edited_timestamp) }
      : {}),
    ...(isStarter && context.title ? { title: context.title } : {}),
    forumTags: isStarter ? context.forumTags : [],
    embeds: Array.isArray(message.embeds)
      ? message.embeds.filter((value) => Boolean(asRecord(value)))
      : [],
    attachments: normalizeAttachments(message.attachments),
  }
}

function recordsFromMessageList(payload: unknown): Record<string, unknown>[] {
  if (!Array.isArray(payload)) throw new Error('Discord thread messages response invalid')
  return payload.flatMap((item) => {
    const message = asRecord(item)
    return message ? [message] : []
  })
}

function buildInitialThreadQuery(scanCursor: DiscordSourceRemoteScanCursor | undefined): string {
  const query = new URLSearchParams({ limit: '100' })
  if (scanCursor?.pendingBeforeMessageId) {
    query.set('before', scanCursor.pendingBeforeMessageId)
  } else if (scanCursor?.lastSeenMessageId) {
    query.set('after', scanCursor.lastSeenMessageId)
  }
  return query.toString()
}

function nextScanCursor(
  inputCursor: DiscordSourceRemoteScanCursor | undefined,
  highWaterMessageId: string | undefined,
  nextBeforeMessageId: string | undefined,
): DiscordSourceRemoteScanCursor | undefined {
  if (!highWaterMessageId) return inputCursor
  if (nextBeforeMessageId && inputCursor?.lastSeenMessageId) {
    return {
      lastSeenMessageId: inputCursor.lastSeenMessageId,
      pendingBeforeMessageId: nextBeforeMessageId,
      pendingHighWaterMessageId: inputCursor.pendingHighWaterMessageId ?? highWaterMessageId,
    }
  }
  return { lastSeenMessageId: inputCursor?.pendingHighWaterMessageId ?? highWaterMessageId }
}

async function readSourceMessages(
  env: Env,
  input: DiscordSourceReadRequest,
): Promise<
  | {
      state: 'available'
      captures: Array<Record<string, unknown>>
      scanCursor?: DiscordSourceRemoteScanCursor
    }
  | DiscordSourceReadFailure
> {
  const channelId = input.threadId ?? input.channelId
  const [channelResponse, guildMetadata] = await Promise.all([
    discordApi(env, `/channels/${encodeURIComponent(channelId)}`),
    readGuildMetadata(env, input.guildId),
  ])
  if (channelResponse.status === 403) {
    return {
      state: 'uncheckable',
      reason: 'forbidden',
      stage: 'channel',
    }
  }
  if (channelResponse.status === 404) {
    if (guildMetadata.access === 'available') {
      return { state: 'unavailable', reason: 'not_found', stage: 'channel' }
    }
    return {
      state: 'uncheckable',
      reason: guildMetadata.access === 'unavailable' ? 'bot_access' : 'read_failed',
      stage: 'channel',
    }
  }
  const channel = asRecord(await discordJson(channelResponse, 'Discord channel read'))
  if (!channel) throw new Error('Discord channel response invalid')
  const channelType = asNumber(channel.type)
  const threadId =
    input.threadId ??
    (channelType !== undefined && THREAD_CHANNEL_TYPES.has(channelType) ? channelId : undefined)
  const starterMessageId = threadId ?? input.starterMessageId ?? input.savedMessageIds[0]
  if (!validSnowflake(starterMessageId))
    throw new Error('Discord source starter message is missing')

  const starterPromise = discordApi(
    env,
    `/channels/${encodeURIComponent(channelId)}/messages/${encodeURIComponent(starterMessageId)}`,
  )
  const parentMetadataPromise = threadId
    ? readThreadParentMetadata(env, channel)
    : Promise.resolve<ThreadParentMetadata>({
        channelName: asString(channel.name) || undefined,
        forumTags: [],
      })
  const firstPagePromise = threadId
    ? discordApi(
        env,
        `/channels/${encodeURIComponent(channelId)}/messages?${buildInitialThreadQuery(input.scanCursor)}`,
      )
    : Promise.resolve<Response | undefined>(undefined)

  const [starterResponse, parentMetadata, firstPageResponse] = await Promise.all([
    starterPromise,
    parentMetadataPromise,
    firstPagePromise,
  ])
  if (starterResponse.status === 403) {
    return {
      state: 'uncheckable',
      reason: 'forbidden',
      stage: 'starter',
    }
  }
  if (starterResponse.status === 404)
    return { state: 'unavailable', reason: 'not_found', stage: 'starter' }
  const starter = asRecord(await discordJson(starterResponse, 'Discord starter message read'))
  if (!starter) throw new Error('Discord starter message response invalid')

  const context: DiscordCaptureContext = {
    guildId: input.guildId,
    guildName: guildMetadata.name,
    channelId,
    channelName: parentMetadata.channelName,
    threadId,
    starterMessageId,
    title: threadId ? asString(channel.name) || undefined : undefined,
    forumTags: parentMetadata.forumTags,
  }

  const messages = new Map<string, Record<string, unknown>>()
  messages.set(starterMessageId, starter)
  const starterAuthorId = asString(asRecord(starter.author)?.id)
  let scanCursor = input.scanCursor

  if (threadId && firstPageResponse) {
    if (firstPageResponse.status === 403 || firstPageResponse.status === 404) {
      return {
        state: 'uncheckable',
        reason: firstPageResponse.status === 403 ? 'forbidden' : 'read_failed',
        stage: 'messages',
      }
    }
    let records = recordsFromMessageList(
      await discordJson(firstPageResponse, 'Discord thread messages read'),
    )
    const lowerBound = input.scanCursor?.lastSeenMessageId
    let highWaterMessageId = input.scanCursor?.pendingHighWaterMessageId
    let nextBeforeMessageId: string | undefined
    let completed = false

    for (let page = 0; page < MAX_THREAD_PAGES; page += 1) {
      if (!highWaterMessageId) {
        const newest = asString(records[0]?.id)
        if (validSnowflake(newest)) highWaterMessageId = newest
      }
      let crossedLowerBound = false
      for (const message of records) {
        const id = asString(message.id)
        if (!validSnowflake(id)) continue
        if (lowerBound && compareSnowflakes(id, lowerBound) <= 0) {
          crossedLowerBound = true
          continue
        }
        const author = asRecord(message.author)
        const authorId = asString(author?.id)
        const relevant =
          input.savedMessageIds.includes(id) ||
          input.scanMessageIds.includes(id) ||
          id === starterMessageId ||
          (starterAuthorId && authorId === starterAuthorId) ||
          author?.bot === true ||
          Boolean(asString(message.webhook_id))
        if (relevant) messages.set(id, message)
      }

      if (crossedLowerBound || records.length < 100) {
        completed = true
        break
      }
      const before = asString(records.at(-1)?.id)
      if (!validSnowflake(before)) {
        completed = true
        break
      }
      if (page + 1 >= MAX_THREAD_PAGES) {
        nextBeforeMessageId = before
        break
      }
      const query = new URLSearchParams({ limit: '100', before })
      const response = await discordApi(
        env,
        `/channels/${encodeURIComponent(channelId)}/messages?${query.toString()}`,
      )
      if (response.status === 403 || response.status === 404) {
        return {
          state: 'uncheckable',
          reason: response.status === 403 ? 'forbidden' : 'read_failed',
          stage: 'messages',
        }
      }
      records = recordsFromMessageList(await discordJson(response, 'Discord thread messages read'))
    }

    if (!lowerBound) {
      // 旧来源 / 首次读取保持原来的“最多最近 300 条”合同；完成这次有界 bootstrap 后
      // 只把最顶部 message 作为后续增量高水位，不尝试无限回扫历史。
      // 空 thread 极端情况下以 starter 作为稳定下界，避免下次又退回 bootstrap。
      const bootstrapHighWater = highWaterMessageId ?? starterMessageId
      scanCursor = { lastSeenMessageId: bootstrapHighWater }
    } else {
      scanCursor = nextScanCursor(
        input.scanCursor,
        highWaterMessageId,
        completed ? undefined : nextBeforeMessageId,
      )
    }
  }

  for (const messageId of input.savedMessageIds) {
    if (messages.has(messageId)) continue
    const response = await discordApi(
      env,
      `/channels/${encodeURIComponent(channelId)}/messages/${encodeURIComponent(messageId)}`,
    )
    if (response.status === 404) continue
    if (response.status === 403)
      return { state: 'uncheckable', reason: 'forbidden', stage: 'saved_messages' }
    const message = asRecord(await discordJson(response, 'Discord saved message read'))
    if (message) messages.set(messageId, message)
  }

  const textAttachmentBudget = { remainingBytes: 2_000_000 }
  const captures: Array<Record<string, unknown>> = []
  for (const message of messages.values()) {
    const capture = buildCaptureFromMessage(message, context)
    if (capture && textAttachmentBudget.remainingBytes > 0) {
      captures.push(await addTextAttachmentContent(capture, textAttachmentBudget))
    } else if (capture) {
      captures.push(capture)
    }
  }
  captures.sort((left, right) => {
    const leftTimestamp = Date.parse(asString(left.timestamp))
    const rightTimestamp = Date.parse(asString(right.timestamp))
    return leftTimestamp - rightTimestamp
  })
  return { state: 'available', captures, scanCursor }
}

async function readSavedMessageHealth(
  env: Env,
  input: DiscordSavedMessageCheckRequest,
): Promise<
  | {
      state: 'available'
      captures: Array<Record<string, unknown>>
      missingMessageIds: string[]
      checkedMessageIds: string[]
      rateLimited: boolean
      retryAfterMs?: number
    }
  | Extract<DiscordSourceReadFailure, { state: 'uncheckable' }>
> {
  const channelId = input.threadId ?? input.channelId
  const starterMessageId = input.starterMessageId ?? input.threadId ?? input.messageIds[0]
  if (!validSnowflake(starterMessageId))
    throw new Error('Discord source starter message is missing')

  const context: DiscordCaptureContext = {
    guildId: input.guildId,
    channelId,
    threadId: input.threadId,
    starterMessageId,
    forumTags: [],
  }
  const captures: Array<Record<string, unknown>> = []
  const textAttachmentBudget = { remainingBytes: 2_000_000 }
  const missingMessageIds: string[] = []
  const checkedMessageIds: string[] = []
  let index = 0
  let stopped = false
  let forbidden = false
  let rateLimited = false
  let rateLimitDelay: number | undefined

  const worker = async () => {
    while (!stopped) {
      const current = index
      index += 1
      if (current >= input.messageIds.length) return
      const messageId = input.messageIds[current]
      if (!messageId) return
      const response = await discordApi(
        env,
        `/channels/${encodeURIComponent(channelId)}/messages/${encodeURIComponent(messageId)}`,
      )
      if (response.status === 429) {
        rateLimited = true
        rateLimitDelay = retryAfterMs(response)
        stopped = true
        return
      }
      if (response.status === 403) {
        forbidden = true
        stopped = true
        return
      }
      if (response.status === 404) {
        missingMessageIds.push(messageId)
        checkedMessageIds.push(messageId)
        continue
      }
      const message = asRecord(await discordJson(response, 'Discord saved message health read'))
      if (!message) throw new Error('Discord saved message response invalid')
      const capture = buildCaptureFromMessage(message, context)
      if (!capture) throw new Error('Discord saved message capture invalid')
      captures.push(
        textAttachmentBudget.remainingBytes > 0
          ? await addTextAttachmentContent(capture, textAttachmentBudget)
          : capture,
      )
      checkedMessageIds.push(messageId)
    }
  }

  await Promise.all(Array.from({ length: HEALTH_CHECK_CONCURRENCY }, () => worker()))
  if (forbidden) return { state: 'uncheckable', reason: 'forbidden', stage: 'saved_messages' }
  return {
    state: 'available',
    captures,
    missingMessageIds,
    checkedMessageIds,
    rateLimited,
    ...(rateLimitDelay !== undefined ? { retryAfterMs: rateLimitDelay } : {}),
  }
}

async function handleSourceRead(request: Request, env: Env): Promise<Response> {
  if (!authorizedSetup(request, env)) return json({ error: 'unauthorized' }, { status: 401 })
  let input: DiscordSourceReadRequest | undefined
  try {
    input = parseSourceReadRequest(await request.json())
  } catch {
    input = undefined
  }
  if (!input) return json({ error: 'invalid_source_request' }, { status: 400 })
  try {
    return json(await readSourceMessages(env, input))
  } catch (error) {
    if (error instanceof DiscordRateLimitError) {
      return json(
        { error: 'discord_rate_limited', retryAfterMs: error.retryAfterMs },
        { status: 429 },
      )
    }
    console.error('Discord source read failed', error)
    return json(
      { error: error instanceof Error ? error.message : 'source_read_failed' },
      { status: 502 },
    )
  }
}

async function handleSavedMessageCheck(request: Request, env: Env): Promise<Response> {
  if (!authorizedSetup(request, env)) return json({ error: 'unauthorized' }, { status: 401 })
  let input: DiscordSavedMessageCheckRequest | undefined
  try {
    input = parseSavedMessageCheckRequest(await request.json())
  } catch {
    input = undefined
  }
  if (!input) return json({ error: 'invalid_message_check_request' }, { status: 400 })
  try {
    return json(await readSavedMessageHealth(env, input))
  } catch (error) {
    console.error('Discord saved message health check failed', error)
    return json(
      { error: error instanceof Error ? error.message : 'message_check_failed' },
      { status: 502 },
    )
  }
}

function openPage(request: Request, token: string): Response {
  if (!/^[A-Za-z0-9_-]{30,160}$/u.test(token)) return html('<h1>链接无效</h1>', { status: 400 })
  const origin = new URL(request.url).origin
  const nativeUrl = `srl://discord-source?worker=${encodeURIComponent(origin)}&token=${encodeURIComponent(token)}`
  const handoffUrl = `${origin}/open/${encodeURIComponent(token)}`
  return html(`<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>打开 SRL</title>
<style>body{font-family:system-ui,sans-serif;max-width:560px;margin:0 auto;padding:32px 20px;color:#173641;background:#f5fafb}h1{font-size:1.35rem}p{color:#647b83;line-height:1.65}.a{display:block;width:100%;margin-top:12px;padding:12px 14px;border:1px solid #bfd0d4;border-radius:8px;color:#315e6d;text-decoration:none;font:inherit;font-weight:700;text-align:left;cursor:pointer}.hint{font-size:.82rem;color:#82949b}.handoff-fallback[hidden]{display:none}</style></head>
<body><h1>Discord 来源已接收</h1><p>消息正在你自己的 Worker 中临时等待领取。请复制临时链接，回到正在使用的 SRL 网页 / PWA 粘贴领取，内容会保存到当前应用的资源库。</p>
<a class="a" href="${escapeHtml(nativeUrl)}">打开 SRL Android App</a>
<button class="a" id="copy-handoff" type="button" data-handoff-url="${escapeHtml(handoffUrl)}">复制临时链接，回 PWA 粘贴领取</button>
<input class="a handoff-fallback" id="handoff-fallback" type="url" value="${escapeHtml(handoffUrl)}" readonly aria-label="临时领取链接" hidden>
<p class="hint" id="copy-status" role="status">领取链接仅供一次使用并会自动过期。</p>
<script>
document.getElementById('copy-handoff')?.addEventListener('click', async (event) => {
  const button = event.currentTarget;
  const value = button instanceof HTMLButtonElement ? button.dataset.handoffUrl : '';
  if (!value) return;
  let copied = false;
  try {
    await navigator.clipboard.writeText(value);
    copied = true;
  } catch {
    const input = document.createElement('textarea');
    input.value = value;
    input.setAttribute('readonly', '');
    input.style.position = 'fixed';
    input.style.opacity = '0';
    document.body.append(input);
    input.select();
    copied = document.execCommand('copy');
    input.remove();
  }
  const status = document.getElementById('copy-status');
  if (copied) {
    if (status) status.textContent = '已复制。请切回 SRL 网页 / PWA，打开来源链接高级设置并粘贴领取。';
  } else {
    const fallback = document.getElementById('handoff-fallback');
    if (fallback instanceof HTMLInputElement) {
      fallback.hidden = false;
      fallback.focus();
      fallback.select();
    }
    if (status) status.textContent = '自动复制失败，请长按选中的临时链接并复制。';
  }
});
</script>
</body></html>`)
}

async function handleInteraction(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const rawBody = await request.text()
  if (!(await verifyDiscordRequest(request, rawBody, env.DISCORD_PUBLIC_KEY))) {
    return new Response('invalid request signature', { status: 401 })
  }

  let interaction: DiscordInteraction
  try {
    interaction = JSON.parse(rawBody) as DiscordInteraction
  } catch {
    return new Response('invalid json', { status: 400 })
  }

  if (interaction.type === 1) {
    return json({ type: 1 })
  }

  if (interaction.type !== 2 || interaction.data?.type !== 3) {
    return json({
      type: 4,
      data: { content: '这个命令只用于保存 Discord 消息。', flags: 64 },
    })
  }

  try {
    const capture = buildCapture(interaction)
    if (hasTextAttachments(capture) && interaction.token) {
      ctx.waitUntil(cleanupExpired(env).catch((error) => console.error(error)))
      ctx.waitUntil(
        finishDeferredMessageCommand(interaction, env, request.url, capture).catch((error) => {
          console.error('Discord deferred message command failed', error)
        }),
      )
      return json({ type: 5, data: { flags: 64 } })
    }
    const token = await createHandoff(env, capture)
    const openUrl = `${new URL(request.url).origin}/open/${encodeURIComponent(token)}`
    ctx.waitUntil(cleanupExpired(env).catch((error) => console.error(error)))
    return json({
      type: 4,
      data: {
        content: '已完整接收这条消息。打开资源库后再选择关联到哪个资源。',
        flags: 64,
        components: [
          {
            type: 1,
            components: [{ type: 2, style: 5, label: '打开资源库', url: openUrl }],
          },
        ],
      },
    })
  } catch (error) {
    console.error(error)
    return json({
      type: 4,
      data: {
        content: `保存失败：${error instanceof Error ? error.message : '无法读取消息'}`,
        flags: 64,
      },
    })
  }
}

async function readRequestedApplicationId(request: Request): Promise<string | undefined> {
  try {
    const body = asRecord(await request.json())
    const applicationId = asString(body?.applicationId).trim()
    return applicationId || undefined
  } catch {
    return undefined
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    if (request.method === 'OPTIONS')
      return new Response(null, { status: 204, headers: CORS_HEADERS })

    if (url.pathname === '/health' && request.method === 'GET') {
      const discordVariables = discordVariableStatus(env)
      const discordConfigured = Object.values(discordVariables).every(Boolean)
      try {
        await env.DB.prepare('SELECT 1 FROM handoffs LIMIT 1').first()
        return json({
          ok: true,
          database: true,
          discordConfigured,
          discordVariables,
          applicationId: env.DISCORD_APPLICATION_ID || null,
          commandName: COMMAND_NAME,
        })
      } catch {
        return json(
          {
            ok: false,
            database: false,
            discordConfigured,
            discordVariables,
            applicationId: env.DISCORD_APPLICATION_ID || null,
          },
          { status: 503 },
        )
      }
    }

    if (url.pathname === '/setup/status' && request.method === 'GET') {
      if (!authorizedSetup(request, env)) return json({ error: 'unauthorized' }, { status: 401 })
      try {
        const botApplication = await readCurrentBotApplication(env)
        const applicationIdMatches = asString(botApplication.id) === env.DISCORD_APPLICATION_ID
        const publicKeyMatches =
          asString(botApplication.verify_key).toLowerCase() ===
          (env.DISCORD_PUBLIC_KEY ?? '').trim().toLowerCase()
        return json({
          ok: true,
          applicationId: env.DISCORD_APPLICATION_ID,
          applicationIdMatches,
          publicKeyMatches,
          commandName: COMMAND_NAME,
          commandRegistered: applicationIdMatches ? await readMessageCommandStatus(env) : false,
        })
      } catch (error) {
        return json(
          { error: error instanceof Error ? error.message : 'command_status_failed' },
          { status: 502 },
        )
      }
    }

    if (url.pathname === '/setup/register' && request.method === 'POST') {
      if (!authorizedSetup(request, env)) return json({ error: 'unauthorized' }, { status: 401 })
      const requestedApplicationId = await readRequestedApplicationId(request)
      if (requestedApplicationId && requestedApplicationId !== env.DISCORD_APPLICATION_ID) {
        return json(
          {
            error: 'application_id_mismatch',
            applicationId: env.DISCORD_APPLICATION_ID,
          },
          { status: 409 },
        )
      }
      try {
        await registerMessageCommand(env)
        return json({
          ok: true,
          applicationId: env.DISCORD_APPLICATION_ID,
          commandName: COMMAND_NAME,
          commandRegistered: true,
        })
      } catch (error) {
        return json(
          { error: error instanceof Error ? error.message : 'command_registration_failed' },
          { status: 502 },
        )
      }
    }

    if (url.pathname === '/source/read' && request.method === 'POST') {
      return handleSourceRead(request, env)
    }

    if (url.pathname === '/source/messages/check' && request.method === 'POST') {
      return handleSavedMessageCheck(request, env)
    }

    if (url.pathname === '/interactions' && request.method === 'POST') {
      return handleInteraction(request, env, ctx)
    }

    if (url.pathname.startsWith('/handoff/') && request.method === 'GET') {
      const token = decodeURIComponent(url.pathname.slice('/handoff/'.length))
      const response = await consumeHandoff(env, token)
      ctx.waitUntil(cleanupExpired(env).catch((error) => console.error(error)))
      return response
    }

    if (url.pathname.startsWith('/open/') && request.method === 'GET') {
      return openPage(request, decodeURIComponent(url.pathname.slice('/open/'.length)))
    }

    if (url.pathname === '/' && request.method === 'GET') {
      return html(
        `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SRL Discord Bridge</title></head><body style="font-family:system-ui,sans-serif;max-width:680px;margin:40px auto;padding:0 20px;color:#173641"><h1>SRL Discord Bridge</h1><p>这个 Worker 只负责 Discord 消息的短期 handoff 与用户主动发起的只读来源检查，不是资源永久仓库。</p><p>Interactions Endpoint URL：</p><code style="overflow-wrap:anywhere">${escapeHtml(`${url.origin}/interactions`)}</code></body></html>`,
      )
    }

    return json({ error: 'not_found' }, { status: 404 })
  },
}
