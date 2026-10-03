import { DiscordCaptureContext, DiscordInteraction, validSnowflake } from './DiscordSourceProtocol'

export const THREAD_CHANNEL_TYPES = new Set([10, 11, 12])

export function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

export function asString(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

export function asNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

export function readForumTags(channel?: Record<string, unknown>): string[] {
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

export function hasTextAttachments(capture: Record<string, unknown>): boolean {
  return (
    Array.isArray(capture.attachments) &&
    capture.attachments.some((item) => {
      const attachment = asRecord(item)
      return asString(attachment?.name).toLowerCase().endsWith('.txt')
    })
  )
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
      redirect: 'manual',
      signal: controller.signal,
    })
    if (!response.ok || !response.body) {
      await response.body?.cancel()
      return undefined
    }
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

export async function addTextAttachmentContent(
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

export function buildCapture(interaction: DiscordInteraction): Record<string, unknown> {
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

export function buildCaptureFromMessage(
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
