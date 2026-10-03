/* global Request */

import {
  THREAD_CHANNEL_TYPES,
  addTextAttachmentContent,
  asNumber,
  asRecord,
  asString,
  buildCaptureFromMessage,
  readForumTags,
} from './DiscordSourceCapture'
import {
  DISCORD_SNOWFLAKE_PATTERN,
  DiscordCaptureContext,
  DiscordGuildMetadata,
  DiscordSavedMessageCheckRequest,
  DiscordSourceReadFailure,
  DiscordSourceReadRequest,
  DiscordSourceRemoteScanCursor,
  Env,
  ThreadParentMetadata,
  validSnowflake,
} from './DiscordSourceProtocol'
import { DiscordRateLimitError, authorizedSetup, json } from './DiscordWorkerHttp'

const MAX_THREAD_PAGES = 3

const MAX_SAVED_MESSAGE_IDS = 24

const MAX_HEALTH_MESSAGE_IDS = 12

const HEALTH_CHECK_CONCURRENCY = 2

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

export async function discordJson(response: Response, operation: string): Promise<unknown> {
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

  const captures: Array<Record<string, unknown>> = []
  const textAttachmentBudget = { remainingBytes: 2_000_000 }
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

export async function handleSourceRead(request: Request, env: Env): Promise<Response> {
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

export async function handleSavedMessageCheck(request: Request, env: Env): Promise<Response> {
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
