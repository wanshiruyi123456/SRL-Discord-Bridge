import { asRecord, asString } from './DiscordSourceCapture'
import { type DiscordInteraction, type Env, validSnowflake } from './DiscordSourceProtocol'
import { authorizedSetup, bytesToBase64Url, json, sha256Hex } from './DiscordWorkerHttp'

const DELIVERY_TTL_MS = 7 * 24 * 60 * 60 * 1_000
const UNPAIRED_TTL_MS = 20 * 60 * 1_000
const PAIR_TTL_MS = 10 * 60 * 1_000
const PAGE_SIZE = 20
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{30,160}$/u
const LIBRARY_PATTERN = /^[A-Za-z0-9_-]{8,100}$/u

type DeliveryState = 'pending' | 'saved' | 'waiting_binding'
interface Endpoint {
  library_id: string
  name: string
  discord_user_id: string | null
  is_default: number
}
interface Delivery {
  id: string
  handoff_token_hash: string
  library_id: string | null
  claimed_library_id: string | null
  state: DeliveryState
  title: string | null
  created_at: number
  expires_at: number
  library_name?: string | null
}
export interface InboxHandoffStore {
  create(env: Env, payload: unknown, expiresAt?: number): Promise<string>
  read(env: Env, tokenHash: string, payload: string): Promise<string | undefined>
}

export class InboxError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message)
  }
}

export async function requestBody(request: Request): Promise<Record<string, unknown>> {
  const reader = request.body?.getReader()
  if (!reader) throw new InboxError(400, 'invalid_body')
  const chunks: Uint8Array[] = []
  let size = 0
  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      size += value.byteLength
      if (size > 4_096) {
        await reader.cancel()
        throw new InboxError(413, 'body_too_large')
      }
      chunks.push(value)
    }
  } finally {
    reader.releaseLock()
  }
  const bytes = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) {
    bytes.set(chunk, offset)
    offset += chunk.byteLength
  }
  try {
    const value = asRecord(JSON.parse(new TextDecoder().decode(bytes)))
    if (value) return value
  } catch {
    /* Invalid JSON uses the same bounded client error. */
  }
  throw new InboxError(400, 'invalid_body')
}

export async function authenticatedEndpoint(request: Request, env: Env): Promise<Endpoint> {
  const libraryId = request.headers.get('X-SRL-Library-ID') ?? ''
  const secret = (request.headers.get('Authorization') ?? '').replace(/^Bearer /u, '')
  if (!LIBRARY_PATTERN.test(libraryId) || !TOKEN_PATTERN.test(secret))
    throw new InboxError(401, 'unauthorized')
  const endpoint = await env.DB.prepare(
    `SELECT library_id, name, discord_user_id, is_default FROM inbox_endpoints
     WHERE library_id = ? AND secret_hash = ? AND revoked_at IS NULL`,
  )
    .bind(libraryId, await sha256Hex(secret))
    .first<Endpoint>()
  if (!endpoint) throw new InboxError(401, 'unauthorized')
  return endpoint
}

export function interactionUserId(interaction: DiscordInteraction): string | undefined {
  const id = interaction.member?.user?.id ?? interaction.user?.id
  return validSnowflake(id) ? id : undefined
}

export async function pairDiscordUser(interaction: DiscordInteraction, env: Env): Promise<string> {
  const userId = interactionUserId(interaction)
  const code = asString(
    interaction.data?.options?.find((option) => option.name === 'code')?.value,
  ).trim()
  if (!userId || !/^[A-Za-z0-9_-]{16}$/u.test(code))
    throw new InboxError(400, '配对码无效，请在资源库重新生成')
  const now = Date.now()
  const claimId = crypto.randomUUID()
  const result = await env.DB.batch([
    env.DB.prepare(
      `UPDATE inbox_pair_codes SET consumed_at = ?, claim_id = ?, discord_user_id = ?
       WHERE code_hash = ? AND consumed_at IS NULL AND expires_at > ?
         AND library_id IN (SELECT library_id FROM inbox_endpoints WHERE revoked_at IS NULL)`,
    ).bind(now, claimId, userId, await sha256Hex(code), now),
    env.DB.prepare(
      `UPDATE inbox_endpoints SET is_default = 0
       WHERE discord_user_id = ? AND revoked_at IS NULL
         AND library_id <> (SELECT library_id FROM inbox_pair_codes WHERE claim_id = ?)
         AND EXISTS (SELECT 1 FROM inbox_pair_codes WHERE claim_id = ?)`,
    ).bind(userId, claimId, claimId),
    env.DB.prepare(
      `UPDATE inbox_endpoints SET discord_user_id = ?, paired_at = ?, is_default = 1
       WHERE library_id = (SELECT library_id FROM inbox_pair_codes WHERE claim_id = ?)
         AND revoked_at IS NULL`,
    ).bind(userId, now, claimId),
    env.DB.prepare(
      `SELECT name FROM inbox_endpoints WHERE library_id =
       (SELECT library_id FROM inbox_pair_codes WHERE claim_id = ?)`,
    ).bind(claimId),
  ])
  const row = asRecord(result[3]?.results?.[0])
  if (typeof row?.name !== 'string')
    throw new InboxError(409, '配对码已使用或过期，请在资源库重新生成')
  return row.name
}

function canonicalCapture(value: unknown, field = ''): unknown {
  if (Array.isArray(value)) return value.map((item) => canonicalCapture(item))
  const object = asRecord(value)
  if (object)
    return Object.fromEntries(
      Object.keys(object)
        .sort()
        .map((key) => [key, canonicalCapture(object[key], key)]),
    )
  if (typeof value === 'string' && /^(url|proxyUrl|proxy_url)$/u.test(field)) {
    try {
      const url = new URL(value)
      if (
        url.protocol === 'https:' &&
        ['cdn.discordapp.com', 'media.discordapp.net'].includes(url.hostname)
      ) {
        for (const parameter of ['ex', 'is', 'hm']) url.searchParams.delete(parameter)
        return url.toString()
      }
    } catch {
      /* Keep invalid URL text as part of the snapshot identity. */
    }
  }
  return value
}

export async function inboxCaptureFingerprint(capture: Record<string, unknown>): Promise<string> {
  return sha256Hex(JSON.stringify(canonicalCapture(capture)))
}

export async function createInboxDelivery(
  env: Env,
  userId: string,
  capture: Record<string, unknown>,
  fingerprint: string,
  store: InboxHandoffStore,
  capturedAt: number,
  expectedLibraryId?: string,
): Promise<{ token: string; delivery: Delivery; paired: boolean }> {
  const endpoint = await env.DB.prepare(
    'SELECT library_id, name, discord_user_id, is_default FROM inbox_endpoints WHERE discord_user_id = ? AND revoked_at IS NULL AND is_default = 1',
  )
    .bind(userId)
    .first<Endpoint>()
  if (expectedLibraryId && endpoint?.library_id !== expectedLibraryId)
    throw new InboxError(409, '配对目标已改变，已停止投递，请重新执行。')
  const libraryId = endpoint?.library_id ?? null
  const scope = libraryId ?? `unpaired:${userId}`
  const now = Date.now()
  const existing = await env.DB.prepare(
    'SELECT * FROM inbox_deliveries WHERE dedupe_scope = ? AND fingerprint = ? AND expires_at > ?',
  )
    .bind(scope, fingerprint, now)
    .first<Delivery>()
  const id = existing?.id ?? crypto.randomUUID()
  const expiresAt =
    existing?.expires_at ?? capturedAt + (endpoint ? DELIVERY_TTL_MS : UNPAIRED_TTL_MS)
  // Repeated saves share a receipt and the original payload; this token is a temporary alias.
  const token = await store.create(env, existing ? { inboxDeliveryId: id } : capture, expiresAt)
  const tokenHash = await sha256Hex(token)
  const title = (
    asString(capture.title) ||
    asString(capture.content).split('\n')[0] ||
    'Discord 帖子'
  ).slice(0, 120)
  const targetUnchanged = `(? IS NULL AND NOT EXISTS
    (SELECT 1 FROM inbox_endpoints WHERE discord_user_id = ? AND revoked_at IS NULL AND is_default = 1))
    OR EXISTS (SELECT 1 FROM inbox_endpoints
      WHERE library_id = ? AND discord_user_id = ? AND revoked_at IS NULL AND is_default = 1)`
  const targetValues = [libraryId, userId, libraryId, userId] as const
  // Identity matches createDiscordSourceKey; only its digest is persisted in transport receipts.
  const container =
    asString(capture.threadId).trim() ||
    asString(capture.starterMessageId).trim() ||
    asString(capture.messageId).trim()
  const sourceKeyHash = await sha256Hex(
    `discord:${asString(capture.guildId).trim() || '@me'}:${container}`,
  )
  const result = await env.DB.batch([
    env.DB.prepare(
      'DELETE FROM inbox_deliveries WHERE dedupe_scope = ? AND fingerprint = ? AND expires_at <= ?',
    ).bind(scope, fingerprint, now),
    env.DB.prepare(
      `INSERT OR IGNORE INTO inbox_deliveries
       (id, handoff_token_hash, library_id, dedupe_scope, fingerprint, title, created_at, expires_at, source_key_hash)
       SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
       WHERE ${targetUnchanged}`,
    ).bind(
      id,
      tokenHash,
      libraryId,
      scope,
      fingerprint,
      title,
      capturedAt,
      expiresAt,
      sourceKeyHash,
      ...targetValues,
    ),
    env.DB.prepare(
      `INSERT INTO inbox_delivery_links (token_hash, delivery_id)
       SELECT ?, id FROM inbox_deliveries WHERE dedupe_scope = ? AND fingerprint = ? AND expires_at > ?
       AND (${targetUnchanged})`,
    ).bind(tokenHash, scope, fingerprint, now, ...targetValues),
    env.DB.prepare(
      `SELECT * FROM inbox_deliveries WHERE dedupe_scope = ? AND fingerprint = ? AND expires_at > ?
       AND (${targetUnchanged})`,
    ).bind(scope, fingerprint, now, ...targetValues),
  ])
  const delivery = result[3]?.results?.[0] as Delivery | undefined
  if (!delivery) throw new InboxError(409, '收件目标已改变，请重新保存帖子')
  return { token, delivery, paired: Boolean(endpoint) }
}

async function linkedDelivery(env: Env, token: string): Promise<Delivery | null> {
  if (!TOKEN_PATTERN.test(token)) throw new InboxError(400, 'invalid_token')
  return env.DB.prepare(
    `SELECT d.*, e.name AS library_name FROM inbox_delivery_links l
     JOIN inbox_deliveries d ON d.id = l.delivery_id
     LEFT JOIN inbox_endpoints e ON e.library_id = d.library_id WHERE l.token_hash = ?`,
  )
    .bind(await sha256Hex(token))
    .first<Delivery>()
}

async function deliveryPayload(
  env: Env,
  delivery: Delivery,
  store: InboxHandoffStore,
): Promise<Response> {
  if (delivery.expires_at <= Date.now())
    return json({ error: 'delivery_expired', state: 'expired' }, { status: 410 })
  if (delivery.state !== 'pending')
    return json({ error: 'delivery_already_saved', state: delivery.state }, { status: 409 })
  const handoff = await env.DB.prepare(
    'SELECT payload FROM handoffs WHERE token_hash = ? AND expires_at > ?',
  )
    .bind(delivery.handoff_token_hash, Date.now())
    .first<{ payload: string }>()
  if (!handoff) throw new InboxError(503, 'delivery_payload_unavailable')
  const payload = await store.read(env, delivery.handoff_token_hash, handoff.payload)
  if (payload === undefined) throw new InboxError(503, 'delivery_payload_unavailable')
  let capture: unknown
  try {
    capture = JSON.parse(payload)
  } catch {
    throw new InboxError(503, 'delivery_payload_invalid')
  }
  return json({
    capture,
    delivery: {
      id: delivery.id,
      libraryId: delivery.library_id,
      ...(delivery.claimed_library_id ? { claimedLibraryId: delivery.claimed_library_id } : {}),
      capturedAt: delivery.created_at,
    },
  })
}

function acknowledgedPayloadDeletion(
  env: Env,
  scope?: { deliveryId: string; libraryId: string; now: number },
): D1PreparedStatement {
  const statement = env.DB.prepare(
    `WITH acknowledged AS (
      SELECT id, handoff_token_hash FROM inbox_deliveries
      WHERE state IN ('saved', 'waiting_binding')
      ${
        scope
          ? `AND id = ? AND claimed_library_id = ? AND expires_at > ?
        AND (library_id IS NULL OR EXISTS (SELECT 1 FROM inbox_endpoints e
          WHERE e.library_id = inbox_deliveries.library_id AND e.revoked_at IS NULL))`
          : ''
      }
    ), payloads AS (
      SELECT handoff_token_hash AS token_hash FROM acknowledged
      UNION SELECT l.token_hash FROM inbox_delivery_links l JOIN acknowledged a ON a.id = l.delivery_id
    ) DELETE FROM handoffs
      WHERE token_hash IN (SELECT token_hash FROM payloads)
        OR (token_hash LIKE '%:chunk:%' AND
          substr(token_hash, 1, instr(token_hash, ':chunk:') - 1) IN (SELECT token_hash FROM payloads))`,
  )
  return scope ? statement.bind(scope.deliveryId, scope.libraryId, scope.now) : statement
}

function cleanupCompletedHandoffPayloads(env: Env, libraryId: string, now: number) {
  return env.DB.prepare(
    `WITH acknowledged AS (
      SELECT id, handoff_token_hash FROM inbox_deliveries
      WHERE claimed_library_id = ? AND state IN ('saved', 'waiting_binding') AND expires_at > ?
        AND (library_id IS NULL OR EXISTS (SELECT 1 FROM inbox_endpoints e
          WHERE e.library_id = inbox_deliveries.library_id AND e.revoked_at IS NULL))
    ), payloads AS (
      SELECT handoff_token_hash AS token_hash FROM acknowledged
      UNION SELECT l.token_hash FROM inbox_delivery_links l JOIN acknowledged a ON a.id = l.delivery_id
    ) DELETE FROM handoffs
      WHERE token_hash IN (SELECT token_hash FROM payloads)
        OR (token_hash LIKE '%:chunk:%' AND
          substr(token_hash, 1, instr(token_hash, ':chunk:') - 1) IN (SELECT token_hash FROM payloads))`,
  ).bind(libraryId, now)
}

function deletePendingHandoffPayload(env: Env, id: string, libraryId: string, now: number) {
  return env.DB.prepare(
    `WITH pending AS (
      SELECT id, handoff_token_hash FROM inbox_deliveries
      WHERE id = ? AND library_id = ? AND state = 'pending' AND expires_at > ?
    ), payloads AS (
      SELECT handoff_token_hash AS token_hash FROM pending
      UNION SELECT l.token_hash FROM inbox_delivery_links l JOIN pending p ON p.id = l.delivery_id
    ) DELETE FROM handoffs
      WHERE token_hash IN (SELECT token_hash FROM payloads)
        OR (token_hash LIKE '%:chunk:%' AND
          substr(token_hash, 1, instr(token_hash, ':chunk:') - 1) IN (SELECT token_hash FROM payloads))`,
  ).bind(id, libraryId, now)
}

async function acknowledge(
  env: Env,
  delivery: Delivery,
  libraryId: string,
  body: Record<string, unknown>,
): Promise<Response> {
  if (!LIBRARY_PATTERN.test(libraryId)) throw new InboxError(400, 'invalid_library_id')
  const state = body.state
  if (state !== 'saved' && state !== 'waiting_binding') throw new InboxError(400, 'invalid_state')
  const now = Date.now()
  // Local persistence was verified before ACK; purge body/chunks atomically with its receipt.
  const result = await env.DB.batch([
    env.DB.prepare(
      `UPDATE inbox_deliveries
     SET state = CASE WHEN state = 'saved' THEN 'saved' ELSE ? END,
         acknowledged_at = COALESCE(acknowledged_at, ?),
         claimed_library_id = COALESCE(claimed_library_id, ?)
     WHERE id = ? AND expires_at > ? AND
       (library_id = ? OR (library_id IS NULL AND (claimed_library_id IS NULL OR claimed_library_id = ?)))
       AND (library_id IS NULL OR EXISTS (SELECT 1 FROM inbox_endpoints e
         WHERE e.library_id = inbox_deliveries.library_id AND e.revoked_at IS NULL))`,
    ).bind(state, now, libraryId, delivery.id, now, libraryId, libraryId),
    acknowledgedPayloadDeletion(env, { deliveryId: delivery.id, libraryId, now }),
  ])
  if (!result[0]?.meta.changes) throw new InboxError(409, 'delivery_target_mismatch_or_expired')
  const receipt = await env.DB.prepare('SELECT state FROM inbox_deliveries WHERE id = ?')
    .bind(delivery.id)
    .first<{ state: DeliveryState }>()
  return json({ ok: true, state: receipt?.state })
}

export async function handleInboxHandoff(
  request: Request,
  env: Env,
  token: string,
  action: 'read' | 'ack' | 'status',
  store: InboxHandoffStore,
): Promise<Response | undefined> {
  try {
    const delivery = await linkedDelivery(env, token)
    if (!delivery) return undefined
    if (action === 'status')
      return json({
        state: delivery.expires_at <= Date.now() ? 'expired' : delivery.state,
        ...(delivery.library_name ? { libraryName: delivery.library_name } : {}),
        createdAt: delivery.created_at,
        expiresAt: delivery.expires_at,
      })
    if (delivery.library_id) {
      const endpoint = await authenticatedEndpoint(request, env)
      if (endpoint.library_id !== delivery.library_id)
        throw new InboxError(409, 'delivery_target_mismatch')
    }
    if (action === 'read') return await deliveryPayload(env, delivery, store)
    const body = await requestBody(request)
    return await acknowledge(env, delivery, asString(body.libraryId), body)
  } catch (error) {
    return inboxErrorResponse(error)
  }
}

export async function cleanupInbox(env: Env, now: number): Promise<void> {
  await env.DB.batch([
    env.DB.prepare('DELETE FROM inbox_pair_codes WHERE expires_at <= ?').bind(now),
    env.DB.prepare('DELETE FROM inbox_deliveries WHERE expires_at <= ?').bind(now),
  ])
  await acknowledgedPayloadDeletion(env).run()
}

function listItem(delivery: Delivery) {
  return {
    id: delivery.id,
    createdAt: delivery.created_at,
    state: delivery.state,
    ...(delivery.title ? { title: delivery.title } : {}),
  }
}

export function inboxErrorResponse(error: unknown): Response {
  if (error instanceof InboxError) return json({ error: error.message }, { status: error.status })
  console.error('Discord inbox request failed')
  return json({ error: 'inbox_unavailable' }, { status: 503 })
}

export async function handleInboxRequest(
  request: Request,
  env: Env,
  store: InboxHandoffStore,
): Promise<Response> {
  const path = new URL(request.url).pathname
  try {
    if (path === '/inbox/pair' && request.method === 'POST') {
      if (!authorizedSetup(request, env)) throw new InboxError(401, 'unauthorized')
      const body = await requestBody(request)
      const name = asString(body.name).trim()
      if (!name || name.length > 80) throw new InboxError(400, 'invalid_name')
      const libraryId = crypto.randomUUID()
      const secret = bytesToBase64Url(crypto.getRandomValues(new Uint8Array(32)))
      const code = bytesToBase64Url(crypto.getRandomValues(new Uint8Array(12)))
      const now = Date.now()
      const expiresAt = now + PAIR_TTL_MS
      await env.DB.batch([
        env.DB.prepare(
          'INSERT INTO inbox_endpoints (library_id, secret_hash, name, created_at) VALUES (?, ?, ?, ?)',
        ).bind(libraryId, await sha256Hex(secret), name, now),
        env.DB.prepare(
          'INSERT INTO inbox_pair_codes (code_hash, library_id, expires_at) VALUES (?, ?, ?)',
        ).bind(await sha256Hex(code), libraryId, expiresAt),
      ])
      return json({ libraryId, secret, code, expiresAt })
    }
    const endpoint = await authenticatedEndpoint(request, env)
    if (path === '/inbox/status' && request.method === 'GET')
      return json({
        libraryId: endpoint.library_id,
        name: endpoint.name,
        paired: Boolean(endpoint.discord_user_id),
        isDefault: endpoint.is_default === 1,
        expiresInDays: 7,
      })
    if (path === '/inbox/pair' && request.method === 'DELETE') {
      await env.DB.prepare(
        'UPDATE inbox_endpoints SET revoked_at = ? WHERE library_id = ? AND revoked_at IS NULL',
      )
        .bind(Date.now(), endpoint.library_id)
        .run()
      return json({ ok: true })
    }
    if (
      (path === '/inbox/cleanup' || path === '/inbox/cleanup-scoped') &&
      request.method === 'DELETE'
    ) {
      const scoped = path === '/inbox/cleanup-scoped'
      const body = scoped ? await requestBody(request) : undefined
      const scope = scoped ? body?.scope : 'both'
      if (scope !== 'posts' && scope !== 'resources' && scope !== 'both')
        throw new InboxError(400, 'invalid_cleanup_scope')
      const now = Date.now()
      const statements: D1PreparedStatement[] = []
      if (scope === 'posts' || scope === 'both') {
        statements.push(
          cleanupCompletedHandoffPayloads(env, endpoint.library_id, now),
          env.DB.prepare(
            `DELETE FROM inbox_deliveries WHERE claimed_library_id = ? AND state IN ('saved','waiting_binding') AND expires_at > ?
           AND (library_id IS NULL OR EXISTS (SELECT 1 FROM inbox_endpoints e
             WHERE e.library_id = inbox_deliveries.library_id AND e.revoked_at IS NULL))`,
          ).bind(endpoint.library_id, now),
        )
      }
      if (scope === 'resources' || scope === 'both') {
        statements.push(
          env.DB.prepare(
            `DELETE FROM inbox_resources WHERE library_id = ? AND expires_at > ?
           AND state = 'imported'`,
          ).bind(endpoint.library_id, now),
        )
      }
      const results = await env.DB.batch(statements)
      const includesPosts = scope === 'posts' || scope === 'both'
      const includesResources = scope === 'resources' || scope === 'both'
      return json({
        ok: true,
        posts: includesPosts ? (results[1]?.meta.changes ?? 0) : 0,
        resources: includesResources ? (results[includesPosts ? 2 : 0]?.meta.changes ?? 0) : 0,
        payloadRows: includesPosts ? (results[0]?.meta.changes ?? 0) : 0,
      })
    }
    if (path === '/inbox/jobs' && request.method === 'GET') {
      const now = Date.now()
      const [pending, recent] = await env.DB.batch<Delivery>([
        env.DB.prepare(
          "SELECT * FROM inbox_deliveries WHERE library_id = ? AND state = 'pending' AND expires_at > ? ORDER BY created_at ASC, id ASC LIMIT ?",
        ).bind(endpoint.library_id, now, PAGE_SIZE + 1),
        env.DB.prepare(
          "SELECT * FROM inbox_deliveries WHERE library_id = ? AND state <> 'pending' AND expires_at > ? ORDER BY acknowledged_at DESC, id DESC LIMIT ?",
        ).bind(endpoint.library_id, now, PAGE_SIZE),
      ])
      const jobs = pending.results ?? []
      return json({
        jobs: jobs.slice(0, PAGE_SIZE).map(listItem),
        recent: (recent.results ?? []).map(listItem),
        hasMore: jobs.length > PAGE_SIZE,
      })
    }
    if (path === '/inbox/waiting-sources' && request.method === 'GET') {
      const after = new URL(request.url).searchParams.get('after') ?? ''
      if (after && !/^[a-f0-9]{64}$/u.test(after)) throw new InboxError(400, 'invalid_cursor')
      const result = await env.DB.prepare(
        `SELECT DISTINCT source_key_hash FROM inbox_deliveries
         WHERE library_id = ? AND state = 'waiting_binding' AND expires_at > ? AND source_key_hash > ?
         ORDER BY source_key_hash ASC LIMIT ?`,
      )
        .bind(endpoint.library_id, Date.now(), after, PAGE_SIZE + 1)
        .all<{ source_key_hash: string }>()
      const sourceKeyHashes = (result.results ?? [])
        .slice(0, PAGE_SIZE)
        .map((row) => row.source_key_hash)
      return json({
        sourceKeyHashes,
        nextCursor: (result.results?.length ?? 0) > PAGE_SIZE ? sourceKeyHashes.at(-1) : null,
      })
    }
    const boundSource = /^\/inbox\/sources\/([a-f0-9]{64})\/ack-bound$/u.exec(path)
    if (boundSource && request.method === 'POST') {
      const updated = await env.DB.prepare(
        `UPDATE inbox_deliveries SET state = 'saved', acknowledged_at = ?
         WHERE library_id = ? AND source_key_hash = ? AND state = 'waiting_binding' AND expires_at > ?
           AND EXISTS (SELECT 1 FROM inbox_endpoints WHERE library_id = ? AND revoked_at IS NULL)`,
      )
        .bind(Date.now(), endpoint.library_id, boundSource[1], Date.now(), endpoint.library_id)
        .run()
      return json({ ok: true, state: 'saved', updated: updated.meta.changes ?? 0 })
    }
    const match = /^\/inbox\/jobs\/([A-Za-z0-9_-]{8,100})(\/ack)?$/u.exec(path)
    if (match) {
      const delivery = await env.DB.prepare(
        'SELECT * FROM inbox_deliveries WHERE id = ? AND library_id = ? AND expires_at > ?',
      )
        .bind(match[1], endpoint.library_id, Date.now())
        .first<Delivery>()
      if (!delivery) throw new InboxError(404, 'delivery_not_found_or_expired')
      if (!match[2] && request.method === 'GET') return await deliveryPayload(env, delivery, store)
      if (match[2] && request.method === 'POST')
        return await acknowledge(env, delivery, endpoint.library_id, await requestBody(request))
      if (!match[2] && request.method === 'DELETE') {
        if (delivery.state !== 'pending' || delivery.library_id !== endpoint.library_id)
          throw new InboxError(409, 'delivery_no_longer_cancellable')
        const now = Date.now()
        const results = await env.DB.batch([
          deletePendingHandoffPayload(env, delivery.id, endpoint.library_id, now),
          env.DB.prepare(
            `DELETE FROM inbox_deliveries WHERE id = ? AND library_id = ? AND state = 'pending' AND expires_at > ?`,
          ).bind(delivery.id, endpoint.library_id, now),
        ])
        if (!results[1]?.meta.changes) throw new InboxError(409, 'delivery_changed_before_cancel')
        return json({ ok: true })
      }
    }
    return json({ error: 'not_found' }, { status: 404 })
  } catch (error) {
    return inboxErrorResponse(error)
  }
}
