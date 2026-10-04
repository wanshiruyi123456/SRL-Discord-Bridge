/* global Request */

import {
  addTextAttachmentContent,
  asNumber,
  asRecord,
  asString,
  buildCapture,
  hasTextAttachments,
} from './DiscordSourceCapture'
import { DiscordInteraction, Env } from './DiscordSourceProtocol'
import {
  cleanupInbox,
  InboxError,
  createInboxDelivery,
  handleInboxHandoff,
  handleInboxRequest,
  inboxCaptureFingerprint,
  interactionUserId,
  pairDiscordUser,
} from './DiscordInbox'
import { discordJson, handleSavedMessageCheck, handleSourceRead } from './DiscordSourceReader'
import {
  cleanupResources,
  createResourceJobs,
  handleResourceRequest,
  queuePastedResourceLinks,
} from './DiscordResources'
import {
  CORS_HEADERS,
  authorizedSetup,
  bytesToBase64Url,
  escapeHtml,
  html,
  json,
  sha256Hex,
  verifyDiscordRequest,
} from './DiscordWorkerHttp'

const COMMAND_NAME = '保存到资源库'
const POST_COMMAND_NAME = '保存帖子到SRL（云端暂存）'
const RESOURCE_COMMAND_NAME = '下载资源到SRL（云端暂存）'
const DIRECT_RESOURCE_COMMAND_NAME = '下载直链'
const PASTE_INBOX_COMMAND_NAME = '粘贴收件'
const PAIR_COMMAND_NAME = '绑定资源库'
const INLINE_HANDOFF_MAX_BYTES = 1_800_000
const HANDOFF_CHUNK_CHARACTERS = 250_000
const HANDOFF_CHUNK_BATCH_SIZE = 20
const HANDOFF_CHUNK_MARKER = /^srl-chunks-v1:(\d+)$/u

function handoffTtlSeconds(env: Env): number {
  const configured = Number(env.HANDOFF_TTL_SECONDS)
  return Number.isFinite(configured)
    ? Math.min(1_800, Math.max(300, Math.round(configured)))
    : 1_200
}

async function cleanupExpired(env: Env): Promise<void> {
  const now = Date.now()
  const results = await Promise.allSettled([
    env.DB.batch([
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
    ]),
    cleanupInbox(env, now),
    cleanupResources(env, now),
  ])
  const failures = results.filter((result) => result.status === 'rejected')
  if (failures.length)
    throw new AggregateError(
      failures.map((failure) => failure.reason),
      'Discord transport cleanup incomplete',
    )
}

function splitHandoffPayload(payload: string): string[] {
  const chunks: string[] = []
  for (let start = 0; start < payload.length;) {
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

async function createHandoff(env: Env, payload: unknown, expiration?: number): Promise<string> {
  const token = bytesToBase64Url(crypto.getRandomValues(new Uint8Array(32)))
  const tokenHash = await sha256Hex(token)
  const now = Date.now()
  const expiresAt = expiration ?? now + handoffTtlSeconds(env) * 1_000
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

async function commandRegistrationFailure(response: Response, name: string): Promise<Error> {
  const body = asRecord(await response.json().catch(() => undefined))
  const code = asNumber(body?.code)
  const retryAfter = asNumber(body?.retry_after)
  const errors = asRecord(body?.errors)
  const fields = ['name', 'description', 'options', 'integration_types', 'contexts'].filter(
    (field) => errors?.[field] !== undefined,
  )
  // Fixed command names, numeric codes and known field names only; never upstream text or secrets.
  console.warn('Discord command registration rejected', {
    command: name,
    status: response.status,
    code,
    fields,
    retryAfter,
  })
  return new Error(
    `Discord 指令「${name}」注册失败（HTTP ${response.status}${code !== undefined ? `，错误码 ${code}` : ''}${fields.length ? `，字段 ${fields.join('、')}` : ''}）${response.status === 429 ? `；请${retryAfter !== undefined ? `等待 ${Math.ceil(retryAfter)} 秒后` : '稍后'}再注册` : ''}`,
  )
}

function commandMatches(actual: unknown, expected: unknown): boolean {
  if (Array.isArray(expected))
    return (
      Array.isArray(actual) &&
      actual.length === expected.length &&
      expected.every((item, index) => commandMatches(actual[index], item))
    )
  const record = asRecord(expected)
  if (record) {
    const candidate = asRecord(actual)
    return Boolean(
      candidate &&
      Object.entries(record).every(([key, value]) => commandMatches(candidate[key], value)),
    )
  }
  return actual === expected
}

async function writeDiscordCommand(url: string, init: RequestInit): Promise<Response> {
  let waited = 0
  for (let attempt = 0; ; attempt += 1) {
    const response = await fetch(url, init)
    if (response.status !== 429 || attempt >= 2) return response
    const body = asRecord(
      await response
        .clone()
        .json()
        .catch(() => undefined),
    )
    const seconds = asNumber(body?.retry_after) ?? Number(response.headers.get('Retry-After'))
    const milliseconds = Math.ceil(seconds * 1000)
    // Existing SRL registration UI aborts at 8 seconds; never hold it for a long rate limit.
    if (!Number.isFinite(milliseconds) || milliseconds <= 0 || waited + milliseconds > 4000)
      return response
    waited += milliseconds
    await response.body?.cancel()
    await new Promise<void>((resolve) => setTimeout(resolve, milliseconds))
  }
}

async function registerMessageCommand(env: Env): Promise<void> {
  if (!env.DISCORD_APPLICATION_ID || !env.DISCORD_BOT_TOKEN) {
    throw new Error('Discord Application ID / Bot Token 未配置')
  }
  const existingResponse = await fetch(discordApplicationCommandsUrl(env), {
    headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
  })
  const existing = await discordJson(existingResponse, 'Discord command migration read')
  if (!Array.isArray(existing)) throw new Error('Discord command list invalid')
  const renamedCommands = new Map([
    [POST_COMMAND_NAME, '保存帖子到SRL'],
    [RESOURCE_COMMAND_NAME, '下载资源到SRL'],
  ])
  const commands = [
    {
      name: PASTE_INBOX_COMMAND_NAME,
      type: 1,
      description: '从粘贴的 Discord 消息正文提取附件并暂存到资源库',
      contexts: [0, 1, 2],
      options: [
        {
          name: '正文内容',
          type: 3,
          description: '粘贴复制的消息正文，最多 4000 字符',
          required: true,
          max_length: 4000,
        },
      ],
    },
    { name: COMMAND_NAME, type: 3 },
    { name: POST_COMMAND_NAME, type: 3 },
    { name: RESOURCE_COMMAND_NAME, type: 3 },
    {
      name: DIRECT_RESOURCE_COMMAND_NAME,
      type: 1,
      description: '将 Discord 附件直链云端暂存到已配对资源库，不保存帖子',
      options: [
        {
          name: '链接',
          type: 3,
          description: 'Discord 文件下载直链，不是消息地址',
          required: true,
          max_length: 4096,
        },
      ],
    },
    {
      name: PAIR_COMMAND_NAME,
      type: 1,
      description: '将帖子和资源下载任务投递到当前资源库',
      options: [{ name: 'code', type: 3, description: '资源库生成的一次性配对码', required: true }],
    },
  ]
  for (const command of commands) {
    const previousName = renamedCommands.get(command.name)
    const oldCommand = existing
      .map(asRecord)
      .find(
        (item) =>
          previousName !== undefined &&
          item?.name === previousName &&
          item?.type === command.type &&
          typeof item?.id === 'string',
      )
    const alreadyRenamed = existing.some((item) => {
      const value = asRecord(item)
      return value?.name === command.name && value?.type === command.type
    })
    const migrate = oldCommand && !alreadyRenamed
    const payload = {
      ...command,
      integration_types: [1],
      contexts: command.contexts ?? [0, 1, 2],
    }
    const current = existing
      .map(asRecord)
      .find((item) => item?.name === command.name && item?.type === command.type)
    if (commandMatches(current, payload) && !oldCommand) continue
    const response = await writeDiscordCommand(
      discordApplicationCommandsUrl(env) +
        (migrate ? '/' + encodeURIComponent(asString(oldCommand.id)) : ''),
      {
        method: migrate ? 'PATCH' : 'POST',
        headers: {
          Authorization: `Bot ${env.DISCORD_BOT_TOKEN}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      },
    )
    if (!response.ok) {
      throw await commandRegistrationFailure(response, command.name)
    }
    if (oldCommand && alreadyRenamed) {
      const removed = await fetch(
        discordApplicationCommandsUrl(env) + '/' + encodeURIComponent(asString(oldCommand.id)),
        {
          method: 'DELETE',
          headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
        },
      )
      if (!removed.ok && removed.status !== 404)
        throw new Error(`Discord old command removal failed: ${removed.status}`)
    }
  }
  const removedCommands = new Set(['保存首楼帖子', '保存所有已标注信息'])
  for (const oldCommand of existing.map(asRecord)) {
    const id = asString(oldCommand?.id)
    if (!id || oldCommand?.type !== 1 || !removedCommands.has(asString(oldCommand?.name))) continue
    const response = await fetch(
      discordApplicationCommandsUrl(env) + '/' + encodeURIComponent(id),
      {
        method: 'DELETE',
        headers: { Authorization: `Bot ${env.DISCORD_BOT_TOKEN}` },
      },
    )
    if (!response.ok && response.status !== 404)
      throw new Error(`Discord obsolete command removal failed: ${response.status}`)
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
  return [
    [COMMAND_NAME, 3],
    [POST_COMMAND_NAME, 3],
    [RESOURCE_COMMAND_NAME, 3],
    [DIRECT_RESOURCE_COMMAND_NAME, 1],
    [PAIR_COMMAND_NAME, 1],
    [PASTE_INBOX_COMMAND_NAME, 1],
  ].every(([name, type]) =>
    payload.some((item) => {
      const command = asRecord(item)
      return asString(command?.name) === name && asNumber(command?.type) === type
    }),
  )
}

const inboxStore = { create: createHandoff, read: readHandoffPayload }

async function finishInboxCommand(
  interaction: DiscordInteraction,
  env: Env,
  requestUrl: string,
  capturedAt: number,
  capture?: Record<string, unknown>,
): Promise<void> {
  try {
    if (!capture) {
      const name = await pairDiscordUser(interaction, env)
      await updateDeferredInteraction(
        interaction,
        env,
        `已绑定到「${name}」。以后使用“${POST_COMMAND_NAME}”会投递到这个资源库；原设备仍可领取之前的帖子。`,
      )
      return
    }
    const userId = interactionUserId(interaction)
    if (!userId) throw new Error('无法确认执行命令的 Discord 用户')
    const fingerprint = await inboxCaptureFingerprint(capture)
    const enriched = await addTextAttachmentContent(capture)
    const result = await createInboxDelivery(
      env,
      userId,
      enriched,
      fingerprint,
      inboxStore,
      capturedAt,
    )
    const state =
      result.delivery.state === 'saved'
        ? '这份帖子已保存到资源库。'
        : result.delivery.state === 'waiting_binding'
          ? '这份帖子已保存，等待关联资源。'
          : result.paired
            ? '帖子已投递，等待资源库上线领取；云端暂存 7 天。'
            : '帖子已临时接收。尚未绑定资源库，请先生成配对码并执行 /绑定资源库；也可通过下方链接手动领取，链接 20 分钟后过期。'
    await updateDeferredInteraction(
      interaction,
      env,
      state,
      `${new URL(requestUrl).origin}/open/${encodeURIComponent(result.token)}`,
    )
  } catch (error) {
    console.error('Discord inbox command failed')
    await updateDeferredInteraction(
      interaction,
      env,
      `操作失败：${error instanceof Error ? error.message : '请稍后重试'}`,
    )
  }
}

async function finishPastedInboxCommand(interaction: DiscordInteraction, env: Env): Promise<void> {
  try {
    const userId = interactionUserId(interaction)
    const content = interaction.data?.options?.find(
      (option) => option.name === '正文内容' && option.type === 3,
    )?.value
    if (!userId || typeof content !== 'string' || !content.trim() || content.length > 4_000)
      throw new InboxError(400, '请粘贴 1–4000 字符的消息正文。')
    const queued = await queuePastedResourceLinks(interaction, env, content)
    if (!queued.count)
      throw new InboxError(
        400,
        '正文中没有找到受支持的 Discord 附件直链；这条指令只下载附件，不保存帖子正文。',
      )
    await updateDeferredInteraction(interaction, env, queued.message)
  } catch (error) {
    await updateDeferredInteraction(
      interaction,
      env,
      `附件解析失败：${error instanceof Error ? error.message : '请稍后重试'}`,
    )
  }
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
    const attachments = Array.isArray(enrichedCapture.attachments)
      ? enrichedCapture.attachments
      : []
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

function openPage(request: Request, token: string): Response {
  if (!/^[A-Za-z0-9_-]{30,160}$/u.test(token)) return html('<h1>链接无效</h1>', { status: 400 })
  const origin = new URL(request.url).origin
  const nativeUrl = `srl://discord-source?worker=${encodeURIComponent(origin)}&token=${encodeURIComponent(token)}`
  const handoffUrl = `${origin}/open/${encodeURIComponent(token)}`
  return html(`<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="color-scheme" content="light dark"><title>接收 Discord 帖子 · SRL</title>
<style>
:root{color-scheme:light dark;--bg:#f3f6f4;--surface:#fff;--ink:#233b37;--muted:#64766f;--line:#dbe5df;--tint:#eef5f0;--accent:#28694e;--on-accent:#fff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:"PingFang SC","Microsoft YaHei",sans-serif;font-size:15px;line-height:1.6}
main{width:100%;max-width:480px;margin:0 auto;padding:calc(36px + env(safe-area-inset-top)) max(20px,env(safe-area-inset-right)) calc(28px + env(safe-area-inset-bottom)) max(20px,env(safe-area-inset-left))}
.brand{display:flex;align-items:center;gap:8px;color:var(--muted);font-size:12px;letter-spacing:.12em}.brand b{letter-spacing:0;color:var(--accent);font-size:14px}.brand i{width:1px;height:12px;background:var(--line)}
h1{margin:25px 0 8px;font-size:27px;line-height:1.35;letter-spacing:-.03em}.intro{margin:0 0 24px;color:var(--muted);font-size:14px}
.receipt{padding:16px 18px;border:1px solid var(--line);border-radius:16px;background:var(--surface)}.receipt-top{display:flex;align-items:center;justify-content:space-between;gap:8px}.status-label{display:flex;align-items:center;gap:8px;font-weight:600;font-size:13px}.status-dot{width:7px;height:7px;flex:none;border-radius:50%;background:var(--accent)}
#delivery-state{margin:4px 0 0;font-size:14px;overflow-wrap:anywhere}.refresh{display:inline-flex;align-items:center;justify-content:center;gap:5px;min-height:44px;padding:0 8px;margin:-6px -8px -6px 0;border:0;background:none;color:var(--muted);font:inherit;font-size:12px;cursor:pointer;flex:none}
.actions{margin-top:24px;display:grid;gap:10px}.action{display:flex;align-items:center;justify-content:space-between;gap:12px;width:100%;min-width:0;min-height:56px;padding:14px 18px;border:1px solid var(--line);border-radius:12px;background:var(--surface);color:var(--ink);text-decoration:none;font:inherit;font-weight:600;text-align:left;white-space:normal;overflow-wrap:anywhere;cursor:pointer}.action span{min-width:0}.action svg,.refresh svg{flex:none;width:18px;height:18px}.primary{background:var(--accent);color:var(--on-accent);border-color:var(--accent)}.action:hover{filter:brightness(.97)}.action:focus-visible,.refresh:focus-visible,input:focus-visible{outline:3px solid var(--accent);outline-offset:3px}button:disabled{opacity:.55;cursor:wait}
.web-help{margin:2px 3px 0;font-size:12px;color:var(--muted)}.handoff-fallback{width:100%;min-width:0;padding:12px;border:1px solid var(--line);border-radius:8px;background:var(--surface);color:var(--ink);font:inherit;font-size:13px}.handoff-fallback[hidden]{display:none}.note{margin:24px 0 0;padding-top:16px;border-top:1px solid var(--line);font-size:12px;color:var(--muted);overflow-wrap:anywhere}
@media(prefers-color-scheme:dark){:root{--bg:#18211e;--surface:#222e29;--ink:#e6ede8;--muted:#a1b4a8;--line:#35473c;--tint:#293c30;--accent:#9fcab1;--on-accent:#172b20}}
</style></head>
<body><main>
<div class="brand"><b>SRL</b><i aria-hidden="true"></i><span>DISCORD 帖子接收</span></div>
<h1 id="delivery-heading">帖子已暂存</h1><p class="intro">打开资源库，接着整理这条帖子。</p>
<section class="receipt" aria-label="接收进度"><div class="receipt-top"><span class="status-label"><i class="status-dot" aria-hidden="true"></i>接收进度</span><button class="refresh" id="refresh-state" type="button" aria-label="刷新接收进度"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M20 10a8 8 0 0 0-14-5L3 8m0-5v5h5M4 14a8 8 0 0 0 14 5l3-3m0 5v-5h-5"/></svg>刷新</button></div><p id="delivery-state" role="status" aria-live="polite">正在读取接收进度…</p></section>
<div class="actions">
<a class="action primary" id="open-native" href="${escapeHtml(nativeUrl)}"><span>打开安卓 App</span><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="m9 5 7 7-7 7"/></svg></a>
<button class="action" id="copy-handoff" type="button" data-handoff-url="${escapeHtml(handoffUrl)}"><span>复制领取链接</span><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><rect x="8" y="8" width="12" height="12" rx="2"/><path d="M15 8V4H4v11h4"/></svg></button>
<p class="web-help">网页 / iOS PWA：复制后回资源库，在连接设置中粘贴领取。</p>
<input class="handoff-fallback" id="handoff-fallback" type="url" value="${escapeHtml(handoffUrl)}" readonly aria-label="临时领取链接" hidden>
</div><p class="note" id="copy-status" role="status">临时链接会自动过期。帖子在本机保存成功后确认接收；原直传链接仅能领取一次。</p>
</main>
<script>
async function updateDeliveryState() {
  const target = document.getElementById('delivery-state');
  const button = document.getElementById('refresh-state');
  if (button) button.disabled = true;
  try {
    const response = await fetch('/handoff/${encodeURIComponent(token)}/status', { cache: 'no-store', credentials: 'omit' });
    const state = await response.json();
    const labels = {
      pending: '云端已接收，等待资源库保存。',
      saved: '帖子已保存到资源库。',
      waiting_binding: '帖子已保存，等待你关联到资源。',
      expired: '临时链接已过期。请回 Discord 重新保存这条消息。'
    };
    const heading = document.getElementById('delivery-heading');
    if (heading) heading.textContent = ({ pending: '帖子已暂存', saved: '帖子已保存', waiting_binding: '等待关联资源', expired: '链接已过期' })[state.state] || '接收进度';
    if (target) target.textContent = (state.libraryName ? '目标：' + state.libraryName + '。' : '') + (labels[state.state] || '暂时无法读取状态，请稍后刷新。');
  } catch {
    if (target) target.textContent = '暂时无法读取状态，请稍后刷新。';
  } finally { if (button) button.disabled = false; }
}
document.getElementById('refresh-state')?.addEventListener('click', updateDeliveryState);
void updateDeliveryState();
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
    if (status) status.textContent = '已复制。请切回 SRL 网页 / PWA，打开收件箱的连接设置并粘贴领取。';
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

  const commandName = interaction.data?.name
  if (
    interaction.type === 2 &&
    interaction.data?.type === 1 &&
    commandName === PASTE_INBOX_COMMAND_NAME
  ) {
    if (!interaction.token || !interactionUserId(interaction))
      return json({
        type: 4,
        data: { content: '无法确认 Discord 命令身份，请重新操作。', flags: 64 },
      })
    ctx.waitUntil(finishPastedInboxCommand(interaction, env))
    ctx.waitUntil(cleanupExpired(env).catch(() => console.error('Discord resource cleanup failed')))
    return json({ type: 5, data: { flags: 64 } })
  }
  const isPairCommand =
    interaction.type === 2 && interaction.data?.type === 1 && commandName === PAIR_COMMAND_NAME
  const isPostCommand =
    interaction.type === 2 &&
    interaction.data?.type === 3 &&
    (commandName === POST_COMMAND_NAME || commandName === '保存帖子到SRL')
  if (
    interaction.type === 2 &&
    ((interaction.data?.type === 3 &&
      (commandName === RESOURCE_COMMAND_NAME || commandName === '下载资源到SRL')) ||
      (interaction.data?.type === 1 && commandName === DIRECT_RESOURCE_COMMAND_NAME))
  ) {
    ctx.waitUntil(
      (async () => {
        try {
          const content = await createResourceJobs(interaction, env)
          await updateDeferredInteraction(interaction, env, content)
        } catch (error) {
          await updateDeferredInteraction(
            interaction,
            env,
            error instanceof Error ? error.message : '资源下载任务创建失败，请稍后重试',
          )
        }
      })(),
    )
    ctx.waitUntil(cleanupExpired(env).catch(() => console.error('Discord resource cleanup failed')))
    return json({ type: 5, data: { flags: 64 } })
  }
  if (isPairCommand || isPostCommand) {
    try {
      const capture = isPostCommand ? buildCapture(interaction) : undefined
      const capturedAt = Date.now()
      if (!interaction.token || !interactionUserId(interaction))
        throw new Error('无法确认 Discord 命令身份')
      ctx.waitUntil(finishInboxCommand(interaction, env, request.url, capturedAt, capture))
      ctx.waitUntil(cleanupExpired(env).catch(() => console.error('Discord inbox cleanup failed')))
      return json({ type: 5, data: { flags: 64 } })
    } catch {
      return json({
        type: 4,
        data: { content: 'Discord 未提供完整的命令身份或目标消息，请重新操作。', flags: 64 },
      })
    }
  }

  if (interaction.type !== 2 || interaction.data?.type !== 3 || commandName !== COMMAND_NAME) {
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
  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    await cleanupExpired(env)
  },
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
          postCommandName: POST_COMMAND_NAME,
          resourceCommandName: RESOURCE_COMMAND_NAME,
          directResourceCommandName: DIRECT_RESOURCE_COMMAND_NAME,
          pairCommandName: PAIR_COMMAND_NAME,
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

    if (url.pathname === '/inbox/resources' || url.pathname.startsWith('/inbox/resources/')) {
      return handleResourceRequest(request, env)
    }
    if (url.pathname.startsWith('/inbox/')) {
      const response = await handleInboxRequest(request, env, inboxStore)
      if (response.ok)
        ctx.waitUntil(
          cleanupExpired(env).catch(() => console.error('Discord inbox cleanup failed')),
        )
      return response
    }

    if (url.pathname.startsWith('/handoff/') && request.method === 'GET') {
      const match = /^\/handoff\/([A-Za-z0-9_-]{30,160})(\/status)?$/u.exec(url.pathname)
      if (!match) return json({ error: 'invalid_token' }, { status: 400 })
      const token = match[1]!
      const action = match[2] ? 'status' : 'read'
      let response = await handleInboxHandoff(request, env, token, action, inboxStore)
      if (!response && action === 'status') {
        const row = await env.DB.prepare(
          'SELECT created_at, expires_at, consumed_at FROM handoffs WHERE token_hash = ?',
        )
          .bind(await sha256Hex(token))
          .first<{ created_at: number; expires_at: number; consumed_at: number | null }>()
        response = json(
          row
            ? {
                state:
                  row.expires_at <= Date.now()
                    ? 'expired'
                    : row.consumed_at === null
                      ? 'pending'
                      : 'saved',
                createdAt: row.created_at,
                expiresAt: row.expires_at,
              }
            : { state: 'expired' },
        )
      }
      response ??= await consumeHandoff(env, token)
      ctx.waitUntil(cleanupExpired(env).catch((error) => console.error(error)))
      return response
    }

    if (request.method === 'POST') {
      const match = /^\/handoff\/([A-Za-z0-9_-]{30,160})\/ack$/u.exec(url.pathname)
      if (match)
        return (
          (await handleInboxHandoff(request, env, match[1]!, 'ack', inboxStore)) ??
          json({ error: 'delivery_not_found_or_expired' }, { status: 404 })
        )
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
