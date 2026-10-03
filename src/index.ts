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
  createInboxDelivery,
  handleInboxHandoff,
  handleInboxRequest,
  inboxCaptureFingerprint,
  interactionUserId,
  pairDiscordUser,
} from './DiscordInbox'
import { discordJson, handleSavedMessageCheck, handleSourceRead } from './DiscordSourceReader'
import { cleanupResources, createResourceJobs, handleResourceRequest } from './DiscordResources'
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
const POST_COMMAND_NAME = '保存帖子到SRL'
const RESOURCE_COMMAND_NAME = '下载资源到SRL'
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

async function registerMessageCommand(env: Env): Promise<void> {
  if (!env.DISCORD_APPLICATION_ID || !env.DISCORD_BOT_TOKEN) {
    throw new Error('Discord Application ID / Bot Token 未配置')
  }
  const commands = [
    { name: COMMAND_NAME, type: 3 },
    { name: POST_COMMAND_NAME, type: 3 },
    { name: RESOURCE_COMMAND_NAME, type: 3 },
    {
      name: PAIR_COMMAND_NAME,
      type: 1,
      description: '将帖子和资源下载任务投递到当前资源库',
      options: [{ name: 'code', type: 3, description: '资源库生成的一次性配对码', required: true }],
    },
  ]
  for (const command of commands) {
    const response = await fetch(discordApplicationCommandsUrl(env), {
      method: 'POST',
      headers: {
        Authorization: `Bot ${env.DISCORD_BOT_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        ...command,
        integration_types: [1],
        contexts: [0, 1, 2],
      }),
    })
    if (!response.ok) {
      throw new Error(`Discord command registration failed: ${response.status}`)
    }
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
    [PAIR_COMMAND_NAME, 1],
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
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>打开 SRL</title>
<style>body{font-family:system-ui,sans-serif;max-width:560px;margin:0 auto;padding:32px 20px;color:#173641;background:#f5fafb}h1{font-size:1.35rem}p{color:#647b83;line-height:1.65}.a{display:block;width:100%;margin-top:12px;padding:12px 14px;border:1px solid #bfd0d4;border-radius:8px;color:#315e6d;text-decoration:none;font:inherit;font-weight:700;text-align:left;cursor:pointer}.hint{font-size:.82rem;color:#82949b}.handoff-fallback[hidden]{display:none}</style></head>
<body><h1 id="delivery-heading">Discord 来源已接收</h1><p id="delivery-state" role="status">消息正在你自己的 Worker 中临时等待领取。请复制临时链接，回到正在使用的 SRL 网页 / PWA 粘贴领取，内容会保存到当前应用的资源库。</p>
<button class="a" id="refresh-state" type="button">刷新接收进度</button>
<a class="a" href="${escapeHtml(nativeUrl)}">打开 SRL Android App</a>
<button class="a" id="copy-handoff" type="button" data-handoff-url="${escapeHtml(handoffUrl)}">复制临时链接，回网页 / PWA 粘贴领取</button>
<input class="a handoff-fallback" id="handoff-fallback" type="url" value="${escapeHtml(handoffUrl)}" readonly aria-label="临时领取链接" hidden>
<p class="hint" id="copy-status" role="status">链接会自动过期。新帖子投递会在本机保存成功后确认；旧分享链接仍为一次性领取。</p>
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

  const commandName = interaction.data?.name
  const isPairCommand =
    interaction.type === 2 && interaction.data?.type === 1 && commandName === PAIR_COMMAND_NAME
  const isPostCommand =
    interaction.type === 2 && interaction.data?.type === 3 && commandName === POST_COMMAND_NAME
  if (
    interaction.type === 2 &&
    interaction.data?.type === 3 &&
    commandName === RESOURCE_COMMAND_NAME
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
