export interface Env {
  DB: D1Database
  DISCORD_APPLICATION_ID: string
  DISCORD_PUBLIC_KEY: string
  DISCORD_BOT_TOKEN: string
  HANDOFF_TTL_SECONDS?: string
}

export interface DiscordInteraction {
  id?: string
  type: number
  context?: number
  token?: string
  user?: { id?: string }
  member?: { user?: { id?: string } }
  guild_id?: string
  channel_id?: string
  channel?: Record<string, unknown>
  data?: {
    name?: string
    type?: number
    options?: Array<{ name: string; type?: number; value?: unknown }>
    target_id?: string
    resolved?: {
      messages?: Record<string, Record<string, unknown>>
    }
  }
}

export type DiscordSourceRemoteScanCursor =
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

export interface DiscordSourceReadRequest {
  guildId?: string
  channelId: string
  threadId?: string
  starterMessageId?: string
  savedMessageIds: string[]
  scanMessageIds: string[]
  scanCursor?: DiscordSourceRemoteScanCursor
}

export interface DiscordSavedMessageCheckRequest {
  guildId?: string
  channelId: string
  threadId?: string
  starterMessageId?: string
  messageIds: string[]
}

export interface DiscordCaptureContext {
  guildId?: string
  guildName?: string
  channelId: string
  channelName?: string
  threadId?: string
  starterMessageId: string
  title?: string
  forumTags: string[]
}

export interface ThreadParentMetadata {
  channelName?: string
  forumTags: string[]
}

export interface DiscordGuildMetadata {
  name?: string
  access: 'available' | 'unavailable' | 'unknown'
}

export type DiscordSourceReadFailure =
  | { state: 'unavailable'; reason: 'not_found'; stage: 'channel' | 'starter' }
  | {
      state: 'uncheckable'
      reason: 'bot_access' | 'forbidden' | 'read_failed'
      stage: 'channel' | 'starter' | 'messages' | 'saved_messages'
    }

export const DISCORD_SNOWFLAKE_PATTERN = /^\d{5,32}$/u

export function validSnowflake(value: string | undefined): value is string {
  return Boolean(value && DISCORD_SNOWFLAKE_PATTERN.test(value))
}
