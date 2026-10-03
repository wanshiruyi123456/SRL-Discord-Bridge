/* global Request */

import { Env } from './DiscordSourceProtocol'

export const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Authorization, Content-Type, X-SRL-Library-ID',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
}

export class DiscordRateLimitError extends Error {
  readonly retryAfterMs?: number

  constructor(operation: string, retryAfterMs?: number) {
    super(`${operation} rate limited`)
    this.name = 'DiscordRateLimitError'
    this.retryAfterMs = retryAfterMs
  }
}

export function json(value: unknown, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers)
  headers.set('Content-Type', 'application/json; charset=utf-8')
  headers.set('Cache-Control', 'no-store')
  for (const [key, item] of Object.entries(CORS_HEADERS)) headers.set(key, item)
  return new Response(JSON.stringify(value), { ...init, headers })
}

export function html(value: string, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers)
  headers.set('Content-Type', 'text/html; charset=utf-8')
  return new Response(value, { ...init, headers })
}

export function escapeHtml(value: string): string {
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

export function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/gu, '-').replace(/\//gu, '_').replace(/=+$/gu, '')
}

export async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))
  return bytesToHex(new Uint8Array(digest))
}

export async function verifyDiscordRequest(
  request: Request,
  rawBody: string,
  publicKeyHex: string,
) {
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

export function authorizedSetup(request: Request, env: Env): boolean {
  const header = request.headers.get('Authorization') ?? ''
  return Boolean(env.DISCORD_BOT_TOKEN) && header === `Bearer ${env.DISCORD_BOT_TOKEN}`
}
