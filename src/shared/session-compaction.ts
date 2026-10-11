// When a session last compacted, kept per session id.
//
// A compaction drops the session's earlier work from the agent's context, so
// the prompt hook stops treating memories from before it as already known.
// SessionStart (source "compact") records the time; UserPromptSubmit reads it.
// They are separate processes, hence a file.
//
// Fail-open throughout: a filesystem error must never break a hook. On error
// the time reads as unknown, which keeps the session's own memories excluded.
import { mkdirSync, readFileSync, writeFileSync } from 'fs';
import { createHash } from 'crypto';
import { dirname, join } from 'path';
import { resolveDataDir } from './paths.js';
import { logger } from '../utils/logger.js';

const MARKER_DIR_NAME = 'session-compacted-at';
const SAFE_SESSION_ID = /^[A-Za-z0-9_-]{1,128}$/;

function markerPath(sessionId: string): string {
  const name = SAFE_SESSION_ID.test(sessionId)
    ? sessionId
    : `sha256-${createHash('sha256').update(sessionId).digest('hex')}`;
  return join(resolveDataDir(), 'state', MARKER_DIR_NAME, name);
}

export function markCompacted(sessionId: string, at: Date = new Date()): void {
  try {
    const marker = markerPath(sessionId);
    mkdirSync(dirname(marker), { recursive: true });
    writeFileSync(marker, at.toISOString());
  } catch (error) {
    logger.warn('HOOK', 'session-compaction: markCompacted failed open', {
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

/** ISO time of the session's last compaction, or null when it never compacted. */
export function readCompactedAt(sessionId: string): string | null {
  let raw: string;
  try {
    raw = readFileSync(markerPath(sessionId), 'utf8').trim();
  } catch {
    return null;
  }
  const epoch = Date.parse(raw);
  return Number.isNaN(epoch) ? null : new Date(epoch).toISOString();
}
