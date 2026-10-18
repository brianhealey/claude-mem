// When a session last compacted, kept per session id.
//
// A compaction drops the session's earlier work from the agent's context, so
// the prompt hook stops treating memories from before it as already known.
// SessionStart (source "compact") or Codex PostCompact records the time;
// UserPromptSubmit reads it. They are separate processes, hence a file.
//
// Fail-open throughout (see session-marker-files): on error the time reads as
// unknown, which keeps the session's own memories excluded.
import { sessionMarkerFiles } from './session-marker-files.js';

const markers = sessionMarkerFiles('session-compacted-at', 'session-compaction');

export function markCompacted(sessionId: string, at: Date = new Date()): void {
  markers.mark(sessionId, at);
}

/** ISO time of the session's last compaction, or null when it never compacted. */
export function readCompactedAt(sessionId: string): string | null {
  const raw = markers.read(sessionId);
  if (raw === null) return null;
  const epoch = Date.parse(raw);
  return Number.isNaN(epoch) ? null : new Date(epoch).toISOString();
}
