// SPDX-License-Identifier: Apache-2.0

// Event jobs of one session are generated together. An event's job is
// published with a delay, and the first job of a session to run takes the
// session's other queued event jobs with it, so one provider call covers
// several tool calls instead of one call each. The server (which publishes)
// reads the window; the worker (which generates) reads the batch size.

// How long an event waits for its neighbours. Longer means bigger batches and
// fewer calls, and memories that appear that much later.
const DEFAULT_BATCH_WINDOW_MS = 30_000;
const MAX_BATCH_WINDOW_MS = 5 * 60_000;
// Each event's payload is truncated to 16 KB in the prompt, so this also
// bounds the prompt at roughly 128 KB.
const DEFAULT_BATCH_MAX_EVENTS = 8;
const MAX_BATCH_MAX_EVENTS = 32;

function readBoundedInt(raw: string | undefined, fallback: number, min: number, max: number): number {
  if (raw === undefined || raw.trim() === '') return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

// 0 publishes every job immediately, which with fast workers means no batching.
export function resolveGenerationBatchWindowMs(env: NodeJS.ProcessEnv = process.env): number {
  return readBoundedInt(env.CLAUDE_MEM_SERVER_GENERATION_BATCH_WINDOW_MS, DEFAULT_BATCH_WINDOW_MS, 0, MAX_BATCH_WINDOW_MS);
}

// 1 keeps one call per event.
export function resolveGenerationBatchMaxEvents(env: NodeJS.ProcessEnv = process.env): number {
  return readBoundedInt(env.CLAUDE_MEM_SERVER_GENERATION_BATCH_MAX_EVENTS, DEFAULT_BATCH_MAX_EVENTS, 1, MAX_BATCH_MAX_EVENTS);
}
