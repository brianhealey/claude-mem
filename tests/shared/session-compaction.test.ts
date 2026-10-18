import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { markCompacted, readCompactedAt } from '../../src/shared/session-compaction.js';
import { postCompactHandler } from '../../src/cli/handlers/post-compact.js';

describe('session-compaction', () => {
  let dataDir: string;
  const origEnv = process.env.CLAUDE_MEM_DATA_DIR;

  beforeEach(() => {
    dataDir = mkdtempSync(join(tmpdir(), 'session-compaction-test-'));
    process.env.CLAUDE_MEM_DATA_DIR = dataDir;
  });

  afterEach(() => {
    if (origEnv === undefined) delete process.env.CLAUDE_MEM_DATA_DIR;
    else process.env.CLAUDE_MEM_DATA_DIR = origEnv;
    rmSync(dataDir, { recursive: true, force: true });
  });

  it('reads no time for a session that never compacted', () => {
    expect(readCompactedAt('session-never-compacted')).toBeNull();
  });

  it('keeps the latest compaction time per session', () => {
    markCompacted('session-abc', new Date('2026-10-11T10:00:00.000Z'));
    markCompacted('session-abc', new Date('2026-10-11T12:30:00.000Z'));
    expect(readCompactedAt('session-abc')).toBe('2026-10-11T12:30:00.000Z');
    expect(readCompactedAt('session-other')).toBeNull();
  });

  it('accepts a session id that is not a safe file name', () => {
    markCompacted('../../etc/passwd', new Date('2026-10-11T10:00:00.000Z'));
    expect(readCompactedAt('../../etc/passwd')).toBe('2026-10-11T10:00:00.000Z');
  });

  it('reads no time from a marker that is not a date', () => {
    const dir = join(dataDir, 'state', 'session-compacted-at');
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'session-garbled'), 'not a date');
    expect(readCompactedAt('session-garbled')).toBeNull();
  });

  it('is recorded by a Codex PostCompact on the root thread, not on a subagent thread', async () => {
    await postCompactHandler.execute({ sessionId: 'codex-subagent-root', agentId: 'agent-1', platform: 'codex', cwd: dataDir });
    expect(readCompactedAt('codex-subagent-root')).toBeNull();

    await postCompactHandler.execute({ sessionId: 'codex-root', platform: 'codex', cwd: dataDir });
    expect(readCompactedAt('codex-root')).not.toBeNull();
  });
});
