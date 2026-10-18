// IO discipline (see src/shared/hook-io.ts): this handler is PURE. It returns a
// HookResult and MUST NOT call process.stderr.write / process.stdout.write /
// console.* / process.exit.
//
// Codex PostCompact: the compaction just replaced this thread's history, memory
// timeline included. PostCompact output cannot carry additionalContext, so the
// handler only marks the thread; src/shared/codex-compaction-gate.ts says which
// hook brings the timeline back.
import type { EventHandler, NormalizedHookInput, HookResult } from '../types.js';
import { HOOK_EXIT_CODES } from '../../shared/hook-constants.js';
import { markTimelinePending } from '../../shared/codex-compaction-gate.js';
import { markCompacted } from '../../shared/session-compaction.js';

export const postCompactHandler: EventHandler = {
  async execute(input: NormalizedHookInput): Promise<HookResult> {
    markTimelinePending(input.sessionId, input.agentId);
    // The prompt hook's compaction time, in case SessionStart never reaches
    // claude-mem. Root thread only: a subagent thread carries the root's
    // session id, and its compaction leaves the root's context as it was.
    if (input.sessionId && !input.agentId) {
      markCompacted(input.sessionId);
    }
    return { continue: true, suppressOutput: true, exitCode: HOOK_EXIT_CODES.SUCCESS };
  },
};
