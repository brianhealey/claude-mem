import { afterEach, describe, expect, it } from 'bun:test';
import {
  sessionInitHandler,
  setSessionInitDependenciesForTesting,
} from '../../../src/cli/handlers/session-init.js';
import type { SettingsDefaults } from '../../../src/shared/SettingsDefaultsManager.js';

const SESSION_ID = 'session-server-prompt';

interface ContextCall { request: Record<string, unknown>; options: unknown }

function useServerRuntime(options: {
  semanticInject: boolean;
  rows?: Array<Record<string, unknown>>;
  contextError?: Error;
}): ContextCall[] {
  const calls: ContextCall[] = [];
  setSessionInitDependenciesForTesting({
    loadFromFileOnce: () => ({
      CLAUDE_MEM_SEMANTIC_INJECT: String(options.semanticInject),
      CLAUDE_MEM_SEMANTIC_INJECT_LIMIT: '2',
      CLAUDE_MEM_CONTEXT_MAIN_AGENT_ONLY: 'true',
    }) as unknown as SettingsDefaults,
    shouldTrackProject: () => true,
    resolveRuntimeContext: () => ({
      runtime: 'server',
      projectId: 'project-1',
      client: {
        startSession: async () => ({ session: { id: 'server-session-1' } }),
        contextObservations: async (request: Record<string, unknown>, requestOptions: unknown) => {
          calls.push({ request, options: requestOptions });
          if (options.contextError) throw options.contextError;
          return { observations: options.rows ?? [], context: '' };
        },
      },
    }) as never,
  });
  return calls;
}

function submitPrompt(prompt = 'why does the value tab report a net negative benefit?') {
  return sessionInitHandler.execute({
    sessionId: SESSION_ID,
    cwd: '/tmp/session-init-server-prompt-context',
    platform: 'claude-code',
    prompt,
  });
}

describe('sessionInitHandler prompt context in server runtime', () => {
  afterEach(() => {
    setSessionInitDependenciesForTesting();
  });

  it('injects memories matched to the prompt, asking the server to leave out this session\'s own and summaries', async () => {
    const calls = useServerRuntime({
      semanticInject: true,
      rows: [
        { id: 'a', content: 'Own work\nbody', kind: 'discovery', contentSessionId: SESSION_ID, metadata: {}, createdAtEpoch: 0 },
        { id: 'b', content: 'A summary', kind: 'summary', contentSessionId: 'other', metadata: {}, createdAtEpoch: 0 },
        {
          id: 'c', content: 'Value tab accounting\nfull text', kind: 'discovery', contentSessionId: 'other',
          metadata: { title: 'Value tab accounting', narrative: 'Generation cost dominates.' },
          createdAtEpoch: Date.UTC(2026, 9, 10),
        },
        { id: 'd', content: 'Untitled memory\nmore', kind: 'change', contentSessionId: null, metadata: {}, createdAtEpoch: Date.UTC(2026, 9, 9) },
        { id: 'e', content: 'Past the limit', kind: 'change', contentSessionId: 'other', metadata: {}, createdAtEpoch: 0 },
      ],
    });

    const result = await submitPrompt();

    expect(calls).toHaveLength(1);
    expect(calls[0]!.request).toMatchObject({
      projectId: 'project-1',
      query: 'why does the value tab report a net negative benefit?',
      limit: 2,
      excludeContentSessionId: SESSION_ID,
      excludeKinds: ['summary'],
      excludeSubagents: true,
      platformSource: 'claude',
      purpose: 'prompt',
    });
    expect(Array.isArray(calls[0]!.request.folderProjects)).toBe(true);
    const context = result.hookSpecificOutput?.additionalContext ?? '';
    expect(context).toContain('### Value tab accounting (2026-10-10)');
    expect(context).toContain('Generation cost dominates.');
    expect(context).toContain('### Untitled memory (2026-10-09)');
    expect(context).not.toContain('Own work');
    expect(context).not.toContain('A summary');
    expect(context).not.toContain('Past the limit');
  });

  it('injects nothing when the setting is off, the prompt is short, or nothing matches', async () => {
    const off = useServerRuntime({ semanticInject: false });
    expect((await submitPrompt()).hookSpecificOutput).toBeUndefined();
    expect(off).toHaveLength(0);

    const short = useServerRuntime({ semanticInject: true });
    expect((await submitPrompt('fix it')).hookSpecificOutput).toBeUndefined();
    expect(short).toHaveLength(0);

    const empty = useServerRuntime({ semanticInject: true, rows: [] });
    expect((await submitPrompt()).hookSpecificOutput).toBeUndefined();
    expect(empty).toHaveLength(1);
  });

  it('never blocks the prompt when the server cannot answer', async () => {
    useServerRuntime({ semanticInject: true, contextError: new Error('timeout') });

    const result = await submitPrompt();

    expect(result.continue).toBe(true);
    expect(result.hookSpecificOutput).toBeUndefined();
  });
});
