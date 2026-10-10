// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it } from 'bun:test';
import type { Job } from 'bullmq';
import type pg from 'pg';
import {
  bootstrapServerPostgresSchema,
  createPostgresStorageRepositories,
  type PostgresStorageRepositories,
} from '../../../src/storage/postgres/index.js';
import { ProviderObservationGenerator } from '../../../src/server/generation/ProviderObservationGenerator.js';
import { ServerClassifiedProviderError } from '../../../src/server/generation/providers/shared/error-classification.js';
import type {
  ServerGenerationContext,
  ServerGenerationProvider,
} from '../../../src/server/generation/providers/shared/types.js';
import {
  IngestEventsService,
  resolveGenerationSkipTools,
  shouldGenerateForEvent,
} from '../../../src/server/services/IngestEventsService.js';
import {
  resolveGenerationBatchMaxEvents,
  resolveGenerationBatchWindowMs,
} from '../../../src/server/jobs/generation-batching.js';
import type { GenerateObservationsForEventJob } from '../../../src/server/jobs/types.js';
import { PostgresServerSessionsRepository } from '../../../src/storage/postgres/server-sessions.js';
import { ModeManager } from '../../../src/services/domain/ModeManager.js';
import { buildServerGenerationPrompt } from '../../../src/server/generation/providers/shared/prompt-builder.js';
import { createIsolatedSchema, dropSchema, poolForSchema } from '../../sdk/pg-isolation.js';

const testDatabaseUrl = process.env.CLAUDE_MEM_TEST_POSTGRES_URL;

const OBSERVATION_XML = '<observation><type>discovery</type><title>Batch</title><facts><fact>f</fact></facts></observation>';

class RecordingProvider implements ServerGenerationProvider {
  readonly providerLabel = 'claude' as const;
  contexts: ServerGenerationContext[] = [];

  constructor(private readonly response: string | Error) {}

  async generate(context: ServerGenerationContext) {
    this.contexts.push(context);
    if (this.response instanceof Error) throw this.response;
    return { rawText: this.response, providerLabel: this.providerLabel };
  }
}

describe('generation skip list', () => {
  it('skips navigation tools by default and generates for the rest', () => {
    const skip = resolveGenerationSkipTools({});
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: { tool_name: 'Glob' } }, skip)).toBe(false);
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: { tool_name: 'toolsearch' } }, skip)).toBe(false);
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: { tool_name: 'Edit' } }, skip)).toBe(true);
    expect(shouldGenerateForEvent({ eventType: 'assistant_message', payload: { tool_name: 'Glob' } }, skip)).toBe(true);
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: 'not an object' }, skip)).toBe(true);
  });

  it('is replaced, or turned off, by the environment', () => {
    const custom = resolveGenerationSkipTools({ CLAUDE_MEM_SERVER_GENERATION_SKIP_TOOLS: 'Read, Bash' });
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: { tool_name: 'read' } }, custom)).toBe(false);
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: { tool_name: 'Glob' } }, custom)).toBe(true);
    const off = resolveGenerationSkipTools({ CLAUDE_MEM_SERVER_GENERATION_SKIP_TOOLS: '' });
    expect(shouldGenerateForEvent({ eventType: 'tool_use', payload: { tool_name: 'Glob' } }, off)).toBe(true);
  });
});

describe('generation batching settings', () => {
  it('clamps the window and the batch size', () => {
    expect(resolveGenerationBatchWindowMs({})).toBe(30_000);
    expect(resolveGenerationBatchWindowMs({ CLAUDE_MEM_SERVER_GENERATION_BATCH_WINDOW_MS: '0' })).toBe(0);
    expect(resolveGenerationBatchWindowMs({ CLAUDE_MEM_SERVER_GENERATION_BATCH_WINDOW_MS: '99999999' })).toBe(300_000);
    expect(resolveGenerationBatchWindowMs({ CLAUDE_MEM_SERVER_GENERATION_BATCH_WINDOW_MS: 'soon' })).toBe(30_000);
    expect(resolveGenerationBatchMaxEvents({})).toBe(8);
    expect(resolveGenerationBatchMaxEvents({ CLAUDE_MEM_SERVER_GENERATION_BATCH_MAX_EVENTS: '0' })).toBe(1);
  });
});

describe('event generation batching', () => {
  if (!testDatabaseUrl) {
    it.skip('requires CLAUDE_MEM_TEST_POSTGRES_URL', () => {});
    return;
  }

  let pool: pg.Pool;
  let schemaName: string;
  let storage: PostgresStorageRepositories;
  let teamId: string;
  let projectId: string;
  let sessionId: string;

  beforeEach(async () => {
    ModeManager.getInstance().loadMode('code');
    schemaName = await createIsolatedSchema(testDatabaseUrl, 'cm_batching');
    pool = poolForSchema(testDatabaseUrl, schemaName);
    await bootstrapServerPostgresSchema(pool);
    storage = createPostgresStorageRepositories(pool);
    const team = await storage.teams.create({ name: 'team' });
    const project = await storage.projects.create({ teamId: team.id, name: 'p' });
    teamId = team.id;
    projectId = project.id;
    const session = await new PostgresServerSessionsRepository(pool).create({ projectId, teamId, contentSessionId: 'content-1' });
    sessionId = session.id;
  });

  afterEach(async () => {
    await pool.end();
    await dropSchema(testDatabaseUrl, schemaName);
  });

  async function addEventJob(index: number, options: { session?: string | null; apiKeyId?: string | null } = {}) {
    const serverSessionId = options.session === undefined ? sessionId : options.session;
    const event = await storage.agentEvents.create({
      projectId,
      teamId,
      serverSessionId,
      sourceAdapter: 'api',
      eventType: 'tool_use',
      payload: { tool_name: 'Edit', marker: `event-${index}` },
      occurredAt: new Date(Date.UTC(2026, 0, 1, 0, 0, index)),
    });
    const job = await storage.observationGenerationJobs.create({
      projectId,
      teamId,
      sourceType: 'agent_event',
      sourceId: event.id,
      agentEventId: event.id,
      serverSessionId,
      jobType: 'observation_generate_for_event',
      payload: { api_key_id: options.apiKeyId ?? null },
    });
    return { event, job };
  }

  function bullJob(entry: Awaited<ReturnType<typeof addEventJob>>): Job<GenerateObservationsForEventJob> {
    return {
      id: `bull-${entry.job.id}`,
      data: {
        kind: 'event',
        team_id: teamId,
        project_id: projectId,
        source_type: 'agent_event',
        source_id: entry.event.id,
        generation_job_id: entry.job.id,
        agent_event_id: entry.event.id,
        api_key_id: null,
        actor_id: null,
        source_adapter: 'api',
      },
    } as unknown as Job<GenerateObservationsForEventJob>;
  }

  function statusOf(jobId: string) {
    return storage.observationGenerationJobs
      .getByIdForScope({ id: jobId, projectId, teamId })
      .then(job => job?.status);
  }

  it('covers a session\'s queued event jobs with one provider call', async () => {
    const entries = [await addEventJob(1), await addEventJob(2), await addEventJob(3)];
    const provider = new RecordingProvider(OBSERVATION_XML);
    const generator = new ProviderObservationGenerator({ pool, provider });

    const result = await generator.process(bullJob(entries[1]!));

    expect(result.observationCount).toBe(1);
    expect(provider.contexts).toHaveLength(1);
    expect(provider.contexts[0]!.events.map(event => (event.payload as { marker: string }).marker))
      .toEqual(['event-1', 'event-2', 'event-3']);
    for (const entry of entries) {
      expect(await statusOf(entry.job.id)).toBe('completed');
    }

    const observations = await storage.observations.listByProject({ projectId, teamId });
    expect(observations).toHaveLength(1);
    const sources = await storage.observationSources.listByObservationForScope({
      observationId: observations[0]!.id,
      projectId,
      teamId,
    });
    expect(sources.map(source => source.agentEventId).sort())
      .toEqual(entries.map(entry => entry.event.id).sort());

    // The batched jobs' own deliveries arrive later and must not pay again.
    const replay = await generator.process(bullJob(entries[0]!));
    expect(replay.observationCount).toBe(0);
    expect(provider.contexts).toHaveLength(1);
  });

  it('respects the batch size and leaves the rest queued', async () => {
    const entries = [await addEventJob(1), await addEventJob(2), await addEventJob(3)];
    const provider = new RecordingProvider(OBSERVATION_XML);
    const generator = new ProviderObservationGenerator({ pool, provider, batchMaxEvents: 2 });

    await generator.process(bullJob(entries[0]!));

    expect(provider.contexts[0]!.events).toHaveLength(2);
    expect(await statusOf(entries[1]!.job.id)).toBe('completed');
    expect(await statusOf(entries[2]!.job.id)).toBe('queued');
  });

  it('does not batch across sessions, API keys, or with batching off', async () => {
    const own = await addEventJob(1);
    const otherSession = await new PostgresServerSessionsRepository(pool).create({ projectId, teamId, contentSessionId: 'content-2' });
    const elsewhere = await addEventJob(2, { session: otherSession.id });
    const otherKey = await addEventJob(3, { apiKeyId: 'key-other' });
    const provider = new RecordingProvider(OBSERVATION_XML);

    await new ProviderObservationGenerator({ pool, provider }).process(bullJob(own));
    expect(provider.contexts[0]!.events).toHaveLength(1);
    expect(await statusOf(elsewhere.job.id)).toBe('queued');
    expect(await statusOf(otherKey.job.id)).toBe('queued');

    const second = await addEventJob(4);
    const third = await addEventJob(5);
    await new ProviderObservationGenerator({ pool, provider, batchMaxEvents: 1 }).process(bullJob(second));
    expect(provider.contexts[1]!.events).toHaveLength(1);
    expect(await statusOf(third.job.id)).toBe('queued');
  });

  it('requeues the batched jobs when the call is refused before any work', async () => {
    const entries = [await addEventJob(1), await addEventJob(2)];
    const provider = new RecordingProvider(
      new ServerClassifiedProviderError('rate limited', { kind: 'rate_limit', cause: null }),
    );
    const generator = new ProviderObservationGenerator({ pool, provider });

    await expect(generator.process(bullJob(entries[0]!))).rejects.toThrow('rate limited');

    expect(await statusOf(entries[0]!.job.id)).toBe('queued');
    expect(await statusOf(entries[1]!.job.id)).toBe('queued');
  });

  it('fails the batched jobs with the carrying job on an unusable response', async () => {
    const entries = [await addEventJob(1), await addEventJob(2)];
    const provider = new RecordingProvider('   ');
    const generator = new ProviderObservationGenerator({ pool, provider });

    await expect(generator.process(bullJob(entries[0]!))).rejects.toThrow();

    expect(await statusOf(entries[0]!.job.id)).toBe('failed');
    expect(await statusOf(entries[1]!.job.id)).toBe('failed');
  });

  it('shows the model the session\'s titles and drops a memory that repeats one', async () => {
    const first = await addEventJob(1);
    const second = await addEventJob(2);
    const repeat = OBSERVATION_XML.replace('<title>Batch</title>', '<title>  batch </title>')
      + '<observation><type>discovery</type><title>New finding</title><facts><fact>g</fact></facts></observation>';

    const firstProvider = new RecordingProvider(OBSERVATION_XML);
    await new ProviderObservationGenerator({ pool, provider: firstProvider, batchMaxEvents: 1 }).process(bullJob(first));
    expect(firstProvider.contexts[0]!.recordedTitles).toEqual([]);
    expect(buildServerGenerationPrompt(firstProvider.contexts[0]!).prompt).not.toContain('<already_recorded>');

    const secondProvider = new RecordingProvider(repeat);
    const result = await new ProviderObservationGenerator({ pool, provider: secondProvider, batchMaxEvents: 1 })
      .process(bullJob(second));

    expect(secondProvider.contexts[0]!.recordedTitles).toEqual(['Batch']);
    expect(buildServerGenerationPrompt(secondProvider.contexts[0]!).prompt)
      .toContain('<already_recorded>\n    <title>Batch</title>\n  </already_recorded>');
    expect(result.observationCount).toBe(1);
    const titles = (await storage.observations.listByProject({ projectId, teamId }))
      .map(observation => observation.metadata.title)
      .sort();
    expect(titles).toEqual(['Batch', 'New finding']);
  });

  it('delays session events at publish and skips generation for navigation tools', async () => {
    const added: { jobId: string; options: unknown }[] = [];
    const service = new IngestEventsService({
      pool,
      resolveEventQueue: () => ({
        add: async (jobId: string, _payload: unknown, options?: unknown) => {
          added.push({ jobId, options });
        },
      }),
    });
    const base = { projectId, teamId, sourceAdapter: 'api', eventType: 'tool_use', occurredAt: new Date() };

    const inSession = await service.ingestOne({ ...base, serverSessionId: sessionId, payload: { tool_name: 'Edit' } });
    const noSession = await service.ingestOne({ ...base, payload: { tool_name: 'Edit' } });
    const navigation = await service.ingestOne({ ...base, serverSessionId: sessionId, payload: { tool_name: 'Glob' } });
    const batch = await service.ingestBatch([
      { ...base, serverSessionId: sessionId, payload: { tool_name: 'LS' } },
      { ...base, serverSessionId: sessionId, payload: { tool_name: 'Bash' } },
    ]);

    expect(inSession.enqueueState).toBe('enqueued');
    expect(noSession.enqueueState).toBe('enqueued');
    expect(navigation.outbox).toBeNull();
    expect(navigation.enqueueState).toBe('skipped');
    expect(navigation.event.id).toBeTruthy();
    expect(batch.map(entry => entry.enqueueState)).toEqual(['skipped', 'enqueued']);
    expect(added.map(entry => entry.options)).toEqual([{ delay: 30_000 }, undefined, { delay: 30_000 }]);
  });
});
