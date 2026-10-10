// SPDX-License-Identifier: Apache-2.0
//
// The read and operator routes the hosted-server dashboard is built on:
// GET /v1/whoami, GET/DELETE /v1/keys, write-scoped minting on POST /v1/keys,
// GET /v1/projects, GET /v1/sessions, GET /v1/memories and GET /v1/jobs/summary.
// Postgres-gated.
//
// The assertions that matter most are the scoping ones: a key never lists or
// revokes another team's keys, and a revoked key stops authenticating.

import { afterEach, beforeEach, describe, expect, it, mock, spyOn } from 'bun:test';
import pg from 'pg';
import { randomUUID } from 'crypto';
import { Server } from '../../src/services/server/Server.js';
import { ServerV1PostgresRoutes } from '../../src/server/routes/v1/ServerV1PostgresRoutes.js';
import {
  bootstrapServerPostgresSchema,
  createPostgresStorageRepositories,
  type PostgresPoolClient,
  type PostgresStorageRepositories,
} from '../../src/storage/postgres/index.js';
import { DisabledServerQueueManager } from '../../src/server/runtime/types.js';
import { logger } from '../../src/utils/logger.js';
import { newApiKey } from '../sdk/pg-isolation.js';

const testDatabaseUrl = process.env.CLAUDE_MEM_TEST_POSTGRES_URL;
const q = (n: string) => `"${n.replaceAll('"', '""')}"`;

describe('dashboard routes', () => {
  if (!testDatabaseUrl) {
    it.skip('requires CLAUDE_MEM_TEST_POSTGRES_URL', () => {});
    return;
  }

  let pool: pg.Pool;
  let client: PostgresPoolClient;
  let schemaName: string;
  let storage: PostgresStorageRepositories;
  let server: Server;
  let port: number;
  let teamId: string;
  let projectId: string;
  let writeKey: string;
  let writeKeyId: string;
  let readKey: string;
  let readKeyId: string;
  let otherTeamKey: string;
  let otherTeamKeyId: string;
  let spies: ReturnType<typeof spyOn>[] = [];

  beforeEach(async () => {
    spies = ['info', 'warn', 'error', 'debug'].map((m) => spyOn(logger, m as 'info').mockImplementation(() => {}));
    pool = new pg.Pool({ connectionString: testDatabaseUrl });
    client = await pool.connect();
    schemaName = `cm_dash_${randomUUID().replaceAll('-', '_')}`;
    await client.query(`CREATE SCHEMA ${q(schemaName)}`);
    await client.query(`SET search_path TO ${q(schemaName)}`);
    await bootstrapServerPostgresSchema(client);
    pool.on('connect', (c) => { c.query(`SET search_path TO ${q(schemaName)}`).catch(() => {}); });
    storage = createPostgresStorageRepositories(client);

    const team = await storage.teams.create({ name: 'team' });
    const project = await storage.projects.create({ teamId: team.id, name: 'main-project' });
    teamId = team.id;
    projectId = project.id;
    const w = newApiKey(); writeKey = w.raw;
    writeKeyId = (await storage.auth.createApiKey({ keyHash: w.hash, teamId, projectId, actorId: 't', scopes: ['memories:read', 'memories:write'] })).id;
    const r = newApiKey(); readKey = r.raw;
    readKeyId = (await storage.auth.createApiKey({ keyHash: r.hash, teamId, projectId, actorId: 't', scopes: ['memories:read'] })).id;

    const otherTeam = await storage.teams.create({ name: 'other' });
    const otherProject = await storage.projects.create({ teamId: otherTeam.id, name: 'other-project' });
    const o = newApiKey(); otherTeamKey = o.raw;
    otherTeamKeyId = (await storage.auth.createApiKey({ keyHash: o.hash, teamId: otherTeam.id, projectId: otherProject.id, actorId: 't', scopes: ['memories:read', 'memories:write'] })).id;

    server = new Server({
      getInitializationComplete: () => true, getMcpReady: () => true,
      onShutdown: mock(() => Promise.resolve()), onRestart: mock(() => Promise.resolve()),
      workerPath: '/test/worker.cjs', runtime: 'server-beta',
      getAiStatus: () => ({ provider: 'disabled', authMethod: 'api-key', lastInteraction: null }),
    });
    server.registerRoutes(new ServerV1PostgresRoutes({
      pool: pool as never, queueManager: new DisabledServerQueueManager('disabled'),
      authMode: 'api-key',
    }));
    server.finalizeRoutes();
    await server.listen(0, '127.0.0.1');
    const addr = server.getHttpServer()?.address();
    if (!addr || typeof addr === 'string') throw new Error('no port');
    port = addr.port;
  });

  afterEach(async () => {
    try { await server.close(); } catch (e: unknown) {
      if ((e as NodeJS.ErrnoException)?.code !== 'ERR_SERVER_NOT_RUNNING') throw e;
    }
    await client.query(`DROP SCHEMA IF EXISTS ${q(schemaName)} CASCADE`);
    client.release();
    await pool.end();
    spies.forEach((s) => s.mockRestore());
    mock.restore();
  });

  const url = (p: string) => `http://127.0.0.1:${port}${p}`;
  const call = (method: string, path: string, key: string, body?: unknown) => fetch(url(path), {
    method,
    headers: { Authorization: `Bearer ${key}`, ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  it('GET /v1/whoami describes the presented key', async () => {
    const r = await call('GET', '/v1/whoami', readKey);
    expect(r.status).toBe(200);
    expect(await r.json()).toEqual({ apiKeyId: readKeyId, teamId, projectId, scopes: ['memories:read'] });
  });

  it('GET /v1/keys lists only this team\'s keys, without key material', async () => {
    const r = await call('GET', '/v1/keys', writeKey);
    expect(r.status).toBe(200);
    const raw = await r.text();
    expect(raw).not.toContain('keyHash');
    expect(raw).not.toContain('key_hash');
    const { keys } = JSON.parse(raw) as { keys: Array<{ id: string; status: string; current: boolean; scopes: string[] }> };
    expect(keys.map(k => k.id).sort()).toEqual([writeKeyId, readKeyId].sort());
    expect(keys.find(k => k.id === writeKeyId)?.current).toBe(true);
    expect(keys.find(k => k.id === readKeyId)?.current).toBe(false);
    expect(keys.every(k => k.status === 'active')).toBe(true);
  });

  it('a read-only key can neither list nor revoke keys', async () => {
    expect([401, 403]).toContain((await call('GET', '/v1/keys', readKey)).status);
    expect([401, 403]).toContain((await call('DELETE', `/v1/keys/${writeKeyId}`, readKey)).status);
  });

  it('DELETE /v1/keys/:id revokes a key, which then stops authenticating', async () => {
    expect((await call('GET', '/v1/whoami', readKey)).status).toBe(200);
    const r = await call('DELETE', `/v1/keys/${readKeyId}`, writeKey);
    expect(r.status).toBe(200);
    expect(await r.json()).toEqual({ revoked: true, id: readKeyId });
    expect((await call('GET', '/v1/whoami', readKey)).status).toBe(403);

    const { keys } = await (await call('GET', '/v1/keys', writeKey)).json() as { keys: Array<{ id: string; status: string }> };
    expect(keys.find(k => k.id === readKeyId)?.status).toBe('revoked');
    // Revoking again is a no-op, not an error.
    expect((await call('DELETE', `/v1/keys/${readKeyId}`, writeKey)).status).toBe(200);
  });

  it('a key cannot revoke itself, another team\'s key, or an unknown key', async () => {
    expect((await call('DELETE', `/v1/keys/${writeKeyId}`, writeKey)).status).toBe(409);
    expect((await call('DELETE', `/v1/keys/${otherTeamKeyId}`, writeKey)).status).toBe(404);
    expect((await call('DELETE', '/v1/keys/no-such-key', writeKey)).status).toBe(404);
    // The other team's key is untouched.
    expect((await call('GET', '/v1/whoami', otherTeamKey)).status).toBe(200);
  });

  it('POST /v1/keys can mint a write key, and still defaults to read-only', async () => {
    const minted = await (await call('POST', '/v1/keys', writeKey, { scopes: ['memories:read', 'memories:write'] })).json() as { apiKey: string; scopes: string[] };
    expect(minted.scopes).toEqual(['memories:read', 'memories:write']);
    const write = await call('POST', '/v1/memories', minted.apiKey, { projectId, content: 'written with a minted key' });
    expect(write.status).toBe(201);

    const defaulted = await (await call('POST', '/v1/keys', writeKey, {})).json() as { scopes: string[] };
    expect(defaulted.scopes).toEqual(['memories:read']);
    expect((await call('POST', '/v1/keys', writeKey, { scopes: ['admin'] })).status).toBe(400);
  });

  it('GET /v1/projects lists the key\'s project with its observation count', async () => {
    await storage.observations.create({ projectId, teamId, content: 'one' });
    await storage.observations.create({ projectId, teamId, content: 'two' });
    const r = await call('GET', '/v1/projects', readKey);
    expect(r.status).toBe(200);
    const { projects } = await r.json() as { projects: Array<{ id: string; name: string; observationCount: number }> };
    expect(projects).toHaveLength(1);
    expect(projects[0]).toMatchObject({ id: projectId, name: 'main-project', observationCount: 2 });
  });

  it('GET /v1/memories pages newest-first and filters by session', async () => {
    const session = await storage.sessions.create({ projectId, teamId, contentSessionId: 'content-1' });
    for (let i = 0; i < 5; i += 1) {
      await storage.observations.create({
        projectId, teamId, content: `observation ${i}`,
        serverSessionId: i < 2 ? session.id : null,
      });
    }
    type Page = { observations: Array<{ content: string }>; hasMore: boolean };
    const first = await (await call('GET', `/v1/memories?projectId=${projectId}&limit=3`, readKey)).json() as Page;
    expect(first.observations.map(o => o.content)).toEqual(['observation 4', 'observation 3', 'observation 2']);
    expect(first.hasMore).toBe(true);
    const second = await (await call('GET', `/v1/memories?projectId=${projectId}&limit=3&offset=3`, readKey)).json() as Page;
    expect(second.observations.map(o => o.content)).toEqual(['observation 1', 'observation 0']);
    expect(second.hasMore).toBe(false);

    const inSession = await (await call('GET', `/v1/memories?projectId=${projectId}&serverSessionId=${session.id}`, readKey)).json() as Page;
    expect(inSession.observations.map(o => o.content).sort()).toEqual(['observation 0', 'observation 1']);

    expect((await call('GET', '/v1/memories', readKey)).status).toBe(400);
    // A project-scoped key cannot read a different project.
    expect((await call('GET', `/v1/memories?projectId=${projectId}`, otherTeamKey)).status).toBe(403);
  });

  it('GET /v1/sessions lists sessions with their observation counts', async () => {
    const session = await storage.sessions.create({ projectId, teamId, contentSessionId: 'content-1', metadata: { project: 'my-repo' } });
    await storage.observations.create({ projectId, teamId, content: 'in session', serverSessionId: session.id });
    const r = await call('GET', `/v1/sessions?projectId=${projectId}`, readKey);
    expect(r.status).toBe(200);
    const { sessions } = await r.json() as { sessions: Array<Record<string, unknown>> };
    expect(sessions).toHaveLength(1);
    expect(sessions[0]).toMatchObject({ id: session.id, contentSessionId: 'content-1', folder: 'my-repo', observationCount: 1 });
  });

  it('GET /v1/jobs/summary returns zeroed counts and is not read as a job id', async () => {
    const r = await call('GET', '/v1/jobs/summary', readKey);
    expect(r.status).toBe(200);
    expect(await r.json()).toEqual({
      counts: { queued: 0, processing: 0, completed: 0, failed: 0, cancelled: 0 },
      lastCompletedAtEpoch: null,
      lastFailedAtEpoch: null,
      failedByReason: [],
      lastGeneration: null,
      captureToMemory: { medianMs: null, sampleSize: 0 },
    });
  });

  it('GET /v1/folders counts observations per folder and kind', async () => {
    await storage.observations.create({ projectId, teamId, content: 'a', kind: 'bugfix', metadata: { project: 'cluster' } });
    await storage.observations.create({ projectId, teamId, content: 'b', kind: 'bugfix', metadata: { project: 'cluster' } });
    await storage.observations.create({ projectId, teamId, content: 'c', kind: 'decision', metadata: { project: 'claude-mem' } });
    await storage.observations.create({ projectId, teamId, content: 'd', kind: 'decision' });
    const r = await call('GET', `/v1/folders?projectId=${projectId}`, readKey);
    expect(r.status).toBe(200);
    const body = await r.json() as {
      folders: Array<{ folder: string | null; observationCount: number }>;
      kinds: Array<{ kind: string; observationCount: number }>;
    };
    const byFolder = Object.fromEntries(body.folders.map(f => [String(f.folder), f.observationCount]));
    expect(byFolder).toEqual({ cluster: 2, 'claude-mem': 1, null: 1 });
    expect(Object.fromEntries(body.kinds.map(k => [k.kind, k.observationCount]))).toEqual({ bugfix: 2, decision: 2 });
    expect((await call('GET', '/v1/folders', readKey)).status).toBe(400);
  });

  it('GET /v1/memories filters by folder (ASCII case-insensitive) and by kind', async () => {
    await storage.observations.create({ projectId, teamId, content: 'cluster bug', kind: 'bugfix', metadata: { project: 'Cluster' } });
    await storage.observations.create({ projectId, teamId, content: 'cluster decision', kind: 'decision', metadata: { project: 'cluster' } });
    await storage.observations.create({ projectId, teamId, content: 'other bug', kind: 'bugfix', metadata: { project: 'claude-mem' } });
    type Page = { observations: Array<{ content: string }> };
    const inFolder = await (await call('GET', `/v1/memories?projectId=${projectId}&folder=cluster`, readKey)).json() as Page;
    expect(inFolder.observations.map(o => o.content).sort()).toEqual(['cluster bug', 'cluster decision']);
    const both = await (await call('GET', `/v1/memories?projectId=${projectId}&folder=cluster&kind=bugfix`, readKey)).json() as Page;
    expect(both.observations.map(o => o.content)).toEqual(['cluster bug']);
  });

  it('GET /v1/sessions/:id/events lists events with the observations made from them', async () => {
    const session = await storage.sessions.create({ projectId, teamId, contentSessionId: 'content-1' });
    const first = await storage.agentEvents.create({
      projectId, teamId, serverSessionId: session.id, sourceAdapter: 'api', eventType: 'tool_use',
      payload: { tool_name: 'Bash', tool_input: { command: 'secret-looking input' } }, occurredAt: new Date(1_700_000_000_000),
    });
    const second = await storage.agentEvents.create({
      projectId, teamId, serverSessionId: session.id, sourceAdapter: 'api', eventType: 'tool_use',
      payload: { tool_name: 'Edit' }, occurredAt: new Date(1_700_000_001_000),
    });
    const observation = await storage.observations.create({ projectId, teamId, content: 'from bash', serverSessionId: session.id });
    await storage.observationSources.addSource({
      observationId: observation.id, projectId, teamId, sourceType: 'agent_event', sourceId: first.id, agentEventId: first.id,
    });

    const r = await call('GET', `/v1/sessions/${session.id}/events`, readKey);
    expect(r.status).toBe(200);
    const raw = await r.text();
    // Payloads stay out of the listing.
    expect(raw).not.toContain('secret-looking input');
    const { events, hasMore } = JSON.parse(raw) as { events: Array<Record<string, unknown>>; hasMore: boolean };
    expect(hasMore).toBe(false);
    expect(events.map(e => [e.id, e.toolName, e.observationIds])).toEqual([
      [first.id, 'Bash', [observation.id]],
      [second.id, 'Edit', []],
    ]);
    const listed = await (await call('GET', `/v1/sessions?projectId=${projectId}`, readKey)).json() as { sessions: Array<{ eventCount: number }> };
    expect(listed.sessions[0].eventCount).toBe(2);
    // Another team cannot read this session's events.
    expect((await call('GET', `/v1/sessions/${session.id}/events`, otherTeamKey)).status).toBe(404);
  });

  describe('failed jobs', () => {
    const seedFailedJob = async (reason: string) => {
      const event = await storage.agentEvents.create({
        projectId, teamId, sourceAdapter: 'api', eventType: 'tool_use', payload: { n: randomUUID() }, occurredAt: new Date(),
      });
      const job = await storage.observationGenerationJobs.create({
        projectId, teamId, sourceType: 'agent_event', sourceId: event.id, agentEventId: event.id,
        jobType: 'observation_generate_for_event', bullmqJobId: `evt_${event.id}`,
      });
      await client.query(
        `UPDATE observation_generation_jobs
            SET status = 'failed', failed_at = now(), attempts = max_attempts, last_error = $2::jsonb
          WHERE id = $1`,
        [job.id, JSON.stringify({ reason })],
      );
      return job.id;
    };

    it('GET /v1/jobs/summary groups failures by reason', async () => {
      await seedFailedJob('quota exhausted');
      await seedFailedJob('quota exhausted');
      await seedFailedJob('upstream 503');
      const body = await (await call('GET', '/v1/jobs/summary', readKey)).json() as {
        counts: Record<string, number>; failedByReason: Array<{ reason: string; count: number }>;
      };
      expect(body.counts.failed).toBe(3);
      expect(body.failedByReason).toEqual([
        { reason: 'quota exhausted', count: 2 },
        { reason: 'upstream 503', count: 1 },
      ]);
    });

    it('POST /v1/jobs/retry-failed requeues failed jobs, optionally for one reason', async () => {
      const quotaA = await seedFailedJob('quota exhausted');
      const quotaB = await seedFailedJob('quota exhausted');
      const upstream = await seedFailedJob('upstream 503');
      const statusOf = async (id: string) =>
        (await client.query<{ status: string; attempts: number; max_attempts: number }>(
          'SELECT status, attempts, max_attempts FROM observation_generation_jobs WHERE id = $1', [id])).rows[0];

      expect([401, 403]).toContain((await call('POST', '/v1/jobs/retry-failed', readKey, {})).status);

      const scoped = await call('POST', '/v1/jobs/retry-failed', writeKey, { reason: 'quota exhausted' });
      expect(scoped.status).toBe(200);
      expect(await scoped.json()).toMatchObject({ requeued: 2, remaining: false });
      expect((await statusOf(quotaA)).status).toBe('queued');
      expect((await statusOf(quotaB)).status).toBe('queued');
      expect((await statusOf(upstream)).status).toBe('failed');
      // Like the single retry, it leaves room for one more attempt.
      const retried = await statusOf(quotaA);
      expect(retried.attempts).toBe(retried.max_attempts - 1);

      const rest = await (await call('POST', '/v1/jobs/retry-failed', writeKey, {})).json() as { requeued: number };
      expect(rest.requeued).toBe(1);
      expect((await statusOf(upstream)).status).toBe('queued');
      // Nothing left to retry.
      expect(await (await call('POST', '/v1/jobs/retry-failed', writeKey, {})).json()).toMatchObject({ requeued: 0 });
      // The other team's key requeues nothing of ours.
      await seedFailedJob('again');
      expect(await (await call('POST', '/v1/jobs/retry-failed', otherTeamKey, {})).json()).toMatchObject({ requeued: 0 });
    });
  });
});
