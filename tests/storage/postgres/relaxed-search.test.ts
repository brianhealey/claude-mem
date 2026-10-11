// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import type pg from 'pg';
import {
  bootstrapServerPostgresSchema,
  createPostgresStorageRepositories,
  type PostgresStorageRepositories,
} from '../../../src/storage/postgres/index.js';
import { PostgresServerSessionsRepository } from '../../../src/storage/postgres/server-sessions.js';
import { createIsolatedSchema, dropSchema, poolForSchema } from '../../sdk/pg-isolation.js';

const testDatabaseUrl = process.env.CLAUDE_MEM_TEST_POSTGRES_URL;

describe('observation search widening', () => {
  if (!testDatabaseUrl) {
    it.skip('requires CLAUDE_MEM_TEST_POSTGRES_URL', () => {});
    return;
  }

  let pool: pg.Pool;
  let schemaName: string;
  let storage: PostgresStorageRepositories;
  let teamId: string;
  let projectId: string;

  beforeAll(async () => {
    schemaName = await createIsolatedSchema(testDatabaseUrl, 'cm_relaxed_search');
    pool = poolForSchema(testDatabaseUrl, schemaName);
    await bootstrapServerPostgresSchema(pool);
    storage = createPostgresStorageRepositories(pool);
    const team = await storage.teams.create({ name: 'team' });
    const project = await storage.projects.create({ teamId: team.id, name: 'p' });
    teamId = team.id;
    projectId = project.id;
    const contents = [
      'Corrected 7-day memory value picture shows net negative benefit',
      'Value endpoint totals snapshot for 7-day window',
      'Worker concurrency increased and deployed to homelab cluster',
      'Docker Desktop started and daemon verified',
      'Queue depth and worker memory check for job processing',
    ];
    for (const content of contents) {
      await storage.observations.create({ projectId, teamId, content });
    }
  });

  afterAll(async () => {
    await pool.end();
    await dropSchema(testDatabaseUrl, schemaName);
  });

  const search = (query: string, limit = 20) => storage.observations
    .search({ projectId, teamId, query, limit })
    .then(rows => rows.map(row => row.content));

  it('finds memories holding most of a question\'s terms when none holds them all', async () => {
    const results = await search('value endpoint 7-day totals net negative benefit caveats');
    expect(results.slice(0, 2).sort()).toEqual([
      'Corrected 7-day memory value picture shows net negative benefit',
      'Value endpoint totals snapshot for 7-day window',
    ]);
    expect(results).not.toContain('Docker Desktop started and daemon verified');
  });

  it('does not widen to rows sharing only a stray term', async () => {
    expect(await search('worker kubernetes ingress certificate renewal schedule')).toEqual([]);
  });

  it('ranks whole-query matches ahead of partial ones', async () => {
    const results = await search('worker memory');
    expect(results[0]).toBe('Queue depth and worker memory check for job processing');
    expect(results).toContain('Worker concurrency increased and deployed to homelab cluster');
  });

  it('leaves single-term and well-matched searches alone', async () => {
    expect(await search('docker')).toEqual(['Docker Desktop started and daemon verified']);
    expect(await search('nonexistentterm')).toEqual([]);
  });

  it('leaves out an agent session\'s own rows and the excluded kinds', async () => {
    const sessions = new PostgresServerSessionsRepository(pool);
    const own = await sessions.create({ projectId, teamId, contentSessionId: 'agent-own' });
    const other = await sessions.create({ projectId, teamId, contentSessionId: 'agent-other' });
    await storage.observations.create({ projectId, teamId, serverSessionId: own.id, content: 'Pangolin fact from the asking session' });
    await storage.observations.create({ projectId, teamId, serverSessionId: other.id, content: 'Pangolin fact from another session' });
    await storage.observations.create({ projectId, teamId, serverSessionId: other.id, kind: 'summary', content: 'Pangolin session summary' });
    await storage.observations.create({ projectId, teamId, content: 'Pangolin fact with no session' });

    const read = (extra: Record<string, unknown>) => storage.observations
      .search({ projectId, teamId, query: 'pangolin', ...extra })
      .then(rows => rows.map(row => row.content).sort());

    expect(await read({})).toHaveLength(4);
    expect(await read({ excludeContentSessionId: 'agent-own', excludeKinds: ['summary'] })).toEqual([
      'Pangolin fact from another session',
      'Pangolin fact with no session',
    ]);
    // A recency read (no query) applies the same exclusions.
    const recent = await storage.observations.search({ projectId, teamId, excludeContentSessionId: 'agent-own', limit: 50 });
    expect(recent.map(row => row.content)).not.toContain('Pangolin fact from the asking session');
  });

  it('survives query text that is not valid tsquery syntax', async () => {
    expect(await search('what\'s (the) "deal" & with | this:* <-> !')).toEqual([]);
  });
});
