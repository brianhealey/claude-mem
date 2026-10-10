// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import type pg from 'pg';
import {
  bootstrapServerPostgresSchema,
  createPostgresStorageRepositories,
  type PostgresStorageRepositories,
} from '../../../src/storage/postgres/index.js';
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

  it('survives query text that is not valid tsquery syntax', async () => {
    expect(await search('what\'s (the) "deal" & with | this:* <-> !')).toEqual([]);
  });
});
