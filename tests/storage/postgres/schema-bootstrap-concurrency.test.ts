// SPDX-License-Identifier: Apache-2.0
//
// The server and the generation worker boot together and both bootstrap the
// schema. Unserialized, the two DDL transactions deadlock and one process dies
// on startup. Postgres-gated.

import { afterAll, beforeAll, describe, expect, it, spyOn } from 'bun:test';
import pg from 'pg';
import { randomUUID } from 'crypto';
import { bootstrapServerPostgresSchema } from '../../../src/storage/postgres/index.js';
import { logger } from '../../../src/utils/logger.js';

const testDatabaseUrl = process.env.CLAUDE_MEM_TEST_POSTGRES_URL;

describe('bootstrapServerPostgresSchema under concurrency', () => {
  if (!testDatabaseUrl) {
    it.skip('requires CLAUDE_MEM_TEST_POSTGRES_URL', () => {});
    return;
  }

  const schemaName = `cm_boot_${randomUUID().replaceAll('-', '_')}`;
  let admin: pg.Client;
  let spies: ReturnType<typeof spyOn>[] = [];

  beforeAll(async () => {
    spies = ['info', 'warn', 'error', 'debug'].map((m) => spyOn(logger, m as 'info').mockImplementation(() => {}));
    admin = new pg.Client({ connectionString: testDatabaseUrl });
    await admin.connect();
    await admin.query(`CREATE SCHEMA "${schemaName}"`);
  });

  afterAll(async () => {
    await admin.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    await admin.end();
    spies.forEach((s) => s.mockRestore());
  });

  it('survives several processes bootstrapping the same schema at once', async () => {
    const bootOnce = async () => {
      const client = new pg.Client({ connectionString: testDatabaseUrl, options: `-c search_path=${schemaName}` });
      await client.connect();
      try {
        await bootstrapServerPostgresSchema(client);
      } finally {
        await client.end();
      }
    };
    // A few rounds: the first creates everything, later ones re-run the ALTERs
    // against existing tables, which is the rollout case.
    for (let round = 0; round < 4; round += 1) {
      const results = await Promise.allSettled([bootOnce(), bootOnce(), bootOnce(), bootOnce()]);
      expect(results.filter(r => r.status === 'rejected').map(r => String((r as PromiseRejectedResult).reason))).toEqual([]);
    }
    const columns = await admin.query(
      `SELECT column_name FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'api_keys' AND column_name IN ('label', 'last_used_at')`,
      [schemaName],
    );
    expect(columns.rows).toHaveLength(2);
  });
});
