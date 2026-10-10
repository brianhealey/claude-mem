// SPDX-License-Identifier: Apache-2.0

import type { Application, Request, RequestHandler, Response } from 'express';
import { z, type ZodTypeAny } from 'zod';
import type { RouteHandler } from '../../../services/server/Server.js';
import { CreateAgentEventSchema } from '../../../core/schemas/agent-event.js';
import type { PostgresPool } from '../../../storage/postgres/pool.js';
import {
  PostgresAgentEventsRepository,
  type CreatePostgresAgentEventInput,
  type PostgresAgentEvent,
} from '../../../storage/postgres/agent-events.js';
import {
  PostgresObservationGenerationJobEventsRepository,
  PostgresObservationGenerationJobRepository,
  type PostgresObservationGenerationJob,
} from '../../../storage/postgres/generation-jobs.js';
import { PostgresAuthRepository } from '../../../storage/postgres/auth.js';
import { PostgresObservationRepository } from '../../../storage/postgres/observations.js';
import { PostgresProjectsRepository } from '../../../storage/postgres/projects.js';
import { logger } from '../../../utils/logger.js';
import { requirePostgresServerAuth } from '../../middleware/postgres-auth.js';
import { PostgresDataDeletionRepository } from '../../../storage/postgres/data-deletion.js';
import { requestIdMiddleware } from '../../middleware/request-id.js';
import type { ActiveServerQueueManager } from '../../runtime/ActiveServerQueueManager.js';
import type { ServerQueueManager } from '../../runtime/types.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { createRecallMcpServer, type RecallBackend } from '../../mcp/recall-mcp-server.js';
import { requireRateLimit, requireMonthlyQuota } from '../../middleware/rate-limit.js';
import { meterRequests } from '../../middleware/usage-metering.js';
import { PostgresUsageRepository } from '../../../storage/postgres/usage.js';
import { createHash, randomBytes } from 'node:crypto';
import { PostgresServerSessionsRepository } from '../../../storage/postgres/server-sessions.js';
import { IngestEventsService, type EnqueueOutcome } from '../../services/IngestEventsService.js';
import { EndSessionService } from '../../services/EndSessionService.js';
import { normalizePlatformSource, normalizePlatformSourceOrNull } from '../../../shared/platform-source.js';
import {
  SERVER_CONTEXT_MAX_LIMIT,
  SERVER_CONTEXT_QUERY_DEFAULT_LIMIT,
  SERVER_CONTEXT_RECENT_DEFAULT_LIMIT,
} from '../../../shared/server-context-limits.js';

const SOURCE_ADAPTER_DEFAULT = 'api';

// The scopes POST /v1/keys may grant. Minting requires memories:write, so
// neither is more than the caller already has.
const MINTABLE_KEY_SCOPES = ['memories:read', 'memories:write'] as const;

const MEMORY_LIST_DEFAULT_LIMIT = 50;
const MEMORY_LIST_MAX_LIMIT = 200;
const SESSION_LIST_DEFAULT_LIMIT = 50;
const SESSION_LIST_MAX_LIMIT = 200;
const SESSION_EVENT_LIST_DEFAULT_LIMIT = 200;
const SESSION_EVENT_LIST_MAX_LIMIT = 500;
// How many recent completed jobs the capture-to-memory median is taken over.
const JOB_LATENCY_SAMPLE_SIZE = 50;
const BULK_RETRY_MAX_JOBS = 200;
const AUDIT_LIST_DEFAULT_LIMIT = 100;
const AUDIT_LIST_MAX_LIMIT = 500;

declare const __DEFAULT_PACKAGE_VERSION__: string;
const MCP_SERVER_VERSION =
  typeof __DEFAULT_PACKAGE_VERSION__ !== 'undefined' ? __DEFAULT_PACKAGE_VERSION__ : '0.0.0-dev';

// The MCP link base: CLAUDE_MEM_PUBLIC_URL in prod (behind a proxy/LB), else
// derived from the request host so the connect command points at this server.
function mcpConnectUrl(req: Request): string {
  const base = (process.env.CLAUDE_MEM_PUBLIC_URL ?? `${req.protocol}://${req.get('host') ?? 'localhost'}`)
    .replace(/\/+$/, '');
  return `${base}/v1/mcp`;
}
function mcpConnectCommand(mcpUrl: string, key: string): string {
  return `claude mcp add --transport http claude-mem ${mcpUrl} --header "Authorization: Bearer ${key}"`;
}

export interface ServerV1PostgresRoutesOptions {
  pool: PostgresPool;
  queueManager: ServerQueueManager;
  authMode?: string;
  allowLocalDevBypass?: boolean;
  // Queue lookup is exposed as a function so tests can swap the queue manager.
  // When the manager is the disabled adapter, enqueue is silently skipped and
  // the outbox row stays in `queued` state for startup reconciliation to
  // pick up — never claim observations were generated.
  getEventQueue?: () => ReturnType<ActiveServerQueueManager['getQueue']> | null;
  getSummaryQueue?: () => ReturnType<ActiveServerQueueManager['getQueue']> | null;
}

interface BatchPreValidationFailure {
  status: number;
  body: { error: string; message: string };
}

const EVENT_QUERY_SCHEMA = z.object({
  generate: z.union([z.literal('true'), z.literal('false')]).optional(),
  wait: z.union([z.literal('true'), z.literal('false')]).optional(),
});

// `?wait=true` polls the outbox row until it reaches a terminal status
// (`completed` / `failed` / `cancelled`). Hard-capped so a stuck provider can
// never block an HTTP worker indefinitely; callers always get a response.
const WAIT_TIMEOUT_MS = 30_000;
const WAIT_POLL_INTERVAL_MS = 100;
const TERMINAL_JOB_STATUSES: readonly PostgresObservationGenerationJob['status'][] = [
  'completed',
  'failed',
  'cancelled',
];

async function waitForTerminalJob(
  jobRepo: PostgresObservationGenerationJobRepository,
  job: PostgresObservationGenerationJob,
  timeoutMs: number = WAIT_TIMEOUT_MS,
  intervalMs: number = WAIT_POLL_INTERVAL_MS,
): Promise<{ job: PostgresObservationGenerationJob; timedOut: boolean }> {
  if (TERMINAL_JOB_STATUSES.includes(job.status)) {
    return { job, timedOut: false };
  }
  const deadline = Date.now() + timeoutMs;
  let current = job;
  while (Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, intervalMs));
    const refreshed = await jobRepo.getByIdForScope({
      id: job.id,
      projectId: job.projectId,
      teamId: job.teamId,
    });
    if (!refreshed) {
      return { job: current, timedOut: false };
    }
    current = refreshed;
    if (TERMINAL_JOB_STATUSES.includes(refreshed.status)) {
      return { job: refreshed, timedOut: false };
    }
  }
  return { job: current, timedOut: true };
}

export class ServerV1PostgresRoutes implements RouteHandler {
  private readonly ingestEvents: IngestEventsService;
  private readonly endSession: EndSessionService;

  constructor(private readonly options: ServerV1PostgresRoutesOptions) {
    this.ingestEvents = new IngestEventsService({
      pool: options.pool,
      resolveEventQueue: () => this.resolveQueue('event') as never,
    });
    this.endSession = new EndSessionService({
      pool: options.pool,
      resolveSummaryQueue: () => this.resolveQueue('summary') as never,
    });
  }

  /**
   * Expose the shared services so other route handlers (e.g. the legacy
   * compat adapters in src/server/compat) can call the EXACT same code path
   * — never duplicate ingest/end logic across routes.
   */
  getIngestEventsService(): IngestEventsService {
    return this.ingestEvents;
  }

  getEndSessionService(): EndSessionService {
    return this.endSession;
  }

  setupRoutes(app: Application): void {
    // Phase 12 — request_id middleware MUST run before auth so the audit log
    // can carry a stable correlation id across "rejected at auth" and
    // "ingested" code paths. requestIdMiddleware is idempotent (it honors
    // an inbound X-Request-Id header) so registering it multiple times for
    // overlapping route trees would still produce one canonical id per req.
    app.use('/v1', requestIdMiddleware());
    const baseWrite = requirePostgresServerAuth(this.options.pool, {
      authMode: this.options.authMode,
      allowLocalDevBypass: this.options.allowLocalDevBypass,
      requiredScopes: ['memories:write'],
    });
    const baseRead = requirePostgresServerAuth(this.options.pool, {
      authMode: this.options.authMode,
      allowLocalDevBypass: this.options.allowLocalDevBypass,
      requiredScopes: ['memories:read'],
    });
    // Paid-readiness guards, all opt-in via env so default behavior is unchanged
    // (empty array → readAuth/writeAuth are just the base auth). Express accepts
    // a middleware array wherever a single handler goes, so the per-route
    // registrations below need no changes. Order after auth: rate limit → quota
    // → meter, so the request is counted only once it's admitted.
    const guards: RequestHandler[] = [];
    const ratePerMin = Number(process.env.CLAUDE_MEM_RATE_LIMIT_PER_MIN ?? '0');
    if (ratePerMin > 0) guards.push(requireRateLimit(this.options.pool, { windowSec: 60, max: ratePerMin }));
    const monthlyCap = Number(process.env.CLAUDE_MEM_MONTHLY_REQUEST_CAP ?? '0');
    if (monthlyCap > 0) guards.push(requireMonthlyQuota(this.options.pool, { kind: 'request', cap: monthlyCap }));
    if (process.env.CLAUDE_MEM_USAGE_METERING === '1') guards.push(meterRequests(this.options.pool));
    // A monthly TOKEN cap gates writes only (ingestion drives generation = token
    // spend); reads stay available so a team over budget can still recall.
    const writeGuards: RequestHandler[] = [...guards];
    const tokenCap = Number(process.env.CLAUDE_MEM_MONTHLY_TOKEN_CAP ?? '0');
    if (tokenCap > 0) writeGuards.push(requireMonthlyQuota(this.options.pool, { kind: 'tokens', cap: tokenCap }));
    const writeAuth: RequestHandler[] = [baseWrite, ...writeGuards];
    const readAuth: RequestHandler[] = [baseRead, ...guards];

    // GET /v1/usage — per-kind usage totals for the caller's team this month.
    app.get('/v1/usage', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const monthStart = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth(), 1));
      const usageRepo = new PostgresUsageRepository(this.options.pool);
      const now = new Date();
      const dayStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
      const [usage, today] = await Promise.all([
        usageRepo.summarize({ teamId, since: monthStart }),
        usageRepo.summarize({ teamId, since: dayStart }),
      ]);
      res.status(200).json({ since: monthStart.toISOString(), usage, todaySince: dayStart.toISOString(), today });
    }));

    // POST /v1/keys — mint an optionally-expiring API key for the caller's team
    // and return the ready-to-paste connect command. READ-ONLY unless `scopes`
    // asks for write as well. Gated by writeAuth: the caller can already write
    // the team's memory, so a read or write key is never more than it holds,
    // and a read key cannot escalate itself into more keys. The raw key is
    // shown exactly once.
    app.post('/v1/keys', writeAuth, this.handleCreate(
      z.object({
        label: z.string().max(120).optional(),
        expiresInDays: z.number().int().positive().max(365).optional(),
        scopes: z.array(z.enum(MINTABLE_KEY_SCOPES)).min(1).optional(),
      }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        const scopes = [...new Set(body.scopes ?? ['memories:read'])];
        const raw = `cm_${randomBytes(24).toString('hex')}`;
        const keyHash = createHash('sha256').update(raw).digest('hex');
        const expiresAt = body.expiresInDays
          ? new Date(Date.now() + body.expiresInDays * 86_400_000)
          : null;
        const key = await new PostgresAuthRepository(this.options.pool).createApiKey({
          keyHash,
          teamId,
          projectId: req.authContext?.projectId ?? null,
          actorId: req.authContext?.apiKeyId ?? 'api',
          scopes,
          expiresAt,
          label: body.label ?? null,
        });
        await this.auditWrite(req, 'api_key.created', key.id, req.authContext?.projectId ?? null, { scopes });
        const mcpUrl = mcpConnectUrl(req);
        res.status(201).json({
          id: key.id,
          apiKey: raw, // shown ONCE — store it now
          label: body.label?.trim() || null,
          scopes,
          expiresAt: expiresAt?.toISOString() ?? null,
          mcpUrl,
          connectCommand: mcpConnectCommand(mcpUrl, raw),
        });
      },
    ));

    // GET /v1/connect — the paste-ready MCP connect command (placeholder key, so
    // a GET never mints). Use POST /v1/keys to get a real read-only key.
    app.get('/v1/connect', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const mcpUrl = mcpConnectUrl(req);
      res.status(200).json({
        mcpUrl,
        connectCommand: mcpConnectCommand(mcpUrl, '<YOUR_API_KEY>'),
        hint: 'POST /v1/keys (write scope) to mint a read-only key for this link.',
      });
    }));

    // GET /v1/whoami — what the presented key is: its team, its project scope
    // (null for a team-wide key) and its scopes. A UI holding only a pasted
    // key needs this before it can address any project-scoped route.
    app.get('/v1/whoami', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      res.status(200).json({
        apiKeyId: req.authContext?.apiKeyId ?? null,
        teamId,
        projectId: req.authContext?.projectId ?? null,
        scopes: req.authContext?.scopes ?? [],
      });
    }));

    // GET /v1/keys — the team's API keys, metadata only. key_hash is never
    // selected, so no response can carry key material. A project-scoped caller
    // sees only that project's keys. writeAuth, like minting: a pasted
    // read-only recall link must not be able to enumerate the team's keys.
    app.get('/v1/keys', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectScope = req.authContext?.projectId ?? null;
      const result = await this.options.pool.query<ApiKeyListRow>(
        `
          SELECT id, project_id, actor_id, label, scopes, revoked_at, expires_at, last_used_at, created_at
          FROM api_keys
          WHERE team_id = $1 AND ($2::text IS NULL OR project_id = $2)
          ORDER BY created_at DESC
          LIMIT 500
        `,
        [teamId, projectScope],
      );
      res.status(200).json({
        keys: result.rows.map(row => serializeApiKeyListEntry(row, req.authContext?.apiKeyId ?? null)),
      });
    }));

    // DELETE /v1/keys/:id — revoke a key. Idempotent on an already-revoked key.
    // A key cannot revoke itself: that would lock the caller out mid-request
    // with no key left to undo it.
    app.delete('/v1/keys/:id', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      if (id === req.authContext?.apiKeyId) {
        res.status(409).json({ error: 'Conflict', message: 'A key cannot revoke itself; use a different key' });
        return;
      }
      const projectScope = req.authContext?.projectId ?? null;
      const result = await this.options.pool.query<{ id: string }>(
        `
          UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()), updated_at = now()
          WHERE id = $1 AND team_id = $2 AND ($3::text IS NULL OR project_id = $3)
          RETURNING id
        `,
        [id, teamId, projectScope],
      );
      if (result.rows.length === 0) {
        res.status(404).json({ error: 'NotFound', message: 'API key not found' });
        return;
      }
      await this.auditWrite(req, 'api_key.revoked', id, projectScope, { via: 'api' });
      res.status(200).json({ revoked: true, id });
    }));

    // GET /v1/projects — the projects this key can read: every project of the
    // team, or just the one a project-scoped key is bound to.
    app.get('/v1/projects', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectScope = req.authContext?.projectId ?? null;
      const result = await this.options.pool.query<{
        id: string; name: string; created_at: Date; observation_count: string;
      }>(
        `
          SELECT projects.id, projects.name, projects.created_at,
            (SELECT count(*) FROM observations WHERE observations.project_id = projects.id) AS observation_count
          FROM projects
          WHERE projects.team_id = $1 AND ($2::text IS NULL OR projects.id = $2)
          ORDER BY projects.created_at ASC
        `,
        [teamId, projectScope],
      );
      res.status(200).json({
        projects: result.rows.map(row => ({
          id: row.id,
          name: row.name,
          createdAtEpoch: new Date(row.created_at).getTime(),
          observationCount: Number(row.observation_count),
        })),
      });
    }));

    // GET /v1/sessions?projectId= — a project's sessions, newest first, each
    // with how many observations it produced.
    app.get('/v1/sessions', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectId = typeof req.query.projectId === 'string' ? req.query.projectId : '';
      if (!projectId) {
        res.status(400).json({ error: 'ValidationError', message: 'projectId required' });
        return;
      }
      if (!this.ensureProjectAllowed(req, res, projectId)) return;
      const limit = clampListLimit(req.query.limit, SESSION_LIST_DEFAULT_LIMIT, SESSION_LIST_MAX_LIMIT);
      const result = await this.options.pool.query<SessionListRow>(
        `
          SELECT server_sessions.id,
            COALESCE(NULLIF(server_sessions.content_session_id, ''), server_sessions.external_session_id) AS content_session_id,
            server_sessions.platform_source, server_sessions.generation_status,
            server_sessions.metadata->>'project' AS folder,
            server_sessions.started_at, server_sessions.ended_at,
            (SELECT count(*) FROM observations WHERE observations.server_session_id = server_sessions.id) AS observation_count,
            (SELECT count(*) FROM agent_events WHERE agent_events.server_session_id = server_sessions.id) AS event_count
          FROM server_sessions
          WHERE server_sessions.project_id = $1 AND server_sessions.team_id = $2
          ORDER BY server_sessions.started_at DESC
          LIMIT $3
        `,
        [projectId, teamId, limit],
      );
      res.status(200).json({ sessions: result.rows.map(serializeSessionListEntry) });
    }));

    // GET /v1/folders?projectId= — the folders (observations.metadata.project,
    // the working directory a session ran in) a project's observations came
    // from, and the observation kinds in use, each with a count. One server
    // project usually holds several folders, so this is how a reader narrows it.
    app.get('/v1/folders', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectId = typeof req.query.projectId === 'string' ? req.query.projectId : '';
      if (!projectId) {
        res.status(400).json({ error: 'ValidationError', message: 'projectId required' });
        return;
      }
      if (!this.ensureProjectAllowed(req, res, projectId)) return;
      const [folders, kinds] = await Promise.all([
        this.options.pool.query<{ folder: string | null; count: string; last_at: Date }>(
          `
            SELECT metadata->>'project' AS folder, count(*) AS count, max(created_at) AS last_at
            FROM observations
            WHERE project_id = $1 AND team_id = $2
            GROUP BY 1
            ORDER BY max(created_at) DESC
          `,
          [projectId, teamId],
        ),
        this.options.pool.query<{ kind: string; count: string }>(
          `
            SELECT kind, count(*) AS count
            FROM observations
            WHERE project_id = $1 AND team_id = $2
            GROUP BY 1
            ORDER BY count(*) DESC
          `,
          [projectId, teamId],
        ),
      ]);
      res.status(200).json({
        folders: folders.rows.map(row => ({
          folder: row.folder,
          observationCount: Number(row.count),
          lastObservationAtEpoch: new Date(row.last_at).getTime(),
        })),
        kinds: kinds.rows.map(row => ({ kind: row.kind, observationCount: Number(row.count) })),
      });
    }));

    // GET /v1/memories/head?projectId= — how many observations a project has
    // and when the newest was created. Carries no content, so a page can poll
    // it for "is there anything new" without each poll being a recorded read.
    app.get('/v1/memories/head', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectId = typeof req.query.projectId === 'string' ? req.query.projectId : '';
      if (!projectId) {
        res.status(400).json({ error: 'ValidationError', message: 'projectId required' });
        return;
      }
      if (!this.ensureProjectAllowed(req, res, projectId)) return;
      const result = await this.options.pool.query<{ count: string; latest: Date | null }>(
        'SELECT count(*) AS count, max(created_at) AS latest FROM observations WHERE project_id = $1 AND team_id = $2',
        [projectId, teamId],
      );
      const row = result.rows[0];
      res.status(200).json({
        observationCount: Number(row?.count ?? 0),
        latestCreatedAtEpoch: row?.latest ? new Date(row.latest).getTime() : null,
      });
    }));

    // GET /v1/audit — the team's audit trail, newest first: who read, wrote,
    // deleted or minted what. writeAuth, because the trail itself says what
    // was searched for and by which key.
    app.get('/v1/audit', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectScope = req.authContext?.projectId ?? null;
      const limit = clampListLimit(req.query.limit, AUDIT_LIST_DEFAULT_LIMIT, AUDIT_LIST_MAX_LIMIT);
      const offset = clampListOffset(req.query.offset);
      const action = typeof req.query.action === 'string' && req.query.action ? req.query.action : null;
      const result = await this.options.pool.query<{
        id: string; project_id: string | null; actor_id: string | null; api_key_id: string | null;
        key_label: string | null; action: string; resource_type: string; resource_id: string | null;
        details: unknown; created_at: Date;
      }>(
        `
          SELECT audit_log.id, audit_log.project_id, audit_log.actor_id, audit_log.api_key_id,
            api_keys.label AS key_label, audit_log.action, audit_log.resource_type,
            audit_log.resource_id, audit_log.details, audit_log.created_at
          FROM audit_log
          LEFT JOIN api_keys ON api_keys.id = audit_log.api_key_id
          WHERE audit_log.team_id = $1
            AND ($2::text IS NULL OR audit_log.project_id = $2)
            AND ($3::text IS NULL OR audit_log.action = $3)
          ORDER BY audit_log.created_at DESC, audit_log.id DESC
          LIMIT $4 OFFSET $5
        `,
        [teamId, projectScope, action, limit + 1, offset],
      );
      const page = result.rows.slice(0, limit);
      res.status(200).json({
        entries: page.map(row => ({
          id: row.id,
          projectId: row.project_id,
          actorId: row.actor_id,
          apiKeyId: row.api_key_id,
          apiKeyLabel: row.key_label,
          action: row.action,
          resourceType: row.resource_type,
          resourceId: row.resource_id,
          details: row.details && typeof row.details === 'object' ? row.details : {},
          createdAtEpoch: new Date(row.created_at).getTime(),
        })),
        limit,
        offset,
        hasMore: result.rows.length > limit,
      });
    }));

    // GET /v1/memories?projectId= — page through a project's observations,
    // newest first, optionally one session's. The browse path: /v1/search and
    // /v1/context answer a question, this lists what is stored.
    app.get('/v1/memories', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectId = typeof req.query.projectId === 'string' ? req.query.projectId : '';
      if (!projectId) {
        res.status(400).json({ error: 'ValidationError', message: 'projectId required' });
        return;
      }
      if (!this.ensureProjectAllowed(req, res, projectId)) return;
      const limit = clampListLimit(req.query.limit, MEMORY_LIST_DEFAULT_LIMIT, MEMORY_LIST_MAX_LIMIT);
      const offset = clampListOffset(req.query.offset);
      const serverSessionId = typeof req.query.serverSessionId === 'string' && req.query.serverSessionId
        ? req.query.serverSessionId
        : null;
      const folder = typeof req.query.folder === 'string' && req.query.folder ? req.query.folder : null;
      const kind = typeof req.query.kind === 'string' && req.query.kind ? req.query.kind : null;
      // Epoch milliseconds: createdAfter is inclusive, createdBefore exclusive.
      const createdAfter = parseEpochQuery(req.query.createdAfter);
      const createdBefore = parseEpochQuery(req.query.createdBefore);
      // One row past the page tells the caller whether another page exists
      // without a second count query.
      const rows = await new PostgresObservationRepository(this.options.pool).listByProject({
        projectId, teamId, serverSessionId, folder, kind, createdAfter, createdBefore, limit: limit + 1, offset,
      });
      const page = rows.slice(0, limit);
      await this.auditWrite(req, 'observation.read', null, projectId, {
        mode: 'list', limit, offset, serverSessionId, folder, kind,
        resultCount: page.length, observationIds: page.map(o => o.id),
      });
      res.status(200).json({
        observations: page.map(serializeObservation),
        limit,
        offset,
        hasMore: rows.length > limit,
      });
    }));

    // POST /v1/events — single event with optional async generation
    app.post('/v1/events', writeAuth, this.asyncHandler(async (req, res) => {
      const parsedQuery = EVENT_QUERY_SCHEMA.safeParse(req.query);
      if (!parsedQuery.success) {
        res.status(400).json({ error: 'ValidationError', issues: parsedQuery.error.issues });
        return;
      }
      const generate = parsedQuery.data.generate !== 'false';
      const wait = parsedQuery.data.wait === 'true';

      const result = CreateAgentEventSchema.safeParse(req.body);
      if (!result.success) {
        res.status(400).json({ error: 'ValidationError', issues: result.error.issues });
        return;
      }
      const body = result.data;
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      if (!this.ensureProjectAllowed(req, res, body.projectId)) return;

      const insertInput = this.toAgentEventInput(body, teamId);
      await this.applyContentSessionLinks([insertInput], [req.body], teamId);
      let event: PostgresAgentEvent;
      let outbox: PostgresObservationGenerationJob | null = null;
      let enqueueState: EnqueueOutcome = 'skipped';
      const ingestOptions = {
        generate,
        source: 'http_post_v1_events',
        apiKeyId: req.authContext?.apiKeyId ?? null,
        actorId: await this.resolveActorId(req),
        sourceAdapter: insertInput.sourceAdapter,
        requestId: req.requestId ?? null,
      };
      try {
        const result = await this.ingestEvents.ingestOne(insertInput, ingestOptions);
        event = result.event;
        outbox = result.outbox;
        enqueueState = result.enqueueState;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'event.write ingest failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'event.write');
        return;
      }

      await this.auditWrite(req, 'event.received', event.id, event.projectId, {
        sourceAdapter: event.sourceAdapter,
        sourceEventId: event.sourceEventId,
        eventType: event.eventType,
        serverSessionId: event.serverSessionId,
        generationJobId: outbox?.id ?? null,
      });

      if (wait) {
        let resolved = outbox;
        let waitTimedOut = false;
        if (outbox) {
          const jobRepo = new PostgresObservationGenerationJobRepository(this.options.pool);
          const result = await waitForTerminalJob(jobRepo, outbox);
          resolved = result.job;
          waitTimedOut = result.timedOut;
        }
        res.status(201).json({
          event: serializeEvent(event),
          generationJob: resolved ? serializeJobStatusResponse(resolved, enqueueState) : null,
          ...(waitTimedOut ? { waitTimedOut: true } : {}),
        });
        return;
      }

      res.status(201).json({
        event: serializeEvent(event),
        ...(outbox
          ? { generationJob: serializeGenerationJob(outbox, enqueueState) }
          : {}),
      });
    }));

    // POST /v1/events/batch — pre-validate, atomic insert, then enqueue
    app.post('/v1/events/batch', writeAuth, this.asyncHandler(async (req, res) => {
      const parsedQuery = EVENT_QUERY_SCHEMA.safeParse(req.query);
      if (!parsedQuery.success) {
        res.status(400).json({ error: 'ValidationError', issues: parsedQuery.error.issues });
        return;
      }
      const generate = parsedQuery.data.generate !== 'false';
      const wait = parsedQuery.data.wait === 'true';

      const batchSchema = z.array(CreateAgentEventSchema).min(1).max(500);
      const result = batchSchema.safeParse(req.body);
      if (!result.success) {
        res.status(400).json({ error: 'ValidationError', issues: result.error.issues });
        return;
      }
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const failure = preValidateBatch(req, result.data);
      if (failure) {
        res.status(failure.status).json(failure.body);
        return;
      }

      const inputs = result.data.map(item => this.toAgentEventInput(item, teamId));
      await this.applyContentSessionLinks(
        inputs,
        Array.isArray(req.body) ? req.body : result.data,
        teamId,
      );

      let inserted: { event: PostgresAgentEvent; outbox: PostgresObservationGenerationJob | null }[] = [];
      let enqueueResults: EnqueueOutcome[] = [];
      const batchIngestOptions = {
        generate,
        source: 'http_post_v1_events_batch',
        apiKeyId: req.authContext?.apiKeyId ?? null,
        actorId: await this.resolveActorId(req),
        // Do not pick a single adapter for the whole batch. ingestBatch
        // builds each event's BullMQ payload via buildEventBullmqPayload,
        // which falls back to event.sourceAdapter when this opt is null —
        // so a mixed batch (e.g. 'mcp' + 'api') keeps per-event metadata
        // accurate in both the persisted outbox payload and the audit row.
        sourceAdapter: null,
        requestId: req.requestId ?? null,
      };
      try {
        const ingested = await this.ingestEvents.ingestBatch(inputs, batchIngestOptions);
        inserted = ingested.map(({ event, outbox }) => ({ event, outbox }));
        enqueueResults = ingested.map(({ enqueueState }) => enqueueState);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'event.batch_write ingest failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'event.batch_write');
        return;
      }

      await this.auditWrite(req, 'event.batch_received', null, null, {
        eventCount: inserted.length,
        generationJobIds: inserted.map(({ outbox }) => outbox?.id ?? null).filter(Boolean),
      });

      if (wait) {
        const jobRepo = new PostgresObservationGenerationJobRepository(this.options.pool);
        const waitDeadline = Date.now() + WAIT_TIMEOUT_MS;
        const resolved: { event: PostgresAgentEvent; outbox: PostgresObservationGenerationJob | null; timedOut: boolean }[] = [];
        for (const item of inserted) {
          if (!item.outbox) {
            resolved.push({ event: item.event, outbox: null, timedOut: false });
            continue;
          }
          const remaining = Math.max(0, waitDeadline - Date.now());
          const result = await waitForTerminalJob(jobRepo, item.outbox, remaining);
          resolved.push({ event: item.event, outbox: result.job, timedOut: result.timedOut });
        }
        const anyTimedOut = resolved.some(r => r.timedOut);
        res.status(201).json({
          events: resolved.map(({ event, outbox, timedOut }, index) => ({
            event: serializeEvent(event),
            generationJob: outbox
              ? serializeJobStatusResponse(outbox, enqueueResults[index]!)
              : null,
            ...(timedOut ? { waitTimedOut: true } : {}),
          })),
          ...(anyTimedOut ? { waitTimedOut: true } : {}),
        });
        return;
      }

      res.status(201).json({
        events: inserted.map(({ event, outbox }, index) => ({
          event: serializeEvent(event),
          ...(outbox
            ? { generationJob: serializeGenerationJob(outbox, enqueueResults[index]!) }
            : {}),
        })),
      });
    }));

    // GET /v1/events/:id — scoped read
    app.get('/v1/events/:id', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const eventsRepo = new PostgresAgentEventsRepository(this.options.pool);
      const fullEvent = await this.loadScopedById(req, res, {
        id,
        teamId,
        table: 'agent_events',
        notFound: 'Event not found',
        load: (projectId) => eventsRepo.getByIdForScope({ id, projectId, teamId }),
      });
      if (!fullEvent) return;
      res.json({ event: serializeEvent(fullEvent) });
    }));

    // GET /v1/events/:id/observations — list observations linked to event via observation_sources.
    // Scope is enforced by joining observations.team_id = $teamId and the
    // event ownership check before any rows are returned. Cross-tenant
    // requests are reported as 404 to avoid revealing existence.
    app.get('/v1/events/:id/observations', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);

      const eventResult = await this.options.pool.query(
        `SELECT id, project_id FROM agent_events WHERE id = $1 AND team_id = $2`,
        [id, teamId],
      );
      const eventRow = eventResult.rows[0] as undefined | { id: string; project_id: string };
      if (!eventRow) {
        res.status(404).json({ error: 'NotFound', message: 'Event not found' });
        return;
      }
      if (!this.ensureProjectAllowed(req, res, eventRow.project_id)) return;

      const obsResult = await this.options.pool.query(
        `
          SELECT o.id, o.project_id, o.team_id, o.server_session_id, o.kind, o.content,
                 o.metadata, o.generation_key, o.created_by_job_id, o.created_at, o.updated_at,
                 os.id AS source_id_pk, os.source_type, os.source_id, os.generation_job_id, os.created_at AS source_created_at
          FROM observation_sources os
          INNER JOIN observations o ON o.id = os.observation_id
          WHERE os.source_type = 'agent_event'
            AND os.source_id = $1
            AND o.team_id = $2
            AND o.project_id = $3
          ORDER BY o.created_at ASC
        `,
        [eventRow.id, teamId, eventRow.project_id],
      );

      await this.auditWrite(req, 'observation.read', eventRow.id, eventRow.project_id, {
        mode: 'event_observations',
        eventId: eventRow.id,
        resultCount: obsResult.rows.length,
        observationIds: obsResult.rows.map(r => r.id),
      });

      res.json({
        eventId: eventRow.id,
        observations: obsResult.rows.map(serializeObservationWithSource),
      });
    }));

    // Phase 11 — team-scoped queue listing. The api key MUST be bound to this
    // team OR a project owned by this team. We never let a project-scoped key
    // read a sibling project's jobs even if it has team-level read scope, so
    // we fall through to a project-only filter when projectId is set on the
    // key. Cross-team requests return 404 to avoid leaking team existence.
    app.get('/v1/teams/:teamId/jobs', readAuth, this.asyncHandler(async (req, res) => {
      const callerTeamId = this.requireTeamId(req, res);
      if (!callerTeamId) return;
      const targetTeamId = this.routeParam(req.params.teamId);
      if (!targetTeamId) {
        res.status(400).json({ error: 'ValidationError', message: 'teamId required' });
        return;
      }
      if (targetTeamId !== callerTeamId) {
        // Don't leak existence — return 404 not 403.
        res.status(404).json({ error: 'NotFound', message: 'Team not found' });
        return;
      }
      const callerProjectId = req.authContext?.projectId ?? null;
      const { status, limit, offset } = parseJobListingQuery(req);
      let jobs: JobListRow[] = [];
      let total = 0;
      try {
        ({ jobs, total } = await this.listJobsForScope({
          teamId: callerTeamId,
          projectId: callerProjectId,
          status,
          limit,
          offset,
        }));
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'team.jobs.list query failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'team.jobs.list');
        return;
      }
      await this.auditWrite(req, 'observation.read', null, callerProjectId, {
        mode: 'team_jobs',
        teamId: callerTeamId,
        projectId: callerProjectId,
        status,
        limit,
        offset,
        resultCount: jobs.length,
      });
      res.status(200).json({
        jobs: jobs.map(row => serializeJobListEntry(row)),
        total,
        limit,
        offset,
      });
    }));

    // Phase 11 — project-scoped queue listing. Project-scoped api keys MAY
    // read this; team-scoped keys MAY read any project under their team.
    // Cross-tenant requests are reported as 404, matching the rest of the
    // routes so existence is never inferable from response status.
    app.get('/v1/projects/:projectId/jobs', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectId = this.routeParam(req.params.projectId);
      if (!projectId) {
        res.status(400).json({ error: 'ValidationError', message: 'projectId required' });
        return;
      }
      // Verify the project actually belongs to this team. Cross-team
      // requests must look identical to "no such project" responses.
      const projectResult = await this.options.pool.query<{ id: string }>(
        'SELECT id FROM projects WHERE id = $1 AND team_id = $2',
        [projectId, teamId],
      );
      if (projectResult.rows.length === 0) {
        res.status(404).json({ error: 'NotFound', message: 'Project not found' });
        return;
      }
      // Project-scoped key must match the requested project; team-scoped key
      // (no projectId on the key) is allowed.
      const callerProjectId = req.authContext?.projectId ?? null;
      if (callerProjectId && callerProjectId !== projectId) {
        res.status(404).json({ error: 'NotFound', message: 'Project not found' });
        return;
      }

      const { status, limit, offset } = parseJobListingQuery(req);
      let jobs: JobListRow[] = [];
      let total = 0;
      try {
        ({ jobs, total } = await this.listJobsForScope({
          teamId,
          projectId,
          status,
          limit,
          offset,
        }));
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'project.jobs.list query failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'project.jobs.list');
        return;
      }
      await this.auditWrite(req, 'observation.read', null, projectId, {
        mode: 'project_jobs',
        teamId,
        projectId,
        status,
        limit,
        offset,
        resultCount: jobs.length,
      });
      res.status(200).json({
        jobs: jobs.map(row => serializeJobListEntry(row)),
        total,
        limit,
        offset,
      });
    }));

    // Phase 12 — GET /v1/jobs (generic, scoped). Project-scoped key sees its
    // project's jobs; team-scoped key sees the team's jobs. Filters: status,
    // source_type, limit, offset, since (ISO timestamp on created_at). The
    // BullMQ payload column is NEVER returned by default — even with admin
    // scope, the caller MUST opt in via `?include=payload`. This anti-pattern
    // guard prevents accidental exfil of sensitive event payloads.
    app.get('/v1/jobs', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const callerProjectId = req.authContext?.projectId ?? null;
      const includeRaw = typeof req.query.include === 'string' ? req.query.include : '';
      const includePayload = includeRaw.split(',').map(p => p.trim()).includes('payload');
      const callerScopes = req.authContext?.scopes ?? [];
      const isAdmin = callerScopes.includes('*') || callerScopes.includes('admin')
        || callerScopes.includes('memories:admin');
      if (includePayload && !isAdmin) {
        // Anti-pattern guard: refuse the include=payload elevation without
        // admin scope. Returning 403 (not silently stripping) makes the
        // attempted privilege escalation visible in the audit chain.
        res.status(403).json({
          error: 'Forbidden',
          message: '`include=payload` requires admin scope',
        });
        return;
      }
      const { status, sourceType, limit, offset, since } = parseGenericJobListingQuery(req);
      let jobs: JobListRow[] = [];
      let total = 0;
      try {
        ({ jobs, total } = await this.listJobsForScope({
          teamId, projectId: callerProjectId, status, sourceType, limit, offset, since,
        }));
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'jobs.list query failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'jobs.list');
        return;
      }
      await this.auditWrite(req, 'observation.read', null, callerProjectId, {
        mode: 'jobs_list',
        teamId,
        projectId: callerProjectId,
        status,
        sourceType,
        limit,
        offset,
        since: since ? since.toISOString() : null,
        resultCount: jobs.length,
        includePayload,
        requestId: req.requestId ?? null,
      });
      res.status(200).json({
        jobs: jobs.map(row => serializeJobListEntry(row, { includePayload })),
        total,
        limit,
        offset,
        requestId: req.requestId ?? null,
      });
    }));

    // GET /v1/jobs/summary — job counts by status for the key's scope, plus
    // when a job last completed and last failed. Registered ahead of
    // /v1/jobs/:id, which would otherwise read "summary" as a job id.
    app.get('/v1/jobs/summary', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectScope = req.authContext?.projectId ?? null;
      const result = await this.options.pool.query<{
        status: string; count: string; last_completed_at: Date | null; last_failed_at: Date | null;
      }>(
        `
          SELECT status, count(*) AS count,
            max(completed_at) AS last_completed_at, max(failed_at) AS last_failed_at
          FROM observation_generation_jobs
          WHERE team_id = $1 AND ($2::text IS NULL OR project_id = $2)
          GROUP BY status
        `,
        [teamId, projectScope],
      );
      const counts: Record<string, number> = { queued: 0, processing: 0, completed: 0, failed: 0, cancelled: 0 };
      let lastCompleted: number | null = null;
      let lastFailed: number | null = null;
      for (const row of result.rows) {
        counts[row.status] = Number(row.count);
        if (row.last_completed_at) lastCompleted = Math.max(lastCompleted ?? 0, new Date(row.last_completed_at).getTime());
        if (row.last_failed_at) lastFailed = Math.max(lastFailed ?? 0, new Date(row.last_failed_at).getTime());
      }
      // Why jobs are failing, what last generated successfully, and how long
      // capture-to-memory takes: the three things an operator checks when the
      // counts above look wrong.
      const [reasons, lastRun, latency] = await Promise.all([
        this.options.pool.query<{ reason: string | null; count: string }>(
          `
            SELECT last_error->>'reason' AS reason, count(*) AS count
            FROM observation_generation_jobs
            WHERE team_id = $1 AND ($2::text IS NULL OR project_id = $2) AND status = 'failed'
            GROUP BY 1
            ORDER BY count(*) DESC
            LIMIT 20
          `,
          [teamId, projectScope],
        ),
        this.options.pool.query<{ provider: string | null; model: string | null }>(
          `
            SELECT events.details->>'provider' AS provider, events.details->>'model' AS model
            FROM observation_generation_job_events events
            INNER JOIN observation_generation_jobs jobs ON jobs.id = events.generation_job_id
            WHERE jobs.team_id = $1 AND ($2::text IS NULL OR jobs.project_id = $2)
              AND events.event_type = 'completed'
            ORDER BY events.created_at DESC
            LIMIT 1
          `,
          [teamId, projectScope],
        ),
        this.options.pool.query<{ median_ms: string | null; sample: string }>(
          `
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS median_ms, count(*) AS sample
            FROM (
              SELECT extract(epoch FROM completed_at - created_at) * 1000 AS duration_ms
              FROM observation_generation_jobs
              WHERE team_id = $1 AND ($2::text IS NULL OR project_id = $2)
                AND status = 'completed' AND completed_at IS NOT NULL
              ORDER BY completed_at DESC
              LIMIT $3
            ) recent
          `,
          [teamId, projectScope, JOB_LATENCY_SAMPLE_SIZE],
        ),
      ]);
      res.status(200).json({
        counts,
        lastCompletedAtEpoch: lastCompleted,
        lastFailedAtEpoch: lastFailed,
        failedByReason: reasons.rows.map(row => ({ reason: row.reason, count: Number(row.count) })),
        lastGeneration: lastRun.rows[0]
          ? { provider: lastRun.rows[0].provider, model: lastRun.rows[0].model }
          : null,
        // Queued-to-completed, so it includes time spent waiting in the queue.
        captureToMemory: {
          medianMs: latency.rows[0]?.median_ms != null ? Math.round(Number(latency.rows[0].median_ms)) : null,
          sampleSize: Number(latency.rows[0]?.sample ?? 0),
        },
      });
    }));

    // POST /v1/jobs/retry-failed — requeue every failed job in the key's scope,
    // optionally only those that failed for one `reason`. For the morning after
    // a provider outage or an exhausted quota, when the failures number in the
    // dozens and share a cause. Capped per call; `remaining` says whether to
    // call again.
    app.post('/v1/jobs/retry-failed', writeAuth, this.handleCreate(
      z.object({ reason: z.string().min(1).max(500).optional() }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        const projectScope = req.authContext?.projectId ?? null;
        const failed = await this.options.pool.query<{ id: string; project_id: string }>(
          `
            SELECT id, project_id
            FROM observation_generation_jobs
            WHERE team_id = $1 AND ($2::text IS NULL OR project_id = $2) AND status = 'failed'
              AND ($3::text IS NULL OR last_error->>'reason' = $3)
            ORDER BY failed_at ASC NULLS LAST, created_at ASC
            LIMIT $4
          `,
          [teamId, projectScope, body.reason ?? null, BULK_RETRY_MAX_JOBS + 1],
        );
        const batch = failed.rows.slice(0, BULK_RETRY_MAX_JOBS);
        const repo = new PostgresObservationGenerationJobRepository(this.options.pool);
        let requeued = 0;
        for (const row of batch) {
          const current = await repo.getByIdForScope({ id: row.id, projectId: row.project_id, teamId });
          // Another request may have retried or cancelled it since the SELECT.
          if (!current || current.status !== 'failed') continue;
          if (await this.requeueGenerationJob(req, current, teamId)) requeued += 1;
        }
        res.status(200).json({
          requeued,
          remaining: failed.rows.length > BULK_RETRY_MAX_JOBS,
          requestId: req.requestId ?? null,
        });
      },
    ));

    // GET /v1/jobs/:id — generation job status, scoped to team/project
    app.get('/v1/jobs/:id', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const repo = new PostgresObservationGenerationJobRepository(this.options.pool);
      const job = await this.loadScopedById(req, res, {
        id,
        teamId,
        table: 'observation_generation_jobs',
        notFound: 'Generation job not found',
        scopeMismatch: 'not-found',
        load: (projectId) => repo.getByIdForScope({ id, projectId, teamId }),
      });
      if (!job) return;
      res.json({ generationJob: serializeGenerationJobStatus(job) });
    }));

    // Phase 12 — POST /v1/jobs/:id/retry. Idempotent operator action: if the
    // job is already queued the call is a no-op (no second BullMQ job is
    // enqueued). On failed/cancelled rows, transition back to queued, clear
    // locked_at/locked_by/failed_at/cancelled_at/last_error, increment a
    // retried_count metadata field for audit, and re-enqueue. The Phase 11
    // outbox idempotency key (team_id, project_id, source_type, source_id,
    // job_type) prevents observation duplication on the generator side.
    app.post('/v1/jobs/:id/retry', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const result = await this.retryGenerationJob(req, res, id, teamId);
      if (!result) return;
      res.status(200).json({
        generationJob: serializeGenerationJobStatus(result.job),
        retriedCount: result.retriedCount,
        alreadyQueued: result.alreadyQueued,
        requestId: req.requestId ?? null,
      });
    }));

    // Phase 12 — POST /v1/jobs/:id/cancel. Operator action: set status to
    // cancelled, set cancelled_at, append a lifecycle event, attempt to
    // remove the BullMQ job if still in flight. Future generator runs check
    // the Postgres status FIRST (Phase 11 lockOutbox guard) so a cancelled
    // job will never produce side effects even if BullMQ delivered it.
    app.post('/v1/jobs/:id/cancel', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const result = await this.cancelGenerationJob(req, res, id, teamId);
      if (!result) return;
      res.status(200).json({
        generationJob: serializeGenerationJobStatus(result.job),
        alreadyCancelled: result.alreadyCancelled,
        requestId: req.requestId ?? null,
      });
    }));

    // POST /v1/sessions/start — create-or-find a server_session, idempotent
    // on platform-scoped external session identity when platformSource is set.
    // Body matches the worker
    // /v1/sessions/start payload but stores into Postgres server_sessions.
    app.post('/v1/sessions/start', writeAuth, this.handleCreate(
      z.object({
        projectId: z.string().min(1),
        externalSessionId: z.string().min(1).optional(),
        contentSessionId: z.string().min(1).nullable().optional(),
        agentId: z.string().min(1).nullable().optional(),
        agentType: z.string().min(1).nullable().optional(),
        platformSource: z.string().min(1).nullable().optional(),
        metadata: z.record(z.string(), z.unknown()).optional(),
      }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        if (!this.ensureProjectAllowed(req, res, body.projectId)) return;
        const repo = new PostgresServerSessionsRepository(this.options.pool);
        const platformSource = normalizePlatformSourceOrNull(body.platformSource);
        try {
          if (body.externalSessionId) {
            const existing = await repo.findByExternalIdForScope({
              externalSessionId: body.externalSessionId,
              projectId: body.projectId,
              teamId,
              platformSource,
            });
            if (existing) {
              res.status(200).json({ session: serializeSession(existing) });
              return;
            }
          }
          const createInput = {
            projectId: body.projectId,
            teamId,
            externalSessionId: body.externalSessionId ?? null,
            contentSessionId: body.contentSessionId ?? null,
            agentId: body.agentId ?? null,
            agentType: body.agentType ?? null,
            platformSource,
            metadata: (body.metadata ?? {}) as Record<string, unknown>,
          };
          let session;
          try {
            session = await repo.create(createInput);
          } catch (error) {
            // Concurrent /v1/sessions/start with the same externalSessionId
            // can race past the findByExternalIdForScope check; the second
            // insert can hit a platform-scoped unique constraint. Refetch and
            // return the row inserted by the winner so legacy clients never
            // see a spurious 500.
            const pgCode = error instanceof Error
              ? (error as Error & { code?: string }).code
              : (error as { code?: string } | null)?.code;
            if (body.externalSessionId && pgCode === '23505') {
              const racedRow = await repo.findByExternalIdForScope({
                externalSessionId: body.externalSessionId,
                projectId: body.projectId,
                teamId,
                platformSource,
              });
              if (racedRow) {
                res.status(200).json({ session: serializeSession(racedRow) });
                return;
              }
            }
            throw error;
          }
          await this.auditWrite(req, 'session.write', session.id, session.projectId);
          res.status(201).json({ session: serializeSession(session) });
        } catch (error) {
          this.handleDbError(error, res, 'session.write');
        }
      },
    ));

    // GET /v1/sessions/:id — scoped read, 404 cross-tenant.
    app.get('/v1/sessions/:id', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const repo = new PostgresServerSessionsRepository(this.options.pool);
      const session = await this.loadScopedById(req, res, {
        id,
        teamId,
        table: 'server_sessions',
        notFound: 'Session not found',
        load: (projectId) => repo.getByIdForScope({ id, projectId, teamId }),
      });
      if (!session) return;
      res.json({ session: serializeSession(session) });
    }));

    // GET /v1/sessions/:id/events — the session's captured events, oldest
    // first, each with the ids of the observations generated from it. Payloads
    // are left out: they hold whole tool inputs and outputs, and
    // GET /v1/events/:id returns one when a reader asks for it.
    app.get('/v1/sessions/:id/events', readAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const repo = new PostgresServerSessionsRepository(this.options.pool);
      const session = await this.loadScopedById(req, res, {
        id,
        teamId,
        table: 'server_sessions',
        notFound: 'Session not found',
        load: (projectId) => repo.getByIdForScope({ id, projectId, teamId }),
      });
      if (!session) return;
      const limit = clampListLimit(req.query.limit, SESSION_EVENT_LIST_DEFAULT_LIMIT, SESSION_EVENT_LIST_MAX_LIMIT);
      const offset = clampListOffset(req.query.offset);
      const result = await this.options.pool.query<{
        id: string; event_type: string; tool_name: string | null; occurred_at: Date; observation_ids: string[] | null;
      }>(
        `
          SELECT agent_events.id, agent_events.event_type,
            agent_events.payload->>'tool_name' AS tool_name, agent_events.occurred_at,
            (SELECT array_agg(observation_sources.observation_id)
               FROM observation_sources
              WHERE observation_sources.agent_event_id = agent_events.id) AS observation_ids
          FROM agent_events
          WHERE agent_events.server_session_id = $1
            AND agent_events.project_id = $2
            AND agent_events.team_id = $3
          ORDER BY agent_events.occurred_at ASC, agent_events.id ASC
          LIMIT $4 OFFSET $5
        `,
        [session.id, session.projectId, teamId, limit + 1, offset],
      );
      const page = result.rows.slice(0, limit);
      res.status(200).json({
        events: page.map(row => ({
          id: row.id,
          eventType: row.event_type,
          toolName: row.tool_name,
          occurredAtEpoch: new Date(row.occurred_at).getTime(),
          observationIds: row.observation_ids ?? [],
        })),
        limit,
        offset,
        hasMore: result.rows.length > limit,
      });
    }));

    // POST /v1/sessions/:id/end — set ended_at (idempotent), enqueue a
    // session-summary generation job. Re-ending the same session is a no-op
    // because the (team_id, project_id, source_type='session_summary',
    // source_id) UNIQUE constraint on observation_generation_jobs prevents
    // duplicate rows; the existing row is returned.
    app.post('/v1/sessions/:id/end', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = this.routeParam(req.params.id);
      const projectId = await this.loadScopedById(req, res, {
        id,
        teamId,
        table: 'server_sessions',
        notFound: 'Session not found',
        load: async (rowProjectId) => rowProjectId,
      });
      if (!projectId) return;

      let endedSession: Awaited<ReturnType<PostgresServerSessionsRepository['endSession']>> = null;
      let summaryOutbox: PostgresObservationGenerationJob | null = null;
      let enqueueState: EnqueueOutcome = 'skipped';
      const endInput = {
        sessionId: id,
        projectId,
        teamId,
        source: 'http_post_v1_sessions_end',
        apiKeyId: req.authContext?.apiKeyId ?? null,
        actorId: await this.resolveActorId(req),
        sourceAdapter: 'api',
      };
      try {
        const result = await this.endSession.end(endInput);
        endedSession = result.session;
        summaryOutbox = result.outbox;
        enqueueState = result.enqueueState;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'session.end failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'session.end');
        return;
      }

      if (!endedSession) {
        res.status(404).json({ error: 'NotFound', message: 'Session not found' });
        return;
      }

      await this.auditWrite(req, 'session.end', endedSession.id, endedSession.projectId);

      res.status(200).json({
        session: serializeSession(endedSession),
        ...(summaryOutbox
          ? { generationJob: serializeGenerationJob(summaryOutbox, enqueueState) }
          : {}),
      });
    }));

    // POST /v1/memories — direct/manual observation insertion (compat alias).
    // MUST NOT call generator and MUST NOT create outbox rows.
    app.post('/v1/memories', writeAuth, this.handleCreate(
      z.object({
        projectId: z.string().min(1),
        serverSessionId: z.string().min(1).nullable().optional(),
        // Callers that write memories from inside a session know the agent's own
        // session id, not the server_sessions row UUID. /v1/events already accepts
        // contentSessionId and resolves it (#2634); without the same affordance here
        // every such memory lands with server_session_id NULL and can never be
        // grouped, scoped or compared by session.
        contentSessionId: z.string().min(1).nullable().optional(),
        // Two sessions in the same team/project can share a contentSessionId across
        // platforms; without the scope the lookup picks whichever started last, so a
        // Cursor memory can land on a Codex session. /v1/events already scopes this.
        platformSource: z.string().min(1).nullable().optional(),
        kind: z.string().min(1).optional(),
        content: z.string().min(1),
        metadata: z.record(z.string(), z.unknown()).optional(),
      }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        if (!this.ensureProjectAllowed(req, res, body.projectId)) return;
        const linkedSessionId = await this.resolveMemorySessionLink({
          serverSessionId: body.serverSessionId ?? null,
          contentSessionId: body.contentSessionId ?? null,
          projectId: body.projectId,
          teamId,
          // same normalization the ingest path applies, so an omitted field and an
          // explicit null keep meaning different things
          ...this.sessionLookupPlatformScope(req.body),
        });
        const createInput = {
          projectId: body.projectId,
          teamId,
          serverSessionId: linkedSessionId,
          kind: body.kind ?? 'manual',
          content: body.content,
          metadata: body.metadata ?? {},
        };
        try {
          const repo = new PostgresObservationRepository(this.options.pool);
          const observation = await repo.create(createInput);
          await this.auditWrite(req, 'memory.write', observation.id, observation.projectId);
          res.status(201).json({ memory: serializeObservation(observation) });
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          logger.warn('SYSTEM', 'memory.write failed', { requestId: req.requestId ?? null }, err);
          this.handleDbError(err, res, 'memory.write');
        }
      },
    ));

    // Phase 8 — full-text search over generated observations using the GIN
    // tsvector index. Results are ranked by ts_rank desc, then updated_at desc.
    // The MCP `observation_search` tool calls this endpoint via HTTP so the
    // single source of truth for the read path is the REST core.
    app.post('/v1/search', readAuth, this.handleCreate(
      z.object({
        projectId: z.string().min(1),
        query: z.string().min(1),
        limit: z.number().int().positive().max(100).optional(),
        platformSource: z.string().min(1).nullable().optional(),
      }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        if (!this.ensureProjectAllowed(req, res, body.projectId)) return;
        const platformSource = normalizePlatformSourceOrNull(body.platformSource);
        let results;
        try {
          const repo = new PostgresObservationRepository(this.options.pool);
          results = await repo.search({
            projectId: body.projectId,
            teamId,
            query: body.query,
            limit: body.limit ?? 20,
            platformSource,
          });
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          logger.warn('SYSTEM', 'observation.search failed', { requestId: req.requestId ?? null }, err);
          this.handleDbError(err, res, 'observation.search');
          return;
        }
        await this.auditWrite(req, 'observation.read', null, body.projectId, {
          mode: 'search',
          query: body.query,
          limit: body.limit ?? 20,
          platformSource,
          resultCount: results.length,
          observationIds: results.map(o => o.id),
        });
        res.status(200).json({
          observations: results.map(serializeObservation),
        });
      },
    ));

    // Phase 8 — context pack: same FTS path as `/v1/search`, but also returns
    // a concatenated context string for direct prompt injection. The MCP
    // `observation_context` tool calls this so MCP and any future REST
    // consumer share the exact same context-packing rule.
    app.post('/v1/context', readAuth, this.handleCreate(
      z.object({
        projectId: z.string().min(1),
        // Optional: a context request with no query asks for the most RECENT
        // observations, which is what a session-start block actually wants.
        query: z.string().min(1).optional(),
        // A recency read needs more rows than a relevance lookup, so the cap is
        // the CLAUDE_MEM_CONTEXT_OBSERVATIONS range and the default depends on
        // whether a query was given.
        limit: z.number().int().positive().max(SERVER_CONTEXT_MAX_LIMIT).optional(),
        platformSource: z.string().min(1).nullable().optional(),
        // Folder labels (observations.metadata.project). When set, only rows
        // generated for one of these folders are returned (ASCII
        // case-insensitive, like the SQLite read path).
        folderProjects: z.array(z.string().min(1)).min(1).max(20).optional(),
        // CLAUDE_MEM_CONTEXT_MAIN_AGENT_ONLY: leave out rows generated from
        // subagent hook events.
        excludeSubagents: z.boolean().optional(),
      }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        if (!this.ensureProjectAllowed(req, res, body.projectId)) return;
        const platformSource = normalizePlatformSourceOrNull(body.platformSource);
        const limit = body.limit
          ?? (body.query ? SERVER_CONTEXT_QUERY_DEFAULT_LIMIT : SERVER_CONTEXT_RECENT_DEFAULT_LIMIT);
        let results;
        try {
          const repo = new PostgresObservationRepository(this.options.pool);
          // One query for both modes, so the platform, folder and subagent
          // filters apply to the recency read exactly as to a relevance read.
          results = await repo.search({
            projectId: body.projectId,
            teamId,
            query: body.query ?? null,
            limit,
            platformSource,
            folderProjects: body.folderProjects ?? null,
            excludeSubagents: body.excludeSubagents === true,
          });
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          logger.warn('SYSTEM', 'observation.context failed', { requestId: req.requestId ?? null }, err);
          this.handleDbError(err, res, 'observation.context');
          return;
        }
        const context = results
          .map(observation => observation.content)
          .filter(text => typeof text === 'string' && text.length > 0)
          .join('\n\n');
        await this.auditWrite(req, 'observation.read', null, body.projectId, {
          mode: 'context',
          query: body.query ?? null,
          limit,
          platformSource,
          folderProjects: body.folderProjects ?? null,
          excludeSubagents: body.excludeSubagents === true,
          resultCount: results.length,
          observationIds: results.map(o => o.id),
        });
        res.status(200).json({
          observations: results.map(serializeObservation),
          context,
        });
      },
    ));

    // Remote authenticated MCP endpoint. The "secure MCP link" a user pastes
    // into Claude Code (or any MCP client) to recall their cloud memory:
    //   claude mcp add --transport http claude-mem <base>/v1/mcp \
    //     --header "Authorization: Bearer cm_..."
    // Same readAuth (memories:read) + team/project scoping + audit trail as
    // /v1/search, so it reads identical data through identical guards. Stateless
    // streamable-HTTP: one transport + server per request, bound to this key's team.
    const mcpHandler = this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectScope = req.authContext?.projectId ?? null;
      const repo = new PostgresObservationRepository(this.options.pool);
      const assertProjectAllowed = (projectId: string): void => {
        if (projectScope && projectScope !== projectId) {
          throw new Error('API key is scoped to a different project');
        }
      };
      const backend: RecallBackend = {
        search: async ({ projectId, query, limit }) => {
          assertProjectAllowed(projectId);
          const rows = await repo.search({ projectId, teamId, query, limit });
          // Audit the read, same as POST /v1/search — the MCP path is no exception.
          await this.auditWrite(req, 'observation.read', null, projectId, {
            mode: 'search', via: 'mcp', query, limit,
            resultCount: rows.length, observationIds: rows.map(o => o.id),
          });
          return rows.map(serializeObservation);
        },
        context: async ({ projectId, query, limit }) => {
          assertProjectAllowed(projectId);
          const rows = await repo.search({ projectId, teamId, query, limit });
          await this.auditWrite(req, 'observation.read', null, projectId, {
            mode: 'context', via: 'mcp', query, limit,
            resultCount: rows.length, observationIds: rows.map(o => o.id),
          });
          return rows.map(serializeObservation);
        },
        recent: async ({ projectId, limit }) => {
          assertProjectAllowed(projectId);
          const rows = await repo.listByProject({ projectId, teamId, limit });
          await this.auditWrite(req, 'observation.read', null, projectId, {
            mode: 'recent', via: 'mcp', limit,
            resultCount: rows.length, observationIds: rows.map(o => o.id),
          });
          return rows.map(serializeObservation);
        },
      };
      const server = createRecallMcpServer(backend, MCP_SERVER_VERSION);
      const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
      res.on('close', () => {
        void transport.close();
        void server.close();
      });
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    });
    // MCP streamable-HTTP only uses POST (JSON-RPC) and GET (SSE). Scope the
    // route to those instead of app.all, so DELETE/PUT/PATCH/OPTIONS don't run
    // auth + transport only to be rejected.
    app.post('/v1/mcp', readAuth, mcpHandler);
    app.get('/v1/mcp', readAuth, mcpHandler);

    // PATCH /v1/memories/:id — correct an observation's text or kind. The
    // search index follows `content` (a generated column), so an edit is
    // searchable at once. Scoped like the delete below.
    app.patch('/v1/memories/:id', writeAuth, this.handleCreate(
      z.object({
        content: z.string().min(1).optional(),
        kind: z.string().min(1).max(64).optional(),
      }).refine(body => body.content !== undefined || body.kind !== undefined, {
        message: 'content or kind required',
      }),
      async (req, res, body) => {
        const teamId = this.requireTeamId(req, res);
        if (!teamId) return;
        const id = String(req.params.id);
        const projectScope = req.authContext?.projectId ?? null;
        try {
          const result = await this.options.pool.query<{ project_id: string }>(
            `
              UPDATE observations
              SET content = COALESCE($4, content), kind = COALESCE($5, kind), updated_at = now()
              WHERE id = $1 AND team_id = $2 AND ($3::text IS NULL OR project_id = $3)
              RETURNING project_id
            `,
            [id, teamId, projectScope, body.content ?? null, body.kind ?? null],
          );
          const projectId = result.rows[0]?.project_id;
          if (!projectId) {
            res.status(404).json({ error: 'not_found' });
            return;
          }
          const updated = await new PostgresObservationRepository(this.options.pool)
            .getByIdForScope({ id, projectId, teamId });
          if (!updated) {
            res.status(404).json({ error: 'not_found' });
            return;
          }
          await this.auditWrite(req, 'observation.updated', id, projectId, {
            via: 'api',
            fields: Object.keys(body),
          });
          res.status(200).json({ memory: serializeObservation(updated) });
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          logger.warn('SYSTEM', 'observation.update failed', { requestId: req.requestId ?? null }, err);
          this.handleDbError(err, res, 'observation.update');
        }
      },
    ));

    // DELETE /v1/memories/:id — forget a single observation (sources cascade).
    app.delete('/v1/memories/:id', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const id = String(req.params.id);
      const projectScope = req.authContext?.projectId ?? null;
      try {
        const deleted = await this.deleteObservationForScope(id, teamId, projectScope);
        if (!deleted) {
          res.status(404).json({ error: 'not_found' });
          return;
        }
        await this.auditWrite(req, 'observation.deleted', id, projectScope, { via: 'api' });
        res.status(200).json({ deleted: true, id });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'observation.delete failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'observation.delete');
      }
    }));

    // DELETE /v1/projects/:projectId/memory — forget EVERYTHING captured for a
    // project (observations, raw events, sessions, jobs). Keeps the project shell.
    app.delete('/v1/projects/:projectId/memory', writeAuth, this.asyncHandler(async (req, res) => {
      const teamId = this.requireTeamId(req, res);
      if (!teamId) return;
      const projectId = String(req.params.projectId);
      if (!this.ensureProjectAllowed(req, res, projectId)) return;
      try {
        // ensureProjectAllowed only checks a key's *optional* project scope, so a
        // team-scoped key could otherwise purge any projectId. Confirm the project
        // belongs to this team before purging, and 404 if it doesn't — without this
        // a cross-team or nonexistent projectId returns 200 with zero counts,
        // misreporting an unauthorized purge as success.
        const project = await new PostgresProjectsRepository(this.options.pool).getByIdForTeam(projectId, teamId);
        if (!project) {
          res.status(404).json({ error: 'not_found' });
          return;
        }
        const counts = await new PostgresDataDeletionRepository(this.options.pool)
          .purgeProjectMemory({ projectId, teamId });
        await this.auditWrite(req, 'project.memory_purged', projectId, projectId, { ...counts, via: 'api' });
        res.status(200).json({ purged: true, projectId, counts });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'project.purge failed', { requestId: req.requestId ?? null }, err);
        this.handleDbError(err, res, 'project.purge');
      }
    }));
  }

  // Phase 11 — resolve actor identity for audit. We look up the api_keys row
  // by id and read its actor_id column. This MUST NOT be used for auth — it
  // is purely a denormalization for audit trails. If the lookup fails for
  // any reason we return null and let the audit row carry a missing actor.
  private async resolveActorId(req: Request): Promise<string | null> {
    const apiKeyId = req.authContext?.apiKeyId ?? null;
    if (!apiKeyId) return null;
    try {
      const result = await this.options.pool.query<{ actor_id: string | null }>(
        'SELECT actor_id FROM api_keys WHERE id = $1',
        [apiKeyId],
      );
      return result.rows[0]?.actor_id ?? null;
    } catch (error) {
      logger.warn('SYSTEM', 'failed to resolve actor_id for audit', {
        apiKeyId,
        error: error instanceof Error ? error.message : String(error),
      });
      return null;
    }
  }

  private resolveQueue(lane: 'summary' | 'event'): ReturnType<ActiveServerQueueManager['getQueue']> | null {
    const override = lane === 'summary' ? this.options.getSummaryQueue : this.options.getEventQueue;
    if (override) {
      return override();
    }
    const manager = this.options.queueManager as Partial<ActiveServerQueueManager>;
    if (typeof manager.getQueue === 'function') {
      try {
        return manager.getQueue(lane);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.warn('SYSTEM', 'queue lane resolution failed; enqueue will be skipped', { lane }, err);
        return null;
      }
    }
    return null;
  }

  private toAgentEventInput(body: z.infer<typeof CreateAgentEventSchema>, teamId: string): CreatePostgresAgentEventInput {
    const sourceAdapter = body.sourceType ?? SOURCE_ADAPTER_DEFAULT;
    const occurredAtEpoch = typeof body.occurredAtEpoch === 'number' ? body.occurredAtEpoch : Date.now();
    return {
      projectId: body.projectId,
      teamId,
      serverSessionId: body.serverSessionId ?? null,
      contentSessionId: body.contentSessionId ?? null,
      sourceAdapter,
      sourceEventId: typeof (body as Record<string, unknown>).sourceEventId === 'string'
        ? ((body as Record<string, unknown>).sourceEventId as string)
        : null,
      eventType: body.eventType,
      platformSource: normalizePlatformSourceOrNull(body.platformSource),
      payload: (body.payload ?? {}) as object,
      metadata: typeof (body as Record<string, unknown>).metadata === 'object'
        && (body as Record<string, unknown>).metadata !== null
        ? ((body as Record<string, unknown>).metadata as Record<string, unknown>)
        : {},
      occurredAt: new Date(occurredAtEpoch),
    };
  }

  private requireTeamId(req: Request, res: Response): string | null {
    const teamId = req.authContext?.teamId ?? null;
    if (!teamId) {
      res.status(403).json({ error: 'Forbidden', message: 'API key is not bound to a team' });
      return null;
    }
    return teamId;
  }


  /**
   * Resolve the server_sessions row for a memory write.
   *
   * Callers writing from inside a session know the agent's own session id, not the
   * server_sessions row UUID. /v1/events already accepts contentSessionId and resolves
   * it (#2634); without the same affordance on the memory path every such write lands
   * with server_session_id NULL and can never be grouped or scoped by session.
   *
   * Explicit serverSessionId always wins. The lookup is best-effort, mirroring the
   * ingest path: a failure must not fail the write — store unlinked, as before.
   * Unlike /v1/events there is no idempotency concern: observations key off `id`, and
   * server_session_id is a plain FK (ON DELETE SET NULL), so linking later cannot
   * drift a key.
   */
  private async resolveMemorySessionLink(input: {
    serverSessionId: string | null;
    contentSessionId: string | null;
    projectId: string;
    teamId: string;
    platformSource?: string | null;
  }): Promise<string | null> {
    if (input.serverSessionId) return input.serverSessionId;
    if (!input.contentSessionId) return null;
    const platformScope = Object.prototype.hasOwnProperty.call(input, 'platformSource')
      ? { platformSource: input.platformSource ?? null }
      : {};
    try {
      return await new PostgresServerSessionsRepository(this.options.pool)
        .findIdByContentSessionId({
          contentSessionId: input.contentSessionId,
          projectId: input.projectId,
          teamId: input.teamId,
          ...platformScope,
        });
    } catch (err) {
      logger.warn('HTTP', 'session linkage lookup failed; storing memory unlinked', {
        error: err instanceof Error ? err.message : String(err),
      });
      return null;
    }
  }

  private async applyContentSessionLinks(
    inputs: CreatePostgresAgentEventInput[],
    rawBodies: unknown[],
    teamId: string,
  ): Promise<void> {
    const repo = new PostgresServerSessionsRepository(this.options.pool);
    const lookups = new Map<string, Promise<string | null>>();

    await Promise.all(inputs.map(async (input, index) => {
      if (input.serverSessionId || !input.contentSessionId) return;

      const platformScope = this.sessionLookupPlatformScope(rawBodies[index]);
      const hasPlatformScope = Object.prototype.hasOwnProperty.call(platformScope, 'platformSource');
      const cacheKey = JSON.stringify([
        input.projectId,
        teamId,
        input.contentSessionId,
        hasPlatformScope,
        hasPlatformScope ? platformScope.platformSource ?? null : null,
      ]);
      let lookup = lookups.get(cacheKey);
      if (!lookup) {
        lookup = repo.findIdByContentSessionId({
          contentSessionId: input.contentSessionId,
          projectId: input.projectId,
          teamId,
          ...platformScope,
        }).catch((err: unknown) => {
          logger.warn('HTTP', 'session linkage lookup failed; storing event unlinked', {
            error: err instanceof Error ? err.message : String(err),
          });
          return null;
        });
        lookups.set(cacheKey, lookup);
      }

      const linkedId = await lookup;
      if (linkedId) input.serverSessionId = linkedId;
    }));
  }

  private sessionLookupPlatformScope(body: unknown): { platformSource?: string | null } {
    if (!body || typeof body !== 'object') return {};
    if (!Object.prototype.hasOwnProperty.call(body, 'platformSource')) return {};

    const value = (body as { platformSource?: unknown }).platformSource;
    return {
      platformSource: typeof value === 'string'
        ? normalizePlatformSource(value)
        : null,
    };
  }

  private ensureProjectAllowed(req: Request, res: Response, projectId: string): boolean {
    if (req.authContext?.projectId && req.authContext.projectId !== projectId) {
      res.status(403).json({ error: 'Forbidden', message: 'API key is scoped to a different project' });
      return false;
    }
    return true;
  }

  // Shared scoped-id lookup for the /:id routes. Resolves the row's project
  // via a team-scoped probe (404 cross-tenant to avoid revealing existence),
  // enforces the api key's project scope, then loads the full row. Routes
  // that must not disclose sibling-project existence pass
  // scopeMismatch: 'not-found' to answer 404 instead of 403.
  private async loadScopedById<T>(
    req: Request,
    res: Response,
    input: {
      id: string;
      teamId: string;
      table: 'agent_events' | 'server_sessions' | 'observation_generation_jobs';
      notFound: string;
      scopeMismatch?: 'not-found';
      load: (projectId: string) => Promise<T | null>;
    },
  ): Promise<T | null> {
    const probe = await this.options.pool.query(
      `SELECT project_id FROM ${input.table} WHERE id = $1 AND team_id = $2`,
      [input.id, input.teamId],
    );
    const row = probe.rows[0] as undefined | { project_id: string };
    if (!row) {
      res.status(404).json({ error: 'NotFound', message: input.notFound });
      return null;
    }
    if (input.scopeMismatch === 'not-found') {
      if (req.authContext?.projectId && req.authContext.projectId !== row.project_id) {
        res.status(404).json({ error: 'NotFound', message: input.notFound });
        return null;
      }
    } else if (!this.ensureProjectAllowed(req, res, row.project_id)) {
      return null;
    }
    const loaded = await input.load(row.project_id);
    if (!loaded) {
      res.status(404).json({ error: 'NotFound', message: input.notFound });
      return null;
    }
    return loaded;
  }

  // Scoped single-observation delete for DELETE /v1/memories/:id.
  // Project-scoped key deletes within its project; a team-scoped key
  // matches by id + team across the team's projects.
  private async deleteObservationForScope(
    id: string,
    teamId: string,
    projectScope: string | null,
  ): Promise<boolean> {
    const deletion = new PostgresDataDeletionRepository(this.options.pool);
    if (projectScope) {
      return deletion.deleteObservation({ id, projectId: projectScope, teamId });
    }
    const byTeam = await this.options.pool.query(
      `DELETE FROM observations WHERE id = $1 AND team_id = $2`,
      [id, teamId],
    );
    return (byTeam.rowCount ?? 0) > 0;
  }

  private handleDbError(error: unknown, res: Response, action: string): void {
    const message = error instanceof Error ? error.message : String(error);
    if (
      message.includes('project_id must belong to team_id')
      || message.includes('server_session_id must belong')
      || message.includes('agent_event source_id must belong')
    ) {
      res.status(403).json({ error: 'Forbidden', message });
      return;
    }
    logger.error('SYSTEM', `${action} failed`, { error: message });
    res.status(500).json({ error: 'InternalError', message: 'Failed to persist event' });
  }

  private async auditWrite(
    req: Request,
    action: string,
    targetId: string | null,
    projectId: string | null,
    details?: Record<string, unknown>,
  ): Promise<void> {
    const repo = new PostgresAuthRepository(this.options.pool);
    const actorId = await this.resolveActorId(req);
    // Phase 12 — every audit row carries request_id when one was minted
    // so dashboards and incident triage can pivot from a single HTTP
    // request to every ingest/job/audit row it produced. Caller-supplied
    // details win on key conflict so explicit overrides still work.
    const detailsWithRequestId: Record<string, unknown> = {
      ...(req.requestId ? { requestId: req.requestId } : {}),
      ...(details ?? {}),
    };
    const auditInput = {
      teamId: req.authContext?.teamId ?? null,
      projectId: projectId ?? req.authContext?.projectId ?? null,
      actorId,
      apiKeyId: req.authContext?.apiKeyId ?? null,
      action,
      resourceType: resolveAuditResourceType(action),
      resourceId: targetId,
      details: detailsWithRequestId,
    };
    try {
      await repo.createAuditLog(auditInput);
    } catch (error) {
      logger.warn('SYSTEM', 'audit log insert failed', {
        action,
        requestId: req.requestId ?? null,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  // Phase 11 — paginated job listing for team/project queue endpoints.
  // Phase 12 — extended with `sourceType`, `since`, and (optional) payload
  // selection. Filtering is enforced in SQL (WHERE team_id [, project_id,
  // status, source_type, created_at]). Application-layer filtering is never
  // trusted alone for tenant scope.
  private async listJobsForScope(input: {
    teamId: string;
    projectId: string | null;
    status: string | null;
    sourceType?: string | null;
    limit: number;
    offset: number;
    since?: Date | null;
  }): Promise<{ jobs: JobListRow[]; total: number }> {
    const params: Array<string | number | Date> = [input.teamId];
    let where = 'WHERE team_id = $1';
    if (input.projectId) {
      params.push(input.projectId);
      where += ` AND project_id = $${params.length}`;
    }
    if (input.status) {
      params.push(input.status);
      where += ` AND status = $${params.length}`;
    }
    if (input.sourceType) {
      params.push(input.sourceType);
      where += ` AND source_type = $${params.length}`;
    }
    if (input.since) {
      params.push(input.since);
      where += ` AND created_at >= $${params.length}`;
    }
    const totalResult = await this.options.pool.query<{ total: string }>(
      `SELECT COUNT(*)::text AS total FROM observation_generation_jobs ${where}`,
      params,
    );
    const total = Number.parseInt(totalResult.rows[0]?.total ?? '0', 10);
    params.push(input.limit, input.offset);
    const limitParamIndex = params.length - 1;
    const offsetParamIndex = params.length;
    const result = await this.options.pool.query<JobListRow>(
      `
        SELECT id, project_id, team_id, source_type, source_id, status, attempts,
               max_attempts, created_at, completed_at, failed_at, last_error, payload
        FROM observation_generation_jobs
        ${where}
        ORDER BY created_at DESC
        LIMIT $${limitParamIndex} OFFSET $${offsetParamIndex}
      `,
      params,
    );
    return { jobs: result.rows, total };
  }

  // Phase 12 — operator retry. Status handling:
  //   - queued: no-op (idempotent; no double enqueue)
  //   - processing: 409 — running worker MUST finish or fail naturally
  //   - completed: 409 — observations index dedupes on (job_id, index,
  //     content) but LLM output is non-deterministic, so a second run
  //     would persist a parallel set of observations. Operator must
  //     create a new generation request instead of retrying.
  //   - failed/cancelled: reset to queued, clear locks, bump retried_count
  //     in payload metadata for audit, then re-enqueue. The deterministic
  //     BullMQ jobId means a duplicate transport publish collapses on the
  //     queue side too.
  private async retryGenerationJob(
    req: Request,
    res: Response,
    id: string,
    teamId: string,
  ): Promise<{ job: PostgresObservationGenerationJob; retriedCount: number; alreadyQueued: boolean } | null> {
    if (!id) {
      res.status(400).json({ error: 'ValidationError', message: 'job id required' });
      return null;
    }
    // Scope check first — same NotFound disclosure as the rest of the routes.
    const repo = new PostgresObservationGenerationJobRepository(this.options.pool);
    const current = await this.loadScopedById(req, res, {
      id,
      teamId,
      table: 'observation_generation_jobs',
      notFound: 'Generation job not found',
      scopeMismatch: 'not-found',
      load: (projectId) => repo.getByIdForScope({ id, projectId, teamId }),
    });
    if (!current) return null;

    // Idempotent fast-path: already queued -> emit audit only, no DB writes.
    if (current.status === 'queued') {
      await this.auditWrite(req, 'generation_job.retried_by_operator', current.id, current.projectId, {
        outcome: 'noop_already_queued',
        currentAttempts: current.attempts,
        requestId: req.requestId ?? null,
      });
      return { job: current, retriedCount: extractRetriedCount(current.payload), alreadyQueued: true };
    }

    if (current.status === 'processing') {
      // Refuse retry on in-flight jobs — the running worker MUST be allowed
      // to finish or fail through its normal lifecycle. Operator can wait
      // or cancel, then retry.
      res.status(409).json({
        error: 'Conflict',
        message: 'Generation job is currently processing; cancel or wait for completion before retrying',
      });
      return null;
    }

    if (current.status === 'completed') {
      // Refuse retry on already-completed jobs. The deduplication index on
      // observations (generation_key = job_id + index + content) does NOT
      // protect against re-running the provider, because LLM output is
      // non-deterministic and the second run almost always produces a
      // different content string. Replaying would persist a parallel set
      // of observations attributed to the same generation_job_id.
      // cancelGenerationJob applies the same 409 guard for the same reason.
      res.status(409).json({
        error: 'Conflict',
        message: 'Generation job already completed; retrying would duplicate observations',
      });
      return null;
    }

    const requeued = await this.requeueGenerationJob(req, current, teamId);
    if (!requeued) {
      res.status(404).json({ error: 'NotFound', message: 'Generation job not found' });
      return null;
    }
    return { ...requeued, alreadyQueued: false };
  }

  // The requeue itself, shared by the single retry above and the bulk
  // POST /v1/jobs/retry-failed: `current` is a failed or cancelled job the
  // caller has already scope-checked. Returns null if the row vanished.
  private async requeueGenerationJob(
    req: Request,
    current: PostgresObservationGenerationJob,
    teamId: string,
  ): Promise<{ job: PostgresObservationGenerationJob; retriedCount: number } | null> {
    const id = current.id;
    // Reset to queued, clear lock + lifecycle timestamps, increment
    // retried_count for audit. attempts is intentionally preserved so the
    // BullMQ attempt cap is not bypassed; if the job hit max_attempts the
    // operator must lift the cap explicitly via a separate flow.
    //
    // current.payload is the canonical BullMQ payload persisted at outbox
    // create time (kind/team_id/project_id/source_type/source_id/
    // generation_job_id/api_key_id/actor_id/source_adapter/request_id).
    // The retry adds operator metadata to the persisted row but enqueues
    // ONLY the BullMQ payload — the worker calls
    // assertServerGenerationJobPayload(job.data) on receipt and would reject
    // the metadata-only object the previous implementation handed it.
    const retriedCount = extractRetriedCount(current.payload) + 1;
    const persistedBullmqPayload = (current.payload && typeof current.payload === 'object'
      ? current.payload
      : {}) as Record<string, unknown>;
    const newPayload = {
      ...persistedBullmqPayload,
      retried_count: retriedCount,
      last_retried_by_actor: req.authContext?.apiKeyId ?? null,
      last_retried_request_id: req.requestId ?? null,
    };
    // The payload we re-publish to BullMQ on retry: refresh request_id (so
    // the worker logs/audit attribute this run to the operator's request)
    // but keep all canonical job context that the worker validates against.
    const retryBullmqPayload = {
      ...persistedBullmqPayload,
      request_id: req.requestId ?? (persistedBullmqPayload as { request_id?: unknown }).request_id ?? null,
    };
    const updated = await this.options.pool.query(
      `
        UPDATE observation_generation_jobs
        SET status = 'queued',
            locked_at = NULL,
            locked_by = NULL,
            failed_at = NULL,
            cancelled_at = NULL,
            completed_at = NULL,
            last_error = NULL,
            attempts = LEAST(attempts, max_attempts - 1),
            payload = $4::jsonb,
            updated_at = now()
        WHERE id = $1 AND project_id = $2 AND team_id = $3
        RETURNING *
      `,
      [id, current.projectId, teamId, JSON.stringify(newPayload)],
    );
    const updatedRow = updated.rows[0];
    if (!updatedRow) return null;

    // Append lifecycle event so the audit chain mirrors the lifecycle tracker.
    const eventsRepo = new PostgresObservationGenerationJobEventsRepository(this.options.pool);
    await eventsRepo.append({
      generationJobId: id,
      projectId: current.projectId,
      teamId,
      eventType: 'queued',
      statusAfter: 'queued',
      attempt: (updatedRow as { attempts: number }).attempts,
      details: {
        source: 'operator_retry',
        requestId: req.requestId ?? null,
        retriedCount,
      },
    });

    // Re-enqueue to BullMQ. If the queue is unavailable we leave the row in
    // queued state and reconciliation will publish it on next startup —
    // never lie about "enqueued" when we couldn't publish.
    const queue = this.resolveEventQueueForRetry(updatedRow as { source_type: string });
    if (queue && updatedRow) {
      try {
        const bullmqJobId = (updatedRow as { bullmq_job_id: string | null }).bullmq_job_id;
        if (bullmqJobId) {
          // Best effort remove first so a terminal-state slot doesn't block.
          try { await queue.remove(bullmqJobId); } catch { /* terminal slot may be missing — ok */ }
          await queue.add(bullmqJobId, retryBullmqPayload as never);
        }
      } catch (error) {
        logger.warn('SYSTEM', 'failed to re-enqueue generation job on operator retry', {
          jobId: id,
          requestId: req.requestId ?? null,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    const refreshed = await new PostgresObservationGenerationJobRepository(this.options.pool).getByIdForScope({ id, projectId: current.projectId, teamId });
    if (!refreshed) return null;

    await this.auditWrite(req, 'generation_job.retried_by_operator', refreshed.id, refreshed.projectId, {
      previousStatus: current.status,
      currentStatus: refreshed.status,
      retriedCount,
      requestId: req.requestId ?? null,
    });

    return { job: refreshed, retriedCount };
  }

  // Phase 12 — operator cancel. Idempotent: a job already in `cancelled`
  // status is a no-op. Active processing rows are still cancelled but the
  // running worker is allowed to finish; Phase 11's lockOutbox guard
  // re-checks Postgres status before any side effect, so a cancelled job
  // will not produce observations even if the BullMQ delivery raced.
  private async cancelGenerationJob(
    req: Request,
    res: Response,
    id: string,
    teamId: string,
  ): Promise<{ job: PostgresObservationGenerationJob; alreadyCancelled: boolean } | null> {
    if (!id) {
      res.status(400).json({ error: 'ValidationError', message: 'job id required' });
      return null;
    }
    const repo = new PostgresObservationGenerationJobRepository(this.options.pool);
    const current = await this.loadScopedById(req, res, {
      id,
      teamId,
      table: 'observation_generation_jobs',
      notFound: 'Generation job not found',
      scopeMismatch: 'not-found',
      load: (projectId) => repo.getByIdForScope({ id, projectId, teamId }),
    });
    if (!current) return null;
    if (current.status === 'cancelled') {
      await this.auditWrite(req, 'generation_job.cancelled_by_operator', current.id, current.projectId, {
        outcome: 'noop_already_cancelled',
        requestId: req.requestId ?? null,
      });
      return { job: current, alreadyCancelled: true };
    }
    if (current.status === 'completed') {
      res.status(409).json({
        error: 'Conflict',
        message: 'Generation job already completed; cannot cancel',
      });
      return null;
    }

    const updateResult = await this.options.pool.query(
      `
        UPDATE observation_generation_jobs
        SET status = 'cancelled',
            cancelled_at = now(),
            updated_at = now()
        WHERE id = $1 AND project_id = $2 AND team_id = $3
        RETURNING *
      `,
      [id, current.projectId, teamId],
    );
    const updatedRow = updateResult.rows[0];
    if (!updatedRow) {
      res.status(404).json({ error: 'NotFound', message: 'Generation job not found' });
      return null;
    }

    const eventsRepo = new PostgresObservationGenerationJobEventsRepository(this.options.pool);
    await eventsRepo.append({
      generationJobId: id,
      projectId: current.projectId,
      teamId,
      eventType: 'cancelled',
      statusAfter: 'cancelled',
      attempt: (updatedRow as { attempts: number }).attempts,
      details: {
        source: 'operator_cancel',
        requestId: req.requestId ?? null,
      },
    });

    // Best-effort BullMQ removal so a delayed/waiting job stops occupying
    // the slot. Active jobs cannot be removed; the lockOutbox status check
    // (Phase 11) is the authoritative side-effect guard.
    const queue = this.resolveEventQueueForRetry(updatedRow as { source_type: string });
    if (queue) {
      const bullmqJobId = (updatedRow as { bullmq_job_id: string | null }).bullmq_job_id;
      if (bullmqJobId) {
        try { await queue.remove(bullmqJobId); } catch {
          // Active jobs can't be removed; that's fine — Postgres status is canonical.
        }
      }
    }

    const refreshed = await repo.getByIdForScope({ id, projectId: current.projectId, teamId });
    if (!refreshed) {
      res.status(404).json({ error: 'NotFound', message: 'Generation job not found' });
      return null;
    }

    await this.auditWrite(req, 'generation_job.cancelled_by_operator', refreshed.id, refreshed.projectId, {
      previousStatus: current.status,
      currentStatus: refreshed.status,
      requestId: req.requestId ?? null,
    });

    return { job: refreshed, alreadyCancelled: false };
  }

  // Phase 12 — pick the right queue lane for a given source_type so retries
  // and cancels can publish to the same lane the original ingest used.
  private resolveEventQueueForRetry(row: { source_type: string }):
    { add: (jobId: string, payload: unknown, options?: unknown) => Promise<unknown>; remove: (jobId: string) => Promise<void> } | null {
    const lane = row.source_type === 'session_summary' ? 'summary' : 'event';
    const queue = this.resolveQueue(lane);
    if (!queue) return null;
    return queue as never;
  }

  private routeParam(value: string | string[] | undefined): string {
    if (Array.isArray(value)) {
      return value[0] ?? '';
    }
    return value ?? '';
  }

  private handleCreate<S extends ZodTypeAny, T = z.infer<S>>(
    schema: S,
    handler: (req: Request, res: Response, body: T) => Promise<void> | void,
  ) {
    return this.asyncHandler(async (req: Request, res: Response) => {
      const result = schema.safeParse(req.body);
      if (!result.success) {
        res.status(400).json({ error: 'ValidationError', issues: result.error.issues });
        return;
      }
      await handler(req, res, result.data as T);
    });
  }

  private asyncHandler(fn: (req: Request, res: Response) => Promise<void> | void) {
    return (req: Request, res: Response, next: (err?: unknown) => void): void => {
      Promise.resolve(fn(req, res)).catch(next);
    };
  }
}

interface ApiKeyListRow {
  id: string;
  project_id: string | null;
  actor_id: string;
  label: string | null;
  scopes: unknown;
  revoked_at: Date | null;
  expires_at: Date | null;
  last_used_at: Date | null;
  created_at: Date;
}

function serializeApiKeyListEntry(row: ApiKeyListRow, callerKeyId: string | null): Record<string, unknown> {
  const expired = row.expires_at !== null && new Date(row.expires_at).getTime() <= Date.now();
  return {
    id: row.id,
    projectId: row.project_id,
    actorId: row.actor_id,
    label: row.label,
    scopes: Array.isArray(row.scopes) ? row.scopes : [],
    status: row.revoked_at ? 'revoked' : expired ? 'expired' : 'active',
    createdAtEpoch: new Date(row.created_at).getTime(),
    expiresAtEpoch: row.expires_at ? new Date(row.expires_at).getTime() : null,
    revokedAtEpoch: row.revoked_at ? new Date(row.revoked_at).getTime() : null,
    lastUsedAtEpoch: row.last_used_at ? new Date(row.last_used_at).getTime() : null,
    current: row.id === callerKeyId,
  };
}

interface SessionListRow {
  id: string;
  content_session_id: string | null;
  platform_source: string | null;
  generation_status: string;
  folder: string | null;
  started_at: Date;
  ended_at: Date | null;
  observation_count: string;
  event_count: string;
}

function serializeSessionListEntry(row: SessionListRow): Record<string, unknown> {
  return {
    id: row.id,
    contentSessionId: row.content_session_id,
    platformSource: row.platform_source,
    generationStatus: row.generation_status,
    folder: row.folder,
    startedAtEpoch: new Date(row.started_at).getTime(),
    endedAtEpoch: row.ended_at ? new Date(row.ended_at).getTime() : null,
    observationCount: Number(row.observation_count),
    eventCount: Number(row.event_count),
  };
}

function clampListLimit(raw: unknown, fallback: number, max: number): number {
  const parsed = typeof raw === 'string' ? Number.parseInt(raw, 10) : Number.NaN;
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

function parseEpochQuery(raw: unknown): Date | null {
  const parsed = typeof raw === 'string' ? Number(raw) : Number.NaN;
  return Number.isFinite(parsed) && parsed > 0 ? new Date(parsed) : null;
}

function clampListOffset(raw: unknown): number {
  const parsed = typeof raw === 'string' ? Number.parseInt(raw, 10) : Number.NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

interface JobListRow {
  id: string;
  project_id: string;
  team_id: string;
  source_type: string;
  source_id: string;
  status: string;
  attempts: number;
  max_attempts: number;
  created_at: Date;
  completed_at: Date | null;
  failed_at: Date | null;
  last_error: unknown;
  // Phase 12 — payload is OPTIONAL because the SELECT may omit it, and
  // serializers strip it unless the caller explicitly opted in.
  payload?: unknown;
}

const SOURCE_TYPE_VALUES = new Set(['agent_event', 'session_summary', 'observation_reindex']);

function parseGenericJobListingQuery(req: Request): {
  status: string | null;
  sourceType: string | null;
  limit: number;
  offset: number;
  since: Date | null;
} {
  const statusRaw = typeof req.query.status === 'string' ? req.query.status.trim() : '';
  const status = statusRaw && JOB_LIST_STATUS_VALUES.has(statusRaw) ? statusRaw : null;
  const sourceTypeRaw = typeof req.query.source_type === 'string' ? req.query.source_type.trim() : '';
  const sourceType = sourceTypeRaw && SOURCE_TYPE_VALUES.has(sourceTypeRaw) ? sourceTypeRaw : null;
  const limit = clampInt(req.query.limit, JOB_LIST_DEFAULT_LIMIT, 1, JOB_LIST_MAX_LIMIT);
  const offset = clampInt(req.query.offset, 0, 0, Number.MAX_SAFE_INTEGER);
  const sinceRaw = typeof req.query.since === 'string' ? req.query.since.trim() : '';
  let since: Date | null = null;
  if (sinceRaw) {
    const parsed = new Date(sinceRaw);
    if (!Number.isNaN(parsed.getTime())) since = parsed;
  }
  return { status, sourceType, limit, offset, since };
}

function extractRetriedCount(payload: Record<string, unknown> | null | undefined): number {
  if (!payload || typeof payload !== 'object') return 0;
  const value = (payload as { retried_count?: unknown }).retried_count;
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    return Math.floor(value);
  }
  return 0;
}

const JOB_LIST_STATUS_VALUES = new Set(['queued', 'processing', 'completed', 'failed', 'cancelled']);
const JOB_LIST_DEFAULT_LIMIT = 50;
const JOB_LIST_MAX_LIMIT = 200;

function parseJobListingQuery(req: Request): {
  status: string | null;
  limit: number;
  offset: number;
} {
  const statusRaw = typeof req.query.status === 'string' ? req.query.status.trim() : '';
  const status = statusRaw && JOB_LIST_STATUS_VALUES.has(statusRaw) ? statusRaw : null;
  const limit = clampInt(req.query.limit, JOB_LIST_DEFAULT_LIMIT, 1, JOB_LIST_MAX_LIMIT);
  const offset = clampInt(req.query.offset, 0, 0, Number.MAX_SAFE_INTEGER);
  return { status, limit, offset };
}

function clampInt(value: unknown, fallback: number, min: number, max: number): number {
  if (typeof value !== 'string') return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function serializeJobListEntry(
  row: JobListRow,
  options: { includePayload?: boolean } = {},
): Record<string, unknown> {
  const base: Record<string, unknown> = {
    id: row.id,
    projectId: row.project_id,
    teamId: row.team_id,
    sourceType: row.source_type,
    sourceId: row.source_id,
    status: row.status,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    createdAtEpoch: new Date(row.created_at).getTime(),
    completedAtEpoch: row.completed_at ? new Date(row.completed_at).getTime() : null,
    failedAtEpoch: row.failed_at ? new Date(row.failed_at).getTime() : null,
    lastError: row.last_error && typeof row.last_error === 'object' ? row.last_error : null,
  };
  // Phase 12 — payload is sensitive (it may carry full event payloads
  // under `agent_events.payload`). Strip by default; only include when the
  // caller explicitly opted in via `?include=payload`. The route handler
  // gates that flag on admin scope BEFORE reaching here.
  if (options.includePayload && row.payload && typeof row.payload === 'object') {
    base.payload = row.payload;
  }
  return base;
}

// Phase 11 — every audit `action` carries a stable resource_type so dashboards
// can group/filter consistently. We map the dotted action name to a canonical
// resource_type keyword. Unknown actions fall back to the prefix (matches the
// previous behavior for backward compatibility).
function resolveAuditResourceType(action: string): string {
  const map: Record<string, string> = {
    'event.received': 'agent_event',
    'event.batch_received': 'agent_event',
    'event.write': 'agent_event',
    'event.batch_write': 'agent_event',
    'session.write': 'server_session',
    'session.end': 'server_session',
    'memory.write': 'observation',
    'observation.read': 'observation',
    'observation.updated': 'observation',
    'observation.deleted': 'observation',
    'observation.search': 'observation',
    'observation.context': 'observation',
    'observation.generated': 'observation',
    'session_summary.generated': 'observation',
    'generation_job.queued': 'observation_generation_job',
    'generation_job.enqueued': 'observation_generation_job',
    'generation_job.processing': 'observation_generation_job',
    'generation_job.completed': 'observation_generation_job',
    'generation_job.failed': 'observation_generation_job',
    'generation_job.scope_violation': 'observation_generation_job',
    'generation_job.revoked_key': 'observation_generation_job',
    'generation_job.retried_by_operator': 'observation_generation_job',
    'generation_job.cancelled_by_operator': 'observation_generation_job',
    'generation_job.stalled': 'observation_generation_job',
  };
  if (map[action]) return map[action]!;
  return action.split('.')[0] ?? 'unknown';
}

function preValidateBatch(
  req: Request,
  events: { projectId: string }[],
): BatchPreValidationFailure | null {
  const apiKeyProjectId = req.authContext?.projectId ?? null;
  const teamId = req.authContext?.teamId ?? null;
  if (!teamId) {
    return {
      status: 403,
      body: { error: 'Forbidden', message: 'API key is not bound to a team' },
    };
  }
  if (!apiKeyProjectId) {
    // No api-key project scope: every event must be in same team. Team
    // ownership is enforced by repos via `assertProjectOwnership`, but here
    // we only check the api-key cross-tenant bound.
    return null;
  }
  for (const event of events) {
    if (event.projectId !== apiKeyProjectId) {
      return {
        status: 403,
        body: {
          error: 'Forbidden',
          message: 'API key is scoped to a different project',
        },
      };
    }
  }
  return null;
}

function serializeSession(session: {
  id: string;
  projectId: string;
  teamId: string;
  externalSessionId: string | null;
  contentSessionId: string | null;
  agentId: string | null;
  agentType: string | null;
  platformSource: string | null;
  generationStatus: string;
  metadata: Record<string, unknown>;
  startedAtEpoch: number;
  endedAtEpoch: number | null;
  lastGeneratedAtEpoch: number | null;
  createdAtEpoch: number;
  updatedAtEpoch: number;
}): Record<string, unknown> {
  return {
    id: session.id,
    projectId: session.projectId,
    teamId: session.teamId,
    externalSessionId: session.externalSessionId,
    contentSessionId: session.contentSessionId,
    agentId: session.agentId,
    agentType: session.agentType,
    platformSource: session.platformSource,
    generationStatus: session.generationStatus,
    metadata: session.metadata,
    startedAtEpoch: session.startedAtEpoch,
    endedAtEpoch: session.endedAtEpoch,
    lastGeneratedAtEpoch: session.lastGeneratedAtEpoch,
    createdAtEpoch: session.createdAtEpoch,
    updatedAtEpoch: session.updatedAtEpoch,
  };
}

function serializeEvent(event: PostgresAgentEvent): Record<string, unknown> {
  return {
    id: event.id,
    projectId: event.projectId,
    teamId: event.teamId,
    serverSessionId: event.serverSessionId,
    sourceAdapter: event.sourceAdapter,
    sourceEventId: event.sourceEventId,
    eventType: event.eventType,
    platformSource: event.platformSource,
    payload: event.payload,
    metadata: event.metadata,
    occurredAtEpoch: event.occurredAtEpoch,
    receivedAtEpoch: event.receivedAtEpoch,
    createdAtEpoch: event.createdAtEpoch,
  };
}

function serializeObservation(observation: {
  id: string;
  projectId: string;
  teamId: string;
  serverSessionId: string | null;
  contentSessionId?: string | null;
  kind: string;
  content: string;
  metadata: Record<string, unknown>;
  createdAtEpoch: number;
  updatedAtEpoch: number;
}): Record<string, unknown> {
  return {
    id: observation.id,
    projectId: observation.projectId,
    teamId: observation.teamId,
    serverSessionId: observation.serverSessionId,
    ...(observation.contentSessionId !== undefined ? { contentSessionId: observation.contentSessionId } : {}),
    kind: observation.kind,
    content: observation.content,
    metadata: observation.metadata,
    createdAtEpoch: observation.createdAtEpoch,
    updatedAtEpoch: observation.updatedAtEpoch,
  };
}

interface ObservationWithSourceRow {
  id: string;
  project_id: string;
  team_id: string;
  server_session_id: string | null;
  kind: string;
  content: string;
  metadata: unknown;
  generation_key: string | null;
  created_by_job_id: string | null;
  created_at: Date;
  updated_at: Date;
  source_id_pk: string;
  source_type: string;
  source_id: string;
  generation_job_id: string | null;
  source_created_at: Date;
}

function serializeObservationWithSource(row: ObservationWithSourceRow): Record<string, unknown> {
  return {
    id: row.id,
    projectId: row.project_id,
    teamId: row.team_id,
    serverSessionId: row.server_session_id,
    kind: row.kind,
    content: row.content,
    metadata: row.metadata && typeof row.metadata === 'object' ? row.metadata : {},
    generationKey: row.generation_key,
    createdByJobId: row.created_by_job_id,
    createdAtEpoch: new Date(row.created_at).getTime(),
    updatedAtEpoch: new Date(row.updated_at).getTime(),
    source: {
      id: row.source_id_pk,
      sourceType: row.source_type,
      sourceId: row.source_id,
      generationJobId: row.generation_job_id,
      createdAtEpoch: new Date(row.source_created_at).getTime(),
    },
  };
}

function serializeGenerationJob(
  job: PostgresObservationGenerationJob,
  enqueueState: 'enqueued' | 'queued_only' | 'skipped',
): Record<string, unknown> {
  return {
    id: job.id,
    status: job.status,
    bullmqJobId: job.bullmqJobId,
    sourceType: job.sourceType,
    sourceId: job.sourceId,
    transport: enqueueState,
    createdAtEpoch: job.createdAtEpoch,
    updatedAtEpoch: job.updatedAtEpoch,
  };
}

// `?wait=true` polls the outbox row until it reaches a terminal status
// (or hits WAIT_TIMEOUT_MS). The serialized payload reports `status`,
// `attempts`, and `lastError`-equivalents on the outbox row itself; the
// caller queries the observations endpoints to fetch the actual content.
function serializeJobStatusResponse(
  job: PostgresObservationGenerationJob,
  enqueueState: 'enqueued' | 'queued_only' | 'skipped',
): Record<string, unknown> {
  return {
    id: job.id,
    status: job.status,
    transport: enqueueState,
    bullmqJobId: job.bullmqJobId,
    sourceType: job.sourceType,
    sourceId: job.sourceId,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    createdAtEpoch: job.createdAtEpoch,
    updatedAtEpoch: job.updatedAtEpoch,
  };
}

function serializeGenerationJobStatus(
  job: PostgresObservationGenerationJob,
): Record<string, unknown> {
  return {
    id: job.id,
    projectId: job.projectId,
    teamId: job.teamId,
    sourceType: job.sourceType,
    sourceId: job.sourceId,
    agentEventId: job.agentEventId,
    serverSessionId: job.serverSessionId,
    jobType: job.jobType,
    status: job.status,
    bullmqJobId: job.bullmqJobId,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    nextAttemptAtEpoch: job.nextAttemptAtEpoch,
    completedAtEpoch: job.completedAtEpoch,
    failedAtEpoch: job.failedAtEpoch,
    cancelledAtEpoch: job.cancelledAtEpoch,
    lastError: job.lastError,
    createdAtEpoch: job.createdAtEpoch,
    updatedAtEpoch: job.updatedAtEpoch,
  };
}
