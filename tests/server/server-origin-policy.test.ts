// SPDX-License-Identifier: Apache-2.0
//
// The hosted server's dashboard is a browser page on the server's public name.
// Its writes carry an Origin header, and the CORS middleware refuses any Origin
// it was not told about, so the server must derive a policy from
// CLAUDE_MEM_PUBLIC_URL / CLAUDE_MEM_ALLOWED_ORIGINS.

import { describe, expect, it } from 'bun:test';
import type { Request, Response } from 'express';
import { serverOriginPolicyFromEnv } from '../../src/server/runtime/ServerService.js';
import { createCorsMiddleware, createWorkerHostGuard } from '../../src/services/worker/http/middleware.js';

function run(handler: ReturnType<typeof createCorsMiddleware>, headers: Record<string, string>, method = 'POST') {
  let status = 200;
  let passed = false;
  const res = {
    status(code: number) { status = code; return this; },
    json() { return this; },
    end() { return this; },
    setHeader() {},
  } as unknown as Response;
  handler({ headers, method, path: '/v1/search' } as unknown as Request, res, () => { passed = true; });
  return { status, passed };
}

describe('serverOriginPolicyFromEnv', () => {
  it('builds no policy when the operator has not named the server', () => {
    expect(serverOriginPolicyFromEnv({})).toBeUndefined();
    expect(serverOriginPolicyFromEnv({ CLAUDE_MEM_PUBLIC_URL: 'not a url' })).toBeUndefined();
  });

  it('admits the dashboard on the public name over https and http, and nothing else', () => {
    const policy = serverOriginPolicyFromEnv({ CLAUDE_MEM_PUBLIC_URL: 'https://mem.example.test/' });
    expect(policy?.allowedOrigins).toEqual(['https://mem.example.test']);
    const cors = createCorsMiddleware(policy);
    const host = createWorkerHostGuard(policy!);

    // The published origin, and the same host reached over plain http.
    expect(run(cors, { origin: 'https://mem.example.test', host: 'mem.example.test' }).passed).toBe(true);
    expect(run(cors, { origin: 'http://mem.example.test', host: 'mem.example.test' }).passed).toBe(true);
    // Another site's page is still refused.
    expect(run(cors, { origin: 'https://evil.example', host: 'mem.example.test' })).toEqual({ status: 403, passed: false });

    // The Host check trusts the public name and IPs (probes), not other names.
    expect(run(host, { host: 'mem.example.test' }).passed).toBe(true);
    expect(run(host, { host: '10.42.0.7:37877' }).passed).toBe(true);
    expect(run(host, { host: 'evil.example' })).toEqual({ status: 403, passed: false });
  });

  it('without a policy only localhost pages pass, which is the bug this guards', () => {
    const cors = createCorsMiddleware(undefined);
    expect(run(cors, { origin: 'http://mem.example.test', host: 'mem.example.test' })).toEqual({ status: 403, passed: false });
    expect(run(cors, { origin: 'http://127.0.0.1:37877', host: '127.0.0.1:37877' }).passed).toBe(true);
  });

  it('honors CLAUDE_MEM_ALLOWED_ORIGINS on its own', () => {
    const policy = serverOriginPolicyFromEnv({ CLAUDE_MEM_ALLOWED_ORIGINS: 'https://a.example.test, https://b.example.test' });
    expect(policy?.allowedOrigins).toEqual(['https://a.example.test', 'https://b.example.test']);
  });
});
