import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { test } from 'node:test';
import { demoSession } from './task-board-demo-session.mjs';

const host = 'localhost:19443';
const token = 'fixture-credential';
const cookie = 'factory_operator_session=session-id; Path=/; HttpOnly; Secure; SameSite=Strict';
const headers = {
  host,
  'sec-fetch-site': 'same-origin',
  'sec-fetch-mode': 'cors',
  'sec-fetch-dest': 'empty',
};

async function serve(t, handler) {
  const server = createServer(handler);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  return `http://127.0.0.1:${server.address().port}`;
}

const submitter = { authenticated: true, role: 'submitter', csrf: 'fixture-csrf' };

async function setup(t, reply, credential = token) {
  const calls = [];
  const upstream = await serve(t, async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    calls.push({ method: req.method, path: req.url, host: req.headers.host,
      origin: req.headers.origin, type: req.headers['content-type'], body });
    reply(res);
  });
  const demo = await serve(t, demoSession(credential, upstream));
  return { calls, request: (path = '/ui/session', options = {}) => new Promise((resolve, reject) => {
    request(demo, { path, headers, ...options }, async (response) => {
      const chunks = [];
      for await (const chunk of response) chunks.push(chunk);
      resolve(new Response(response.statusCode === 204 ? null : Buffer.concat(chunks), {
        status: response.statusCode, headers: response.headers,
      }));
    }).on('error', reject).end();
  }) };
}

test('same-origin session fetch uses normal login and returns only the verified session', async (t) => {
  const { calls, request } = await setup(t, (res) => res.writeHead(200, {
    'Set-Cookie': cookie, 'X-Private': token,
  }).end(JSON.stringify({ ...submitter, private: token })));
  const response = await request('/ui/session', { headers: { ...headers, origin: `https://${host}` } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('set-cookie'), cookie);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal(response.headers.get('x-private'), null);
  assert.deepEqual(await response.json(), submitter);
  assert.deepEqual(calls, [{ method: 'POST', path: '/ui/login', host,
    origin: `https://${host}`, type: 'application/x-www-form-urlencoded', body: `token=${token}` }]);
});

test('ineligible requests fall through without contacting login', async (t) => {
  const { calls, request } = await setup(t, (res) => res.writeHead(500).end());
  const controls = [
    ['wrong method', '/ui/session', { method: 'POST' }],
    ['wrong route', '/ui/login', {}],
    ['query', '/ui/session?x=1', {}],
    ['empty query', '/ui/session?', {}],
    ...Object.entries({
      'wrong host': { host: 'localhost:19444' },
      'cross-site': { 'sec-fetch-site': 'cross-site' },
      'navigation': { 'sec-fetch-mode': 'navigate', 'sec-fetch-dest': 'document' },
      'wrong destination': { 'sec-fetch-dest': 'iframe' },
      'duplicate metadata': { 'sec-fetch-site': 'same-origin, same-origin' },
      'wrong origin': { origin: 'https://other.invalid' },
      'prefetch': { 'sec-purpose': 'prefetch' },
      'legacy prefetch': { purpose: 'prefetch' },
      'existing session cookie': { cookie: 'other=1; factory_operator_session=stale' },
      'explicit authorization': { authorization: 'Bearer other' },
    }).map(([name, overrides]) => [name, '/ui/session', { headers: { ...headers, ...overrides } }]),
    ['missing metadata', '/ui/session', { headers: { host } }],
  ];
  for (const [name, path, options] of controls) {
    const response = await request(path, options);
    assert.equal(response.status, 204, name);
    assert.equal(response.headers.get('set-cookie'), null, name);
    assert.deepEqual(calls, [], name);
  }
});

test('disabled helper never calls login', async (t) => {
  const { calls, request } = await setup(t, (res) => res.writeHead(500).end(), '');
  assert.equal((await request()).status, 204);
  assert.deepEqual(calls, []);
});

test('login failures and unexpected sessions fall through without leaking upstream data', async (t) => {
  for (const [name, status, cookies, body] of [
    ['operator', 200, cookie, { ...submitter, role: 'operator' }],
    ['anonymous', 200, cookie, { ...submitter, authenticated: false }],
    ['missing CSRF', 200, cookie, { ...submitter, csrf: '' }],
    ['insecure cookie', 200, cookie.replace('; Secure', ''), submitter],
    ['domain cookie', 200, `${cookie}; Domain=localhost`, submitter],
    ['duplicate cookies', 200, [cookie, 'extra=cookie'], submitter],
    ['redirect', 302, cookie, submitter],
    ['upstream error', 503, cookie, { error: token }],
    ['oversized data', 200, cookie, { ...submitter, extra: token.repeat(4096) }],
  ]) {
    await t.test(name, async (t) => {
      const { calls, request } = await setup(t, (res) => res.writeHead(status, {
        'Set-Cookie': cookies, Location: '/must-not-follow',
      }).end(JSON.stringify(body)));
      const response = await request();
      assert.equal(response.status, 204);
      assert.equal(response.headers.get('set-cookie'), null);
      assert.equal(response.headers.get('location'), null);
      assert.equal(await response.text(), '');
      assert.equal(calls.length, 1);
    });
  }
});
