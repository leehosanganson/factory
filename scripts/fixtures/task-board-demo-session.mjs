import { readFileSync, rmSync } from 'node:fs';
import { createServer, request } from 'node:http';

const host = 'localhost:19443';
const origin = `https://${host}`;
const cookieName = 'factory_operator_session';

function eligible(req) {
  const h = req.headersDistinct;
  const exact = (name, value) => h[name]?.length === 1 && h[name][0] === value;
  return req.method === 'GET' && req.url === '/ui/session' && exact('host', host) &&
    exact('sec-fetch-site', 'same-origin') && exact('sec-fetch-mode', 'cors') &&
    exact('sec-fetch-dest', 'empty') && (!h.origin || exact('origin', origin)) &&
    (!h['content-length'] || exact('content-length', '0')) && !h['transfer-encoding'] &&
    !h['sec-purpose'] && !h.purpose && !h.authorization &&
    !h.cookie?.some((line) => line.split(';').some((pair) => pair.split('=', 1)[0].trim() === cookieName));
}

async function login(token, upstream) {
  const body = new URLSearchParams({ token }).toString();
  const response = await new Promise((resolve, reject) => {
    request(`${upstream}/ui/login`, {
      method: 'POST', agent: false, signal: AbortSignal.timeout(3000),
      headers: { Host: host, Origin: origin, 'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body) },
    }, resolve).on('error', reject).end(body);
  });
  const cookies = response.headers['set-cookie'] ?? [];
  const [cookie] = cookies;
  const [nameValue, ...attributes] = cookie?.split(/;\s*/) ?? [];
  const flags = attributes.map((attr) => attr.toLowerCase());
  if (response.statusCode !== 200 || cookies.length !== 1 ||
      !nameValue.startsWith(`${cookieName}=`) || nameValue === `${cookieName}=` ||
      !['path=/', 'secure', 'httponly', 'samesite=strict'].every((flag) => flags.includes(flag)) ||
      flags.some((flag) => flag.startsWith('domain='))) {
    response.destroy();
    return;
  }
  const chunks = [];
  let bytes = 0;
  for await (const chunk of response) {
    bytes += chunk.length;
    if (bytes > 4096) return;
    chunks.push(chunk);
  }
  const { authenticated, role, csrf } = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if (authenticated === true && role === 'submitter' && typeof csrf === 'string' && csrf) {
    return { cookie, session: { authenticated, role, csrf } };
  }
}

export function demoSession(token, upstream) {
  return async (req, res) => {
    if (token && eligible(req)) {
      const result = await login(token, upstream).catch(() => undefined);
      if (result) {
        res.writeHead(200, {
          'Content-Type': 'application/json', 'Cache-Control': 'no-store',
          'X-Content-Type-Options': 'nosniff', 'Set-Cookie': result.cookie,
        }).end(JSON.stringify(result.session));
        return;
      }
    }
    // The demo proxy handles this as a signal to use the unchanged session route.
    res.writeHead(204).end();
  };
}

if (import.meta.main) {
  const socket = '/run/demo-session/session.sock';
  const token = readFileSync('/run/demo-owner-token', 'utf8').trim();
  if (!/^[a-f0-9]{64}$/.test(token)) throw new Error('Invalid demo submitter credential');
  process.umask(0o077);
  rmSync(socket, { force: true });
  createServer({ requestTimeout: 5000, headersTimeout: 5000 }, demoSession(token, 'http://api:8080')).listen(socket);
}
