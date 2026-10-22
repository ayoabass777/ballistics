const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), Module = require('node:module'), ts = require('typescript');
function load(file, mocks = {}) {
  const filename = path.resolve(__dirname, '..', file), unit = new Module(filename, module);
  unit.filename = filename; unit.paths = Module._nodeModulePaths(path.dirname(filename));
  const actual = unit.require.bind(unit); unit.require = id => mocks[id] || actual(id);
  unit._compile(ts.transpileModule(fs.readFileSync(filename, 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText, filename);
  return unit.exports;
}
let userId = null, token = 'test-token', calls = [], upstream = { status: 200, body: { data: [] } };
const research = load('lib/research.ts');
const schemas = load('lib/account.ts', { './research': research });
const { accountProxy } = load('lib/server/account-proxy.ts', {
  '@clerk/nextjs/server': { auth: async () => ({ userId, getToken: async () => token }) },
  'next/server': { NextResponse: { json: (body, options) => Response.json(body, options) } },
});
const originalFetch = global.fetch, previous = process.env.ACCOUNT_API_BASE_URL;
global.fetch = async (url, options) => { calls.push({ url: String(url), options }); return Response.json(upstream.body, { status: upstream.status }); };
const request = (method = 'GET', body, origin = 'https://ballistics.test') => ({ method, nextUrl: new URL('https://ballistics.test/api/account/saved'), headers: new Headers({ origin }), json: async () => body });
(async () => { try {
  process.env.ACCOUNT_API_BASE_URL = 'http://localhost:8000';
  assert.equal((await accountProxy(request(), 'account/saved', schemas.savedResponseSchema)).status, 401);
  assert.equal(calls.length, 0);
  userId = 'alice';
  const paged = request(); paged.nextUrl = new URL('https://ballistics.test/api/account/saved/page?scope=finished&limit=12&cursor=test&ignored=1');
  upstream.body = {data:{items:[],next_cursor:null}};
  assert.equal((await accountProxy(paged, 'account/saved/page', schemas.savedPageSchema)).status, 200);
  assert.equal(calls.at(-1).url, 'http://localhost:8000/account/saved/page?scope=finished&cursor=test&limit=12');
  calls = []; upstream.body = {data:[]};
  assert.equal((await accountProxy(request('POST', {}, 'https://other.test'), 'account/saved', schemas.savedResponseSchema, schemas.saveCardSchema)).status, 403);
  assert.equal((await accountProxy(request('POST', { card: { fixture_id: -1 }, run_ds: null }), 'account/saved', schemas.savedResponseSchema, schemas.saveCardSchema)).status, 422);
  assert.equal(calls.length, 0);
  let result = await accountProxy(request(), 'account/saved', schemas.savedResponseSchema);
  assert.equal(result.status, 200); assert.equal(result.headers.get('cache-control'), 'no-store');
  assert.equal(calls.at(-1).url, 'http://localhost:8000/account/saved');
  assert.equal(calls.at(-1).options.headers.Authorization, 'Bearer test-token');
  assert.equal(calls.at(-1).options.redirect, 'error');
  const card = { fixture_id: 1, home: 'Home', away: 'Away', league_id: 1, league: 'League', country: null, kickoff: '2026-10-20T12:00:00Z', pick: null };
  upstream = { status: 200, body: { data: [{ card, run_ds: null, saved_at: '2026-10-19T12:34:56.123456+00:00' }] } };
  result = await accountProxy(request('POST', { card, run_ds: null }), 'account/saved', schemas.savedResponseSchema, schemas.saveCardSchema);
  assert.equal(result.status, 200);
  assert.equal((await result.json()).data[0].card.fixture_id, 1);
  assert.equal(schemas.saveCardSchema.parse({ card: { ...card, preview: { list: 'strength', favourite: 'Home' } }, run_ds: null }).card.preview, undefined);
  upstream = { status: 500, body: { detail: 'private database detail' } };
  result = await accountProxy(request(), 'account/saved', schemas.savedResponseSchema);
  assert.equal(result.status, 503); assert.ok(!(await result.text()).includes('private'));
  upstream = { status: 200, body: { unexpected: true } };
  assert.equal((await accountProxy(request(), 'account/saved', schemas.savedResponseSchema)).status, 502);
  token = null;
  assert.equal((await accountProxy(request(), 'account/saved', schemas.savedResponseSchema)).status, 401);
  console.log('PASS: account proxy auth, origin, token forwarding, response validation, no-store and sanitized errors');
} finally { global.fetch = originalFetch; if (previous === undefined) delete process.env.ACCOUNT_API_BASE_URL; else process.env.ACCOUNT_API_BASE_URL = previous; } })().catch(error => { console.error(error); process.exitCode = 1; });
