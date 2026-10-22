const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), Module = require('node:module'), ts = require('typescript');
function load(file, mocks = {}) {
 const filename = path.resolve(__dirname, '..', file), unit = new Module(filename, module);
 unit.filename = filename; unit.paths = Module._nodeModulePaths(path.dirname(filename));
 const actual = unit.require.bind(unit); unit.require = id => mocks[id] || actual(id);
 unit._compile(ts.transpileModule(fs.readFileSync(filename, 'utf8'), { compilerOptions:{ target:ts.ScriptTarget.ES2022, module:ts.ModuleKind.CommonJS }}).outputText,filename);
 return unit.exports;
}
const lib = load('lib/preferences.ts');
assert.equal(lib.dayInZone('America/Vancouver',0,new Date('2026-10-19T01:00:00Z')), '2026-10-18');
assert.equal(lib.dayInZone('Asia/Tokyo',0,new Date('2026-10-19T01:00:00Z')), '2026-10-19');
assert.equal(lib.dayInZone('America/Vancouver',1,new Date('2026-11-01T07:30:00Z')), '2026-11-02');
let userId = null, token = 'test-session-token', calls=[], response={status:200, body:{data:{timezone_override:null,created_at:null,updated_at:null}}};
const route = load('app/api/preferences/route.ts', {
 '@clerk/nextjs/server': {auth:async()=>({userId,getToken:async()=>token})},
 'next/server':{NextResponse:{json:(body,opts)=>Response.json(body,opts)}}, '@/lib/preferences':lib,
});
const originalFetch = global.fetch, previous = process.env.ACCOUNT_API_BASE_URL;
global.fetch = async (url,options) => {calls.push({url:String(url),options}); return Response.json(response.body,{status:response.status});};
function request(method='GET',body,origin='https://ballistics.test') { return {method,nextUrl:new URL('https://ballistics.test/api/preferences'),headers:new Headers({origin}),json:async()=>body}; }
(async()=>{try {
 process.env.ACCOUNT_API_BASE_URL='http://localhost:8000';
 assert.equal((await route.GET(request())).status,401); assert.equal(calls.length,0);
 userId='alice';
 assert.equal((await route.POST(request('POST',{timezone_override:null},'https://other.test'))).status,403);
 assert.equal((await route.POST(request('POST',{timezone_override:null,user_id:'bob'}))).status,422);
 let result=await route.GET(request()); assert.equal(result.status,200); assert.equal(result.headers.get('cache-control'),'no-store');
 assert.equal(calls.at(-1).options.headers.Authorization,'Bearer test-session-token');
 assert.equal(calls.at(-1).options.redirect,'error');
 result=await route.POST(request('POST',{timezone_override:'Europe/London'})); assert.equal(result.status,200);
 assert.deepEqual(JSON.parse(calls.at(-1).options.body),{timezone_override:'Europe/London'});
 response={status:500,body:{detail:'private credentials'}}; result=await route.GET(request()); assert.equal(result.status,503);assert.ok(!(await result.text()).includes('private'));
 response={status:200,body:{data:{timezone_override:'Mars/Olympus',created_at:null,updated_at:null}}};assert.equal((await route.GET(request())).status,502);
 token=null;assert.equal((await route.GET(request())).status,401);
 console.log('PASS: preferences auth, origin, token forwarding, validation, sanitized errors, no-store and timezone date boundaries');
}finally{global.fetch=originalFetch;if(previous===undefined)delete process.env.ACCOUNT_API_BASE_URL;else process.env.ACCOUNT_API_BASE_URL=previous;}})().catch(e=>{console.error(e);process.exitCode=1;});
