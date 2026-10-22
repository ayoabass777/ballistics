// Unit-only dependency injection; production auth and routing are unchanged.
const assert = require("node:assert/strict")
const fs = require("node:fs")
const path = require("node:path")
const Module = require("node:module")
const ts = require("typescript")
const { z } = require("zod")
const filename = path.resolve(__dirname, "../lib/server/forecast-proxy.ts")
const compiled = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS }, reportDiagnostics: true,
})
assert.equal(compiled.diagnostics.length,0)
let userId = null, calls = [], response = { status:200, body:{ count:1 } }
const unit = new Module(filename,module)
unit.filename = filename
unit.paths = Module._nodeModulePaths(path.dirname(filename))
const actualRequire = unit.require.bind(unit)
unit.require = id => id === "@clerk/nextjs/server" ? { auth:async()=>({userId}) }
  : id === "next/server" ? { NextResponse:{ json:(body,options)=>Response.json(body,options) } }
  : actualRequire(id)
unit._compile(compiled.outputText,filename)
const originalFetch = global.fetch, originalOrigin = process.env.FORECAST_API_BASE_URL
global.fetch = async (url,options) => {
  calls.push({url:new URL(url),options})
  return Response.json(response.body,{status:response.status})
}
const request = { nextUrl:new URL("http://localhost/api/forecast-record?team=Brooklyn&unsafe=ignored") }
const read = () => unit.exports.forecastProxy(request,"",z.object({count:z.number()}),["team"])
;(async()=>{
  try {
    process.env.FORECAST_API_BASE_URL = "http://127.0.0.1:8001"
    assert.equal((await read()).status,401)
    assert.equal(calls.length,0)
    userId = "test-user"
    delete process.env.FORECAST_API_BASE_URL
    assert.equal((await read()).status,503)
    assert.equal(calls.length,0)
    process.env.FORECAST_API_BASE_URL = "http://127.0.0.1:8001"
    let result = await read()
    assert.equal(result.status,200)
    assert.equal(result.headers.get("cache-control"),"no-store")
    assert.deepEqual(await result.json(),{count:1})
    assert.equal(calls[0].url.pathname,"/api/forecast-record")
    assert.equal(calls[0].url.searchParams.get("team"),"Brooklyn")
    assert.equal(calls[0].url.searchParams.has("unsafe"),false)
    assert.equal(calls[0].options.cache,"no-store")
    response = {status:200,body:{count:"bad"}}
    assert.equal((await read()).status,502)
    response = {status:500,body:{error:"private connection details"}}
    result = await read()
    assert.equal(result.status,503)
    assert.ok(!(await result.text()).includes("private"))
    response = {status:422,body:{}}
    assert.equal((await read()).status,422)
    response = {status:404,body:{}}
    assert.equal((await read()).status,404)
    const before = calls.length
    process.env.FORECAST_API_BASE_URL = "file:///private"
    assert.equal((await read()).status,503)
    assert.equal(calls.length,before)
    console.log("PASS: proxy auth, configuration, allowlisted query, no-store, validation and sanitized errors")
  } finally {
    global.fetch = originalFetch
    if (originalOrigin === undefined) delete process.env.FORECAST_API_BASE_URL
    else process.env.FORECAST_API_BASE_URL = originalOrigin
  }
})().catch(error=>{console.error(error);process.exitCode=1})
