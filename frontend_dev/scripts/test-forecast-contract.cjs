const assert = require("node:assert/strict")
const fs = require("node:fs")
const path = require("node:path")
const Module = require("node:module")
const ts = require("typescript")
const root = path.resolve(__dirname, "..")
const filename = path.join(root, "lib/forecast-record.ts")
const compiled = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS }, reportDiagnostics: true,
})
assert.equal(compiled.diagnostics.length, 0)
const contract = new Module(filename, module)
contract.filename = filename
contract.paths = Module._nodeModulePaths(path.dirname(filename))
contract._compile(compiled.outputText, filename)
const { forecastSchema, recordSchema, historySchema, score, reasonLabel } = contract.exports
const forecast = {
  entry_id: 1, event_key: "match:over:1.5", event_label: "Over 1.5 goals", probability: .7,
  captured_at: "2026-10-01T10:00:00+00:00", captured_kickoff: "2026-10-02T18:00:00+00:00",
  status: "pending", reason: "not_yet_checked", outcome_observed_at: null, evidence: null,
  result_observation_state: "unobserved",
}
assert.equal(forecastSchema.safeParse(forecast).success,true)
for (const changes of [{probability:1.1},{probability:null},{status:"winner"},{captured_at:"yesterday"}]) {
  assert.equal(forecastSchema.safeParse({...forecast,...changes}).success,false)
}
assert.equal(recordSchema.safeParse({fixtures:[]}).success,false)
assert.equal(historySchema.safeParse({observations:[]}).success,false)
assert.equal(score(null),null)
assert.equal(score({fixture:{home_team_fulltime_goal:0,away_team_fulltime_goal:0}}),"0–0")
assert.equal(score({fixture:{home_team_fulltime_goal:0,away_team_fulltime_goal:null}}),null)
assert.match(reasonLabel("corrected_kickoff_precedes_capture"),/excluded/)
console.log("PASS: forecast response validation, missing data, score zeroes and exclusion copy")
