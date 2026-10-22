const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),Module=require('node:module'),ts=require('typescript')
const filename=path.resolve(__dirname,'../lib/research.ts'),unit=new Module(filename,module);unit.filename=filename;unit.paths=Module._nodeModulePaths(path.dirname(filename));unit._compile(ts.transpileModule(fs.readFileSync(filename,'utf8'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText,filename)
const {cards,match,discover}=require('./research-tests/fixtures.cjs'),c=unit.exports
c.discoverSchema.parse(discover);c.matchSchema.parse(match)
let saved=c.toggleSaved([],cards[0],'2026-10-13');const original=c.headline(saved[0].card);const copy=structuredClone(cards[0]);cards[0].pick.favourite='Mutated after save';copy.pick.favourite='Changed team';assert.equal(c.headline(saved[0].card),original);assert.equal(c.toggleSaved(saved,copy,'new-run').length,0)
assert.equal(c.matchHref(cards[0],false),'/match/1');assert.match(c.matchHref(cards[0]),/section=strength/)
assert.equal(c.matchSchema.safeParse({...match,goals:{rows:[{...match.goals.rows[0],probability:2}],nothing_stands_out:false}}).success,false)
assert.equal(c.discoverSchema.safeParse({...discover,lists:{...discover.lists,standouts:[{...cards[0],fixture_id:-1}]}}).success,false)
console.log('PASS: payload schemas, fixture identity, frozen saves, unsave and entry URLs')

// PostgreSQL/FastAPI returns timezone offsets; browser legacy saves use Z.
for (const saved_at of ['2026-10-19T12:34:56.123456+00:00', '2026-10-19T05:34:56-07:00', '2026-10-19T12:34:56.123Z']) {
  assert.equal(c.savedSchema.safeParse([{...saved[0], saved_at}]).success, true);
}
for (const saved_at of ['2026-10-19T12:34:56', 'invalid']) {
  assert.equal(c.savedSchema.safeParse([{...saved[0], saved_at}]).success, false);
}
