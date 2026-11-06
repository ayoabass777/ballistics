// Live scores forecast tracker (lib/live.ts): node scripts/test-live.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs'), ts = require('typescript');
const original = require.extensions['.ts'];
require.extensions['.ts'] = (module, filename) => module._compile(ts.transpileModule(fs.readFileSync(filename, 'utf8'), {compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText, filename);
const l = require('../lib/live.ts');
const sc = (home, away, final = false) => ({ home, away, final });
const st = (status, home, away, extra = {}) => ({ fixture_id: 1, status, minute: 60, extra: null, home_goals: home, away_goals: away, home_red_cards: 0, away_red_cards: 0, finished: ['FT','AET','PEN'].includes(status), changed_at: null, polled_at: new Date().toISOString(), ...extra });

// Goals outlook markets
assert.deepEqual(l.trackMarket('Match Over 1.5', 'Brann', 'Molde', sc(1, 0)), ['off_track', '1 goal so far · needs 1 more']);
assert.equal(l.trackMarket('Match Over 1.5', 'Brann', 'Molde', sc(1, 1))[0], 'landed');
assert.equal(l.trackMarket('Match Over 1.5', 'Brann', 'Molde', sc(1, 0, true))[0], 'missed');
assert.equal(l.trackMarket('Molde Over 0.5', 'Brann', 'Molde', sc(3, 0))[0], 'off_track');
assert.equal(l.trackMarket('Molde Over 0.5', 'Brann', 'Molde', sc(0, 1))[0], 'landed');
assert.deepEqual(l.trackMarket('Match Under 3.5', 'Brann', 'Molde', sc(2, 1)), ['on_track', '3 goals · no more allowed']);
assert.equal(l.trackMarket('Match Under 3.5', 'Brann', 'Molde', sc(2, 2))[0], 'missed');
assert.equal(l.trackMarket('Match Under 3.5', 'Brann', 'Molde', sc(1, 0, true))[0], 'landed');
assert.equal(l.trackMarket('Match Goals 1-4', 'Brann', 'Molde', sc(0, 0))[0], 'off_track');
assert.equal(l.trackMarket('Match Goals 1-4', 'Brann', 'Molde', sc(2, 0))[0], 'on_track');
assert.equal(l.trackMarket('Match Goals 1-4', 'Brann', 'Molde', sc(3, 2))[0], 'missed');
assert.equal(l.trackMarket('Match Goals 1-4', 'Brann', 'Molde', sc(0, 0, true))[0], 'missed');
assert.equal(l.trackMarket('Somebody Else Over 0.5', 'Brann', 'Molde', sc(1, 0)), null);

// Team patterns, from the team's side
assert.deepEqual(l.trackPattern('win', 1, 0, false), ['on_track', 'Leading']);
assert.deepEqual(l.trackPattern('win', 1, 1, false), ['off_track', 'Level']);
assert.deepEqual(l.trackPattern('win', 1, 1, true), ['missed', 'Did not happen']);
assert.equal(l.trackPattern('unbeaten', 1, 1, true)[0], 'landed');
assert.equal(l.trackPattern('clean_sheet', 2, 1, false)[0], 'missed');
assert.equal(l.trackPattern('clean_sheet', 2, 0, false)[0], 'on_track');
assert.equal(l.trackPattern('goalless', 0, 0, true)[0], 'landed');
assert.equal(l.trackPattern('score_2goals', 2, 0, false)[0], 'landed');
assert.deepEqual(l.trackPattern('concede_1', 0, 0, false), ['off_track', 'Needs 1 more']);
assert.deepEqual(l.trackPattern('BTTS', 1, 0, false), ['off_track', 'Needs 1 more']);
assert.equal(l.trackPattern('BTTS', 1, 1, false)[0], 'landed');
assert.equal(l.trackPattern('Under_4_5', 3, 2, false)[0], 'missed');
assert.equal(l.trackPattern('Over_1_5', 1, 0, false)[0], 'off_track');
assert.equal(l.trackPattern('halftime_win', 1, 0, false), null);

// Whole match: strength, goals and patterns; away patterns use the away side
const match = { fixture: { home: 'Brann', away: 'Molde' },
  strength: { state: 'big_dog', favourite: 'Molde' },
  goals: { rows: [{ market: 'Match Over 1.5', probability: 0.84 }] },
  patterns: { default_view: 'all', venue: null, all: { top_results: [{ team: 'Molde', side: 'away', event: 'clean_sheet', label: 'Clean sheet', k: 9, n: 12, context: 'all' }], top_attack_defence: [] } } };
const rows = l.trackForecast(match, st('2H', 0, 1));
assert.deepEqual(rows.map(r => [r.kind, r.status, r.detail]), [['strength', 'on_track', 'Leading'], ['goals', 'off_track', '1 goal so far · needs 1 more'], ['patterns', 'on_track', 'No goals conceded']]);
assert.equal(rows[0].title, 'Molde · Big dog');
assert.deepEqual(l.trackForecast(match, st('FT', 0, 1)).map(r => r.status), ['landed', 'missed', 'landed']);
assert.equal(l.trackForecast(match, st('FT', 0, 1))[0].detail, 'Won');
// Extra time: 90-minute markets are not graded; not started or postponed: nothing
assert.deepEqual(l.trackForecast(match, st('AET', 2, 1)).map(r => r.kind), ['strength']);
assert.deepEqual(l.trackForecast(match, st('PST', 0, 0)), []);
assert.deepEqual(l.trackForecast(match, st('NS', null, null)), []);

// List chip from a Discover pick
const card = { ...st('2H', 2, 0), pick: { list: 'goals', market: 'Brann Over 1.5', probability: 0.71 }, home: 'Brann', away: 'Molde' };
assert.equal(l.trackChip(l.trackPick(card)), 'Landed');
assert.equal(l.trackPick({ ...card, pick: { list: 'strength', favourite: 'Molde', big_dog: 'Molde' } }).status, 'off_track');
assert.equal(l.trackPick({ ...card, pick: { list: 'patterns', team: 'Molde', event: 'goalless', k: 5, n: 9, context: 'away' } }).status, 'on_track');
assert.equal(l.trackPick({ ...card, pick: null }), null);

// Clock and staleness
assert.equal(l.clockLabel({ status: '2H', minute: 90, extra: 3 }), "90+3'");
assert.equal(l.clockLabel({ status: 'HT', minute: 45, extra: null }), 'HT');
assert.equal(l.clockLabel({ status: 'PEN', minute: 120, extra: null }), 'FT (pens)');
assert.equal(l.isStale({ finished: false, polled_at: new Date(Date.now() - 120000).toISOString() }), true);
assert.equal(l.isStale({ finished: true, polled_at: new Date(Date.now() - 120000).toISOString() }), false);
require.extensions['.ts'] = original;
console.log('PASS: goals markets, team patterns by side, strength, full-time grading, extra time skipped, list chips, clock labels');
