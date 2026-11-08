'use strict';
// Run from repository root: node --test tests/work-motivator.test.cjs
// Read the actual distributable HTML, not a second copy of its calculation code.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const html=fs.readFileSync(path.join(__dirname,'../apps/web/public/work-motivator/index.html'),'utf8');
const core=html.match(/<script id="sloan-core">([\s\S]*?)<\/script>/);
assert.ok(core,'Embedded calculation core is present');
for(const script of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)) new vm.Script(script[1]);
const context={module:{exports:{}},Intl,Date};
vm.runInNewContext(core[1],context);
const C=context.module.exports;
const T='America/New_York';
const at=s=>C.fromLocal(s,T);
const approx=(a,b,e=1e-8)=>assert.ok(Math.abs(a-b)<e,`${a} != ${b}`);
function session(start,end,rate=20,id='a'){return {id,note:'work',hourlyRate:rate,segments:[{start,end}],runningSince:null,startedAt:start,source:'timer'};}

test('Annualized $5000 and 60h/week, per-second accrual',()=>{
  approx(C.hourlyRate(5000,60),19.23076923076923);
  approx(C.totals([{start:0,end:1000,rate:C.hourlyRate(5000,60)}]).earned,0.00534188034188);
});
test('Reject invalid payroll values',()=>{
  for(const x of [-1,Infinity,NaN])assert.throws(()=>C.hourlyRate(x,60));
  for(const h of [0,0.001,-1,169,NaN])assert.throws(()=>C.hourlyRate(5000,h));
});
test('Week starts Monday, including Sunday and year boundaries',()=>{
  assert.equal(C.weekStart('2026-11-08'),'2026-11-02');
  assert.equal(C.weekStart('2026-11-09'),'2026-11-09');
  assert.equal(C.weekStart('2027-01-01'),'2026-12-28');
});
test('Pauses excluded and hourly rate locked per record',()=>{
  const s=session(0,3600000,20);s.segments.push({start:7200000,end:9000000});
  approx(C.totals(C.sessionSegments(s)).duration,5400000);
  approx(C.totals(C.sessionSegments(s)).earned,30);
});
test('Cross-midnight records split correctly',()=>{
  const a=at('2026-11-05T23:30'),b=at('2026-11-06T01:30');
  const days=C.aggregateDays(C.sessionSegments(session(a,b)),['2026-11-05','2026-11-06'],T);
  approx(days[0].duration/C.HOUR,.5);approx(days[1].duration/C.HOUR,1.5);
});
test('Sunday to Monday splits across weeks',()=>{
  const a=at('2026-11-08T23:30'),b=at('2026-11-09T00:30');
  const w=C.period('2026-11-08','week',T);
  approx(C.totals(C.sessionSegments(session(a,b)),w.start,w.end).duration,1800000);
});
test('DST spring gap rejected; real 23h day',()=>{
  assert.throws(()=>at('2026-03-08T02:30'));
  approx((C.midnight('2026-03-09',T)-C.midnight('2026-03-08',T))/C.HOUR,23);
});
test('DST fall repeated hour rejected; real 25h day',()=>{
  assert.throws(()=>at('2026-11-01T01:30'));
  assert.equal(C.localCandidates('2026-11-01T01:30',T).length,2);
  approx((C.midnight('2026-11-02',T)-C.midnight('2026-11-01',T))/C.HOUR,25);
});
test('Snapshot pauses running session without mutating live state',()=>{
  const s=C.newState();s.current={id:'live',note:'abc',hourlyRate:20,segments:[],runningSince:1000,startedAt:1000};
  const copy=C.snapshot(s,11000);assert.equal(s.current.runningSince,1000);
  assert.equal(copy.current.runningSince,null);
  approx(C.totals(C.sessionSegments(copy.current)).earned,20/360);
});
test('Overlapping records and duplicated IDs rejected',()=>{
  const s=C.newState();s.sessions=[session(1,1000),session(900,1500,20,'b')];
  assert.throws(()=>C.validateState(s));s.sessions[1]=session(2000,2500);
  assert.throws(()=>C.validateState(s));
});
test('Adjacent intervals do not double-count',()=>{
  assert.doesNotThrow(()=>C.assertNoOverlap([{start:1,end:1000},{start:1000,end:2000}]));
});
test('Import normalizes fields and rejects future or malformed input',()=>{
  const s=C.newState();s.sessions=[session(1,1000)];s.sessions[0].organization='other';s.evil=true;
  const out=C.validateState(s);assert.equal(out.sessions[0].organization,'MIT Sloan');
  assert.equal(out.evil,undefined);s.sessions[0].segments[0].end=Date.now()+10000;
  assert.throws(()=>C.validateState(s));
});
test('CSV quotes values, preserves Unicode and neutralizes formulas',()=>{
  const c=C.csv([['中文','a,b','a"b','=1+1',' +cmd','@a','-cmd'],[123,-7]]);
  assert.ok(c.startsWith('\uFEFF'));assert.ok(c.includes('"a""b"'));
  assert.ok(c.includes('"\'=1+1"'));assert.ok(c.includes('"-7"'));
});
test('Empty month totals and leap February',()=>{
  const p=C.period('2028-02-28','month',T);assert.equal(p.days.length,29);
  const totals=C.totals([],p.start,p.end);assert.equal(totals.duration,0);assert.equal(totals.earned,0);
});
test('Active session clipped at period boundary',()=>{
  const s=session(at('2026-11-08T23:00'),at('2026-11-08T23:30'));
  s.runningSince=at('2026-11-08T23:45');const p=C.period('2026-11-09','week',T);
  approx(C.totals(C.sessionSegments(s,at('2026-11-09T01:00')),p.start,p.end).duration,C.HOUR);
});
