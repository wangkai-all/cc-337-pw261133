// Receiver contract tests: identity quarantine, tail grade, conflict sealing and
// arbitration, and the two separate completion claims. No sockets are opened;
// frames are fed through the same ingest() path a live line uses.
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ReceiverCore } from '../server/receiver.mjs';
import { basisKey, contentFp } from '../server/shared.mjs';

function makeCore(t) {
  const dir = mkdtempSync(join(tmpdir(), 'rx-'));
  const core = new ReceiverCore({ dbPath: join(dir, 'receiver.db') });
  t.after(() => {
    core.stop();
    core.db.close();
    rmSync(dir, { recursive: true, force: true });
  });
  return core;
}

const basis = (epoch, releaseStart, globalStart, source = 'live') => ({
  sourceRun: 'run-test',
  epoch,
  releaseStart,
  globalStart,
  source,
  key: basisKey({ sourceRun: 'run-test', epoch, releaseStart, globalStart, source }),
});

const quote = (n, px = 100 + n) => ({ symbol: 'AAPL', tick: n, px, qty: 100 + n, side: n % 2 ? 'bid' : 'ask' });

function frameFor(connId, epoch, release, n, { px = 100 + n, tail, origin } = {}) {
  const q = quote(n, px);
  return { connId, epoch, release, quote: q, fp: contentFp(q), tail, origin };
}

test('a connection without a basis quarantines every frame instead of guessing', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-bare', role: 'live', basis: null });
  const res = core.ingest(frameFor('L-bare', 1, 1, 1));
  assert.equal(res.ok, false);
  assert.equal(res.reason, 'no-basis');
  assert.equal(core.state().quarantine, 1);
  assert.equal(core.state().covered, 0);
});

test('frames on a retired connection are rejected; the replacement keeps ingesting', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-old', role: 'live', basis: basis(5000, 1, 1) });
  core.registerConnection({ connId: 'L-new', role: 'live', basis: basis(5001, 1, 84) });
  assert.equal(core.ingest(frameFor('L-old', 5000, 5, 5)).accepted, true);
  core.setConnectionState('L-old', 'retired', 'old status notice');
  const late = core.ingest(frameFor('L-old', 5000, 6, 6, { tail: true }));
  assert.equal(late.ok, false);
  assert.equal(late.reason, 'retired-connection');
  // The old line's retirement is scoped to its own connId: the new line is alive.
  const fresh = core.ingest(frameFor('L-new', 5001, 1, 84));
  assert.equal(fresh.ok, true);
  assert.equal(fresh.global, 84);
  assert.equal(core.connection('L-new').state, 'live');
});

test('an epoch that disagrees with the declared basis is quarantined', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-1', role: 'live', basis: basis(5000, 1, 1) });
  const res = core.ingest(frameFor('L-1', 4999, 3, 3));
  assert.equal(res.ok, false);
  assert.equal(res.reason, 'epoch-mismatch');
  assert.equal(core.state().covered, 0);
});

test('a release below the declared basis is quarantined', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'R-1', role: 'replay', basis: basis(5005, 10, 60, 'replay') });
  const res = core.ingest(frameFor('R-1', 5005, 9, 59));
  assert.equal(res.ok, false);
  assert.equal(res.reason, 'release-below-basis');
});

test('a claimed fingerprint that does not match the content is rejected', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-1', role: 'live', basis: basis(5000, 1, 1) });
  const res = core.ingest({ connId: 'L-1', epoch: 5000, release: 1, quote: quote(1), fp: 'deadbeef' });
  assert.equal(res.ok, false);
  assert.equal(res.reason, 'fp-mismatch');
});

test('a retry on the same socket+epoch+release is a duplicate, not new coverage', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-1', role: 'live', basis: basis(5000, 1, 1) });
  assert.equal(core.ingest(frameFor('L-1', 5000, 1, 1)).accepted, true);
  const again = core.ingest(frameFor('L-1', 5000, 1, 1));
  assert.equal(again.ok, true);
  assert.equal(again.duplicate, true);
  assert.equal(core.state().covered, 1);
});

test('tail coverage is tentative and only a numbered replay makes it firm', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-old', role: 'live', basis: basis(5000, 1, 1) });
  core.registerConnection({ connId: 'R-1', role: 'replay', basis: basis(5005, 2, 5, 'replay') });
  const tail = core.ingest(frameFor('L-old', 5000, 5, 5, { tail: true }));
  assert.equal(tail.grade, 'tentative');
  let st = core.state();
  assert.equal(st.tentative.length, 1);
  assert.equal(st.verifiedComplete, false, 'a guess is not a verified stream');
  // same content from a replay session: confirmed, grade promoted
  const replay = core.ingest(frameFor('R-1', 5005, 2, 5));
  assert.equal(replay.confirmed, true);
  assert.equal(replay.origin, 'replay');
  st = core.state();
  assert.equal(st.tentative.length, 0);
  assert.equal(core.trace(5).coverage.grade, 'firm');
  assert.ok(
    core.trace(5).events.some((e) => e.type === 'coverage.firmed'),
    'the promotion is recorded as an event'
  );
});

test('two paths with different content seal both variants, decide later, replay wins', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-old', role: 'live', basis: basis(5000, 1, 1) });
  core.registerConnection({ connId: 'L-new', role: 'live', basis: basis(5001, 1, 84) });
  core.registerConnection({ connId: 'R-1', role: 'replay', basis: basis(5100, 80, 80, 'replay') });

  // global 86: old line's late tail disagrees with the live line
  const a = core.ingest(frameFor('L-old', 5000, 86, 86, { tail: true, px: 999 }));
  assert.equal(a.grade, 'tentative');
  const b = core.ingest(frameFor('L-new', 5001, 3, 86));
  assert.equal(b.contested, true);
  let st = core.state();
  assert.equal(st.conflicts.length, 1);
  assert.equal(st.conflicts[0].decision, 'pending');
  assert.equal(st.verifiedComplete, false);
  assert.equal(core.decisions().find((d) => d.global === 86).provisional, true);

  // A replay for the same global arrives with third content and outranks both.
  const c = core.ingest(frameFor('R-1', 5100, 86, 86, { px: 131 }));
  assert.equal(c.contested, true);
  assert.equal(core.state().pendingConflicts.length, 1, 'still pending until a decision is recorded');

  const out = core.resolveConflict('CG-86', 'test');
  assert.equal(out.ok, true);
  assert.equal(out.via, 'replay');
  st = core.state();
  assert.equal(st.conflicts[0].decision, 'decided');
  assert.equal(st.conflicts[0].winner_fp, contentFp(quote(86, 131)));
  assert.equal(st.pendingConflicts.length, 0);
  const winner = core.trace(86).coverage;
  assert.equal(winner.fp, contentFp(quote(86, 131)));
  assert.equal(winner.grade, 'firm');
  assert.equal(core.decisions().find((d) => d.global === 86).provisional, false);
  assert.ok(
    core.recentEvents(50).some((e) => e.type === 'conflict.decided' && e.global === 86),
    'decision is on the event timeline'
  );
});

test('a pending conflict never settles by arrival order', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-old', role: 'live', basis: basis(5000, 1, 1) });
  core.registerConnection({ connId: 'L-new', role: 'live', basis: basis(5001, 1, 84) });
  // Put coverage on 84 from the live line, then let the old tail contradict it.
  core.ingest(frameFor('L-new', 5001, 1, 84));
  const late = core.ingest(frameFor('L-old', 5000, 84, 84, { tail: true, px: 111 }));
  assert.equal(late.contested, true);
  assert.equal(core.state().pendingConflicts.length, 1);
  // Arrival order says tail; class says live.
  const out = core.resolveConflict('CG-84', 'test');
  assert.equal(out.via, 'live');
  assert.equal(core.trace(84).coverage.fp, contentFp(quote(84)));
});

test('cursor advances only contiguously; skipped numbers become gaps and later fill', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-1', role: 'live', basis: basis(5000, 1, 1) });
  core.ingest(frameFor('L-1', 5000, 1, 1));
  core.ingest(frameFor('L-1', 5000, 2, 2));
  core.ingest(frameFor('L-1', 5000, 6, 6)); // skips 3..5
  let st = core.state();
  assert.equal(st.cursor, 2, 'cursor stops at the first hole');
  assert.deepEqual(st.gaps.map((g) => g.global), [3, 4, 5]);
  assert.equal(st.receivedComplete, false);
  core.ingest(frameFor('L-1', 5000, 3, 3));
  st = core.state();
  assert.equal(st.cursor, 3);
  core.ingest(frameFor('L-1', 5000, 4, 4));
  core.ingest(frameFor('L-1', 5000, 5, 5));
  st = core.state();
  assert.equal(st.cursor, 6);
  assert.deepEqual(st.gaps, []);
  assert.equal(st.gapsFilled, 3);
});

test('receivedComplete and verifiedComplete are different claims', (t) => {
  const core = makeCore(t);
  core.total = 3;
  core.registerConnection({ connId: 'L-old', role: 'live', basis: basis(5000, 1, 1) });
  core.ingest(frameFor('L-old', 5000, 1, 1, { tail: true }));
  core.ingest(frameFor('L-old', 5000, 2, 2, { tail: true }));
  core.ingest(frameFor('L-old', 5000, 3, 3, { tail: true }));
  let st = core.state();
  assert.equal(st.cursor, 3);
  assert.equal(st.receivedComplete, true, 'every number has content, even if guessed');
  assert.equal(st.verifiedComplete, false, 'but none of it is firmed');

  core.registerConnection({ connId: 'R-1', role: 'replay', basis: basis(5005, 1, 1, 'replay') });
  for (const n of [1, 2, 3]) core.ingest(frameFor('R-1', 5005, n, n));
  st = core.state();
  assert.equal(st.tentative.length, 0);
  assert.equal(st.verifiedComplete, true);
});

test('trace() returns the whole life of one global: frames, events, coverage', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-1', role: 'live', basis: basis(5000, 1, 1) });
  core.ingest(frameFor('L-1', 5000, 2, 2));
  core.ingest(frameFor('L-1', 5000, 3, 3));
  // Make global 2 notable so the lifecycle carries receiver events too: a
  // second socket replays the same global with different content.
  core.registerConnection({ connId: 'R-1', role: 'replay', basis: basis(5005, 10, 2, 'replay') });
  core.ingest(frameFor('R-1', 5005, 10, 2, { px: 999 }));
  const tr = core.trace(2);
  assert.equal(tr.global, 2);
  assert.equal(tr.coverage.global, 2);
  assert.equal(tr.frames.length, 2);
  assert.equal(tr.frames[0].conn_id, 'L-1');
  assert.equal(tr.frames[0].payload.symbol, 'AAPL');
  assert.ok(tr.events.some((e) => e.type === 'conflict.open'));
  assert.equal(core.trace(999).coverage, null);
});

test('markMissing turns the range below a live frontier into explicit gaps', (t) => {
  const core = makeCore(t);
  core.total = 10;
  core.registerConnection({ connId: 'L-1', role: 'live', basis: basis(5000, 1, 1) });
  core.ingest(frameFor('L-1', 5000, 1, 1));
  core.ingest(frameFor('L-1', 5000, 2, 2));
  assert.equal(core.markMissing(6), 4, 'globals 3..6 are unknowable from this socket');
  const st = core.state();
  assert.deepEqual(st.gaps.map((g) => g.global), [3, 4, 5, 6]);
  assert.equal(st.cursor, 2);
  assert.equal(core.markMissing(6), 0, 'idempotent');
  assert.ok(core.recentEvents(20).some((e) => e.type === 'gaps.marked'));
});

test('a retired line stays retired when its socket finally closes', (t) => {
  const core = makeCore(t);
  core.registerConnection({ connId: 'L-old', role: 'live', basis: basis(5000, 1, 1) });
  core.setConnectionState('L-old', 'retired', 'policy-hold');
  const line = { connId: 'L-old', state: 'retired', closed: false, supersededBy: 'retired' };
  const current = core.current;
  core.current = null; // ensure no reconnect scheduling in the test
  core.onClose(line);
  core.current = current;
  assert.equal(line.closed, true);
  assert.equal(core.connection('L-old').state, 'retired');
});
