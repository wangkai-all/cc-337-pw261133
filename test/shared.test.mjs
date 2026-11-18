// Pure domain logic: identity, content identity, whole-stream identity, ordering
// and gap grouping. These functions are the contract the four processes share,
// so they are tested without any sockets or SQLite.
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  basisKey,
  buildFeed,
  contentFp,
  deriveGlobal,
  manifestFp,
  planFor,
  quoteOf,
  stableStringify,
} from '../server/shared.mjs';
import { groupGaps } from '../server/backfiller.mjs';
import { classRank } from '../server/receiver.mjs';

const q = { symbol: 'AAPL', tick: 3, px: 101.5, qty: 400, side: 'bid' };

test('contentFp ignores numbering: same quote, same fingerprint', () => {
  const a = contentFp(q);
  const b = contentFp({ ...q, tick: 3 });
  assert.equal(a, b);
  assert.notEqual(a, contentFp({ ...q, px: 101.5001 }));
  assert.notEqual(a, contentFp({ ...q, qty: 401 }));
  assert.notEqual(a, contentFp({ ...q, side: 'ask' }));
});

test('contentFp is stable under object key order', () => {
  const reordered = { px: q.px, qty: q.qty, side: q.side, tick: q.tick, symbol: q.symbol };
  assert.equal(contentFp(reordered), contentFp(q));
});

test('manifestFp hashes the ordered (global, fp) list, not just the count', () => {
  const feed = buildFeed({ seed: 7, ticks: 12 });
  const fp = manifestFp(feed.messages);
  assert.equal(manifestFp(feed.messages), fp, 'deterministic');
  const swapped = feed.messages.slice();
  [swapped[0], swapped[1]] = [swapped[1], swapped[0]];
  assert.notEqual(manifestFp(swapped), fp, 'order matters');
  const missing = feed.messages.slice(0, -1);
  assert.notEqual(manifestFp(missing), fp, 'length matters');
});

test('manifestFp accepts quote-only messages and computes their content fp', () => {
  const feed = buildFeed({ seed: 11, ticks: 6 });
  const stripped = feed.messages.map((m) => ({ global: m.global, ...quoteOf(m) }));
  assert.equal(manifestFp(stripped), manifestFp(feed.messages));
});

test('stableStringify sorts object keys recursively', () => {
  assert.equal(stableStringify({ b: 1, a: { d: 2, c: 3 } }), '{"a":{"c":3,"d":2},"b":1}');
  assert.equal(stableStringify([{ y: 1, x: 2 }]), '[{"x":2,"y":1}]');
});

test('deriveGlobal maps a released number only through an explicit basis', () => {
  const basis = { epoch: 5001, releaseStart: 1, globalStart: 84 };
  assert.equal(deriveGlobal(1, basis), 84);
  assert.equal(deriveGlobal(9, basis), 92);
  assert.equal(deriveGlobal(0, basis), null, 'below releaseStart is not a position');
  assert.equal(deriveGlobal(5, null), null, 'no basis, no guess');
});

test('a new epoch with a fresh basis can restart release numbering', () => {
  const oldBasis = { epoch: 5000, releaseStart: 1, globalStart: 1 };
  const newBasis = { epoch: 5001, releaseStart: 1, globalStart: 84 };
  // release 84 exists in both numbering spaces; identity decides which global.
  assert.equal(deriveGlobal(84, oldBasis), 84);
  assert.equal(deriveGlobal(84, newBasis), 167);
  assert.notEqual(basisKey(oldBasis), basisKey(newBasis));
});

test('basisKey changes when globalStart changes for the same release window', () => {
  const a = { sourceRun: 'r1', epoch: 5, releaseStart: 1, globalStart: 10, source: 'live' };
  assert.equal(basisKey(a), basisKey({ ...a }));
  assert.notEqual(basisKey(a), basisKey({ ...a, globalStart: 11 }));
  assert.notEqual(basisKey(a), basisKey({ ...a, source: 'replay' }));
});

test('buildFeed is deterministic per seed and numbers contiguous globals', () => {
  const a = buildFeed({ seed: 29, ticks: 40 });
  const b = buildFeed({ seed: 29, ticks: 40 });
  assert.equal(a.total, b.total);
  assert.deepEqual(a.messages.map((m) => m.global), b.messages.map((m) => m.global));
  assert.deepEqual(a.messages, b.messages);
  assert.deepEqual(
    a.messages.map((m) => m.global),
    Array.from({ length: a.total }, (_, i) => i + 1)
  );
  assert.ok(a.prefill > 0 && a.prefill < a.total);
  assert.notDeepEqual(buildFeed({ seed: 30, ticks: 40 }).messages, a.messages);
});

test('groupGaps merges runs, sorts, dedupes and never exceeds one span', () => {
  assert.deepEqual(groupGaps([{ global: 3 }, { global: 1 }, { global: 2 }]), [{ from: 1, to: 3 }]);
  assert.deepEqual(
    groupGaps([{ global: 5 }, { global: 6 }, { global: 9 }, { global: 1 }, { global: 1 }]),
    [{ from: 1, to: 1 }, { from: 5, to: 6 }, { from: 9, to: 9 }]
  );
  const long = Array.from({ length: 120 }, (_, i) => ({ global: i + 1 }));
  const ranges = groupGaps(long);
  assert.ok(ranges.length > 1, 'a long run is split into inspectable sessions');
  for (const r of ranges) assert.ok(r.to - r.from < 48);
  assert.equal(ranges.at(0).from, 1);
  assert.equal(ranges.at(-1).to, 120);
});

test('source classes rank replay above live above a stalled tail', () => {
  assert.ok(classRank({ origin: 'replay' }) > classRank({ origin: 'live' }));
  assert.ok(classRank({ origin: 'live' }) > classRank({ origin: 'tail' }));
});

test('chaos plan places its stall after prefill and inside the stream', () => {
  const plan = planFor('chaos');
  assert.ok(plan.stallGlobal > plan.prefill);
  assert.ok(plan.stallGlobal < plan.total);
  assert.ok(plan.faultTimes.tailAt > plan.faultTimes.stallAt);
  assert.ok(plan.faultTimes.closeAt > plan.faultTimes.tailAt);
  const steady = planFor('steady');
  assert.equal(steady.hasFaults, false);
  assert.equal(steady.stallGlobal, steady.total + 1);
});
