// Lander contract tests: the three batch states, the two crash windows, the
// amendment path for provisional rows, and reconciliation against the source
// manifest. The HTTP pull loop is not exercised; batches are driven directly,
// which is the same code path /decisions feeds.
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LanderCore } from '../server/lander.mjs';
import { buildFeed, contentFp, manifestFp, quoteOf } from '../server/shared.mjs';

function makeDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'ld-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function makeCore(t, dir, opts = {}) {
  const core = new LanderCore({ dbPath: join(dir, 'lander.db'), receiverUrl: 'http://unused', ...opts });
  t.after(() => {
    core.stop();
    try {
      core.db.close();
    } catch {
      /* already closed by the crash simulation */
    }
  });
  return core;
}

const item = (m, kind = 'new', extra = {}) => ({
  global: m.global,
  fp: contentFp(quoteOf(m)),
  frameId: m.global,
  origin: 'live',
  provisional: false,
  kind,
  ...extra,
});

const feed = buildFeed({ seed: 5, ticks: 8 });
const manifest = {
  total: feed.total,
  totalFp: manifestFp(feed.messages),
  messages: feed.messages,
};

test('a batch lands committed then acked, and the acked cursor is contiguous', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  const items = manifest.messages.slice(0, 6).map((m) => item({ ...m }));
  core.landBatch(items);
  const st = core.stats();
  assert.equal(st.count, 6);
  assert.equal(st.acked, 6);
  assert.equal(st.ackedCursor, 6);
  assert.equal(st.batches, 1);
  assert.equal(core.db.prepare("SELECT state FROM batches").get().state, 'acked');
});

test('a gap in landed globals stops the acked cursor, not the row count', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  core.landBatch(manifest.messages.slice(0, 3).map((m) => item({ ...m })));
  core.landBatch(manifest.messages.slice(4, 7).map((m) => item({ ...m }))); // skip global 4
  const st = core.stats();
  assert.equal(st.count, 6);
  assert.equal(st.acked, 6);
  assert.equal(st.ackedCursor, 3);
});

test('a crash mid-transaction leaves a staging batch; restart rolls it back', (t) => {
  const dir = makeDir(t);
  // Simulate the on-disk signature of a process killed inside BEGIN..COMMIT:
  // the batch header and item rows exist, no landed rows do, state is staging.
  const core0 = makeCore(t, dir);
  const items = manifest.messages.slice(0, 4).map((m) => item({ ...m }));
  core0.db.prepare("INSERT INTO batches (batch_id, opened_at, item_count, state) VALUES ('B-mid', ?, ?, 'staging')").run(Date.now(), items.length);
  for (const it of items) {
    core0.db.prepare('INSERT INTO batch_items (batch_id, global, fp, kind) VALUES (?,?,?,?)').run('B-mid', it.global, it.fp, it.kind);
  }
  core0.stop();
  core0.db.close();

  const core1 = makeCore(t, dir);
  assert.deepEqual(core1.recovered, [{ kind: 'rolled-back', batchId: 'B-mid', items: 4 }]);
  assert.equal(core1.db.prepare("SELECT state FROM batches WHERE batch_id='B-mid'").get().state, 'rolled-back');
  assert.equal(core1.db.prepare('SELECT COUNT(*) AS n FROM batch_items WHERE batch_id=?').get('B-mid').n, 0);
  assert.equal(core1.stats().count, 0);
  assert.ok(core1.state().events.some((e) => e.type === 'recover.rollback'));

  // The redo after restart is a fresh batch and counts normally.
  core1.landBatch(items);
  assert.equal(core1.stats().ackedCursor, 4);
});

test('a crash after COMMIT before ACK is recovered by verifying every row', (t) => {
  const dir = makeDir(t);
  let fatal = null;
  const core0 = makeCore(t, dir, { crashMode: 'after-commit', onFatal: (mode, batchId) => (fatal = { mode, batchId }) });
  const items = manifest.messages.slice(0, 5).map((m) => item({ ...m }));
  core0.landBatch(items);
  assert.equal(fatal.mode, 'after-commit');
  const batch = core0.db.prepare('SELECT * FROM batches').get();
  assert.equal(batch.state, 'committed', 'durable but unacknowledged');
  assert.equal(core0.stats().acked, 0, 'not counted as progress yet');
  assert.equal(core0.stats().ackedCursor, 0);
  core0.stop();
  core0.db.close();

  const core1 = makeCore(t, dir);
  assert.deepEqual(core1.recovered, [{ kind: 'committed-unacked', batchId: batch.batch_id, items: 5, missing: 0, divergent: 0 }]);
  assert.equal(core1.stats().acked, 5);
  assert.equal(core1.stats().ackedCursor, 5);
  assert.equal(core1.db.prepare('SELECT state FROM batches').get().state, 'acked');
});

test('committed-unacked recovery refuses to ack a divergent row', (t) => {
  const dir = makeDir(t);
  const core0 = makeCore(t, dir, { crashMode: 'after-commit', onFatal: () => {} });
  const items = manifest.messages.slice(0, 3).map((m) => item({ ...m }));
  core0.landBatch(items);
  // Corrupt one durable row: the batch manifest is the authority now.
  core0.db.prepare("UPDATE landed SET fp='corrupt' WHERE global=?").run(items[1].global);
  core0.stop();
  core0.db.close();

  const core1 = makeCore(t, dir);
  assert.equal(core1.recovered[0].kind, 'committed-corrupt');
  assert.equal(core1.recovered[0].divergent, 1);
  assert.equal(core1.stats().acked, 0, 'nothing is acked by a failed verification');
  assert.equal(core1.db.prepare('SELECT state FROM batches').get().state, 'committed');
  assert.ok(core1.state().events.some((e) => e.type === 'recover.diverged'));
});

test('a corrected value amends the row, records an amendment and bumps revision', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  const m = feed.messages[0];
  core.landBatch([item({ ...m })]);
  const before = core.landGlobal(m.global);
  assert.equal(before.revision, 0);
  // The arbitration winner changed the content for this global.
  const newFp = contentFp({ ...quoteOf(m), px: Number(m.px) + 1 });
  core.landBatch([{ ...item({ ...m }), fp: newFp, kind: 'amend', fromFp: before.fp }]);
  const after = core.landGlobal(m.global);
  assert.equal(after.fp, newFp);
  assert.equal(after.revision, 1);
  assert.equal(after.superseded_fp, before.fp);
  assert.equal(after.amendments.length, 1);
  assert.equal(after.amendments[0].from_fp, before.fp);
  assert.equal(after.amendments[0].to_fp, newFp);
  assert.ok(core.state().amendments.length >= 1);
});

test('provisional rows are landed but never count as verified complete', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  core.landBatch(manifest.messages.slice(0, 4).map((m) => item({ ...m })));
  const m = feed.messages[4];
  core.landBatch([{ ...item({ ...m }), provisional: true }]);
  let rec = core.reconcile(manifest);
  assert.equal(rec.provisional, 1);
  assert.equal(rec.complete, false);
  assert.ok(rec.mismatch.some((x) => x.global === m.global && x.kind === 'provisional'));
  // Settling the arbitration lands the same fp with provisional cleared.
  core.landBatch([{ ...item({ ...m }), provisional: false, kind: 'settle', fromFp: item(m).fp }]);
  assert.equal(core.landGlobal(m.global).provisional, 0);
  assert.equal(core.stats().provisional, 0);
});

test('reconcile matches a complete stream against the source manifest', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  core.landBatch(manifest.messages.map((m) => item({ ...m })));
  const rec = core.reconcile(manifest);
  assert.equal(rec.complete, true);
  assert.equal(rec.mismatchCount, 0);
  assert.equal(rec.fingerprintMatch, true);
  assert.equal(rec.landedFp, rec.manifestFp);
  assert.equal(rec.acked, manifest.total);
  assert.equal(rec.cursor, manifest.total);
});

test('reconcile reports missing, divergent, unacked and extra rows separately', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  // Land everything except global 2, with global 4's content wrong.
  const items = manifest.messages.filter((m) => m.global !== 2).map((m) => item({ ...m }));
  const target = items.find((it) => it.global === 4);
  target.fp = contentFp({ ...quoteOf(feed.messages.find((m) => m.global === 4)), qty: 1 });
  core.landBatch(items);
  core.db
    .prepare('INSERT INTO landed (global, fp, batch_id, landed_at, acked) VALUES (?,?,?,?,1)')
    .run(manifest.total + 5, 'junk', 'B-extra', Date.now());
  const rec = core.reconcile(manifest);
  assert.equal(rec.complete, false);
  assert.equal(rec.fingerprintMatch, false);
  const kinds = new Map(rec.mismatch.map((m) => [m.global, m.kind]));
  assert.equal(kinds.get(2), 'missing');
  assert.equal(kinds.get(4), 'content');
  assert.equal(kinds.get(manifest.total + 5), 'extra');
  assert.equal(rec.mismatchCount, 3);
});

test('landing progress is independent from the receiver cursor', (t) => {
  const dir = makeDir(t);
  const core = makeCore(t, dir);
  assert.equal(core.stats().ackedCursor, 0);
  const st = core.stats();
  assert.equal(st.maxLanded, 0);
  // Rows can arrive out of order and still not advance the cursor.
  const items = manifest.messages.slice(2, 5).map((m) => item({ ...m }));
  core.landBatch(items);
  assert.equal(core.stats().maxLanded, items.at(-1).global);
  assert.equal(core.stats().ackedCursor, 0);
});
