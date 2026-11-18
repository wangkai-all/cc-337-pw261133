// End-to-end: boot the whole desk (source + receiver + backfiller + lander) as
// real processes in a temp directory at a fast publish cadence, let it finish,
// and check the three independent stores against the supplier's manifest.
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createServer } from 'node:net';

const ROOT = resolve(import.meta.dirname, '..');
const FAST = '6'; // ms per publish tick

async function freePort() {
  return new Promise((res, rej) => {
    const s = createServer();
    s.on('error', rej);
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address();
      s.close(() => res(port));
    });
  });
}

async function boot(t, preset) {
  const [controlPort, sourcePort] = await Promise.all([freePort(), freePort()]);
  const dataDir = mkdtempSync(join(tmpdir(), 'desk-e2e-'));
  const base = `http://127.0.0.1:${controlPort}`;
  const proc = spawn(process.execPath, [join(ROOT, 'server/index.mjs')], {
    cwd: ROOT,
    env: {
      ...process.env,
      PORT: String(controlPort),
      SOURCE_PORT: String(sourcePort),
      FEED_INTERVAL_MS: FAST,
      DATA_DIR: dataDir,
      PRESET: preset,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  proc.stdout.on('data', (d) => (log += d));
  proc.stderr.on('data', (d) => (log += d));
  t.after(async () => {
    proc.kill('SIGTERM');
    await new Promise((r) => {
      const to = setTimeout(() => {
        proc.kill('SIGKILL');
        r();
      }, 3000);
      proc.once('exit', () => {
        clearTimeout(to);
        r();
      });
    });
    rmSync(dataDir, { recursive: true, force: true });
  });
  // wait for the control API
  const deadline = Date.now() + 15000;
  for (;;) {
    try {
      const res = await fetch(`${base}/api/overview`);
      if (res.ok) break;
    } catch {
      /* not up yet */
    }
    if (Date.now() > deadline) throw new Error(`desk did not start:\n${log}`);
    await new Promise((r) => setTimeout(r, 100));
  }
  return { base, log: () => log, dataDir };
}

async function waitFor(fn, { timeout = 45000, interval = 150, label = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    last = await fn();
    if (last?.done) return last.value;
    await new Promise((r) => setTimeout(r, interval));
  }
  throw new Error(`timed out waiting for ${label}; last=${JSON.stringify(last)}`);
}

async function overview(base) {
  return (await fetch(`${base}/api/overview`)).json();
}

async function post(base, pathname, body = {}) {
  const res = await fetch(`${base}${pathname}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

test('steady: one live line ends verified, landed and matching the source manifest', async (t) => {
  const { base } = await boot(t, 'steady');
  const ov = await waitFor(
    async () => {
      const o = await overview(base);
      return { done: o.receiver?.verifiedComplete === true, value: o };
    },
    { label: 'receiver.verifiedComplete' }
  );
  assert.equal(ov.receiver.receivedComplete, true);
  assert.equal(ov.receiver.gaps.length, 0);
  assert.equal(ov.receiver.tentative.length, 0);
  assert.equal(ov.receiver.conflicts.length, 0);

  const landed = await waitFor(
    async () => {
      const o = await overview(base);
      return {
        done: o.lander?.stats?.acked === o.receiver.total && o.lander.stats.ackedCursor === o.receiver.total,
        value: o.lander.stats,
      };
    },
    { label: 'lander acked all' }
  );
  assert.equal(landed.provisional, 0);
  assert.equal(landed.revisions, 0);

  const rec = await (await fetch(`${base}/api/reconcile`)).json();
  assert.equal(rec.complete, true);
  assert.equal(rec.fingerprintMatch, true);
  assert.equal(rec.mismatchCount, 0);
  assert.equal(rec.landed, ov.receiver.total);
  assert.equal(rec.manifestFp, rec.landedFp);
});

test('chaos: reconnects, replay sessions, arbitration and a lander crash recovery all end reconciled', async (t) => {
  const { base, log } = await boot(t, 'chaos');
  const final = await waitFor(
    async () => {
      const o = await overview(base);
      const rec = await (await fetch(`${base}/api/reconcile`)).json();
      const done =
        o.receiver?.verifiedComplete === true &&
        rec.complete === true &&
        o.lander?.stats?.ackedCursor === o.receiver.total;
      return { done, value: { o, rec } };
    },
    { label: 'chaos run reconciled', timeout: 60000 }
  );
  const { o, rec } = final;

  // The old line's late tail was used, and a numbered replay outranked it.
  assert.ok(o.receiver.origins.tail > 0, 'the stalled line contributed tail frames');
  assert.ok(o.receiver.origins.replay > 0, 'numbered replay contributed frames');
  assert.equal(o.receiver.tentative.length, 0, 'no tail guess is left unconfirmed');
  assert.ok(o.receiver.gapsFilled > 0, 'holes were filled');
  assert.ok(o.receiver.conflicts.length >= 1, 'the conflicting tail and live line were sealed');
  assert.ok(o.receiver.conflicts.every((c) => c.decision === 'decided'));
  assert.ok(o.receiver.conflicts.some((c) => c.decided_by === 'backfiller'), 'replay decided a conflict');

  // Backfill sessions are independent records, including their failures.
  assert.ok(o.backfiller.sessions.length >= 2);
  assert.ok(o.backfiller.sessions.some((s) => s.state === 'aborted' || s.state === 'incomplete'), 'a replay attempt failed visibly');

  // The lander crashed mid-batch and recovered by rolling the batch back.
  assert.ok(o.lander.recovered.some((r) => r.kind === 'rolled-back'), `no rollback in ${log().slice(-1500)}`);
  assert.equal(o.lander.stats.provisional, 0);
  assert.equal(o.lander.stats.staged, 0);
  assert.equal(o.lander.stats.committed, 0);

  // Final result equals the source's complete manifest, per row and in total.
  assert.equal(rec.complete, true);
  assert.equal(rec.fingerprintMatch, true);
  assert.equal(rec.mismatchCount, 0);
  assert.equal(rec.landed, o.receiver.total);
});

test('hold: retire first, reject the old tail, pay for it in replay — same final result', async (t) => {
  const { base } = await boot(t, 'chaos');
  await post(base, '/api/reset', { preset: 'chaos', policy: 'hold' });
  const final = await waitFor(
    async () => {
      const o = await overview(base);
      const rec = await (await fetch(`${base}/api/reconcile`)).json();
      const done =
        o.receiver?.policy === 'hold' &&
        o.receiver?.verifiedComplete === true &&
        rec.complete === true &&
        o.lander?.stats?.ackedCursor === o.receiver.total;
      return { done, value: { o, rec } };
    },
    { label: 'hold run reconciled', timeout: 60000 }
  );
  const { o, rec } = final;
  assert.equal(o.receiver.policy, 'hold');
  // No tail was ever accepted: the old line was retired before it flushed.
  assert.equal(o.receiver.origins.tail ?? 0, 0);
  assert.ok(
    (o.receiver.rejected ?? 0) > 0 ||
      o.receiver.connections.some((c) => c.state === 'retired'),
    'the old line was retired (and any late frame refused by identity)'
  );
  // The cost moved to replay: every hole was filled by numbered frames, and no
  // number was ever covered by a guess.
  assert.ok(o.receiver.origins.replay > 0, `replay did the filling: ${o.receiver.origins.replay}`);
  assert.ok(o.receiver.gapsFilled > 0, 'holes were closed by replay');
  assert.equal(o.receiver.conflicts.length, 0, 'no tails, so no tail conflicts');
  assert.equal(o.receiver.tentative.length, 0);
  // Same destination as overlap.
  assert.equal(rec.complete, true);
  assert.equal(rec.fingerprintMatch, true);
  assert.equal(rec.mismatchCount, 0);
});

test('pause freezes receive and landing; resume continues from the durable cursor', async (t) => {
  const { base } = await boot(t, 'steady');
  // Let some data flow.
  await waitFor(
    async () => {
      const o = await overview(base);
      return { done: o.receiver?.cursor > 8, value: o };
    },
    { label: 'some coverage' }
  );
  await post(base, '/api/pause', { paused: true });
  await new Promise((r) => setTimeout(r, 300));
  const a = await overview(base);
  assert.equal(a.receiver.paused, true);
  const cursorAtPause = a.receiver.cursor;
  const landedAtPause = a.lander.stats.ackedCursor;
  await new Promise((r) => setTimeout(r, 900));
  const b = await overview(base);
  assert.equal(b.receiver.cursor, cursorAtPause, 'receiver cursor must not move while paused');
  assert.equal(b.lander.stats.ackedCursor, landedAtPause, 'lander must not advance while paused');
  assert.ok(b.receiver.cursor < b.receiver.total, 'the run was paused mid-stream');

  await post(base, '/api/pause', { paused: false });
  const done = await waitFor(
    async () => {
      const o = await overview(base);
      return { done: o.receiver?.verifiedComplete === true && o.lander?.stats?.ackedCursor === o.receiver.total, value: o };
    },
    { label: 'completion after resume', timeout: 45000 }
  );
  assert.equal(done.receiver.paused, false);
  const rec = await (await fetch(`${base}/api/reconcile`)).json();
  assert.equal(rec.complete, true);
  assert.equal(rec.fingerprintMatch, true);
});

test('page-triggered faults: a late tail conflicts with the live line, is outranked, and the run still reconciles', async (t) => {
  const { base } = await boot(t, 'steady');
  await waitFor(
    async () => {
      const o = await overview(base);
      return { done: o.receiver?.cursor > 20, value: o };
    },
    { label: 'some coverage' }
  );
  // This is exactly what the 故障台 "制造冲突" card posts.
  const armed = await post(base, '/api/trigger', { kind: 'stall', conflictOffset: 0, tailCount: 5 });
  assert.equal(armed.ok, true);

  const conflicted = await waitFor(
    async () => {
      const o = await overview(base);
      const c = o.receiver?.conflicts?.[0];
      return { done: !!c && c.decision === 'decided', value: { o, c } };
    },
    { label: 'conflict decided', timeout: 30000 }
  );
  const { o, c } = conflicted;
  assert.ok(o.receiver.origins.tail > 0, 'the old line flushed a tail');
  assert.ok(o.receiver.origins.live > 0);
  assert.equal(c.decision, 'decided');
  // live (class 2) beats tail (class 1); the source manifest agrees.
  const trace = await (await fetch(`${base}/api/trace/${c.global}`)).json();
  assert.equal(trace.receiver.coverage.grade, 'firm');
  assert.equal(trace.receiver.coverage.fp, trace.manifest.fp);
  assert.ok(trace.receiver.frames.some((f) => f.origin === 'tail' && f.decision === 'rejected'));
  assert.ok(trace.receiver.frames.some((f) => f.origin === 'live' && f.decision === 'accepted'));

  // Nothing about triggering a fault weakens the final completeness claim.
  await waitFor(
    async () => {
      const x = await overview(base);
      const rec = await (await fetch(`${base}/api/reconcile`)).json();
      return {
        done: x.receiver?.verifiedComplete === true && rec.complete === true && x.lander?.stats?.ackedCursor === x.receiver.total,
        value: x,
      };
    },
    { label: 'reconciled after a page-triggered fault', timeout: 45000 }
  );
});

test('page-triggered lander crash (after-commit) recovers row-by-row and keeps landing', async (t) => {
  const { base } = await boot(t, 'steady');
  await waitFor(
    async () => {
      const o = await overview(base);
      return { done: o.receiver?.cursor > 20, value: o };
    },
    { label: 'some coverage' }
  );
  const armed = await post(base, '/api/lander', { path: '/crash', body: { mode: 'after-commit' } });
  assert.equal(armed.armed, 'after-commit');

  const recovered = await waitFor(
    async () => {
      const o = await overview(base);
      const r = o.lander?.recovered || [];
      return { done: r.length > 0, value: { o, r } };
    },
    { label: 'lander recovered', timeout: 30000 }
  );
  assert.ok(recovered.r.some((x) => x.kind === 'committed-unacked'));
  assert.ok(recovered.r.every((x) => x.missing === 0 && x.divergent === 0));

  const done = await waitFor(
    async () => {
      const o = await overview(base);
      return { done: o.lander?.stats?.ackedCursor === o.receiver.total, value: o };
    },
    { label: 'landing continues after recovery', timeout: 45000 }
  );
  assert.equal(done.lander.stats.staged, 0);
  assert.equal(done.lander.stats.committed, 0);
  const rec = await (await fetch(`${base}/api/reconcile`)).json();
  assert.equal(rec.complete, true);
});
