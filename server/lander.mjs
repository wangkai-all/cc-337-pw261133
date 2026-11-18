// Lander (入库器) process. Pulls the receiver's current decisions and lands them
// in its own SQLite file, in transactional batches.
//
// Durability protocol — why the crash demo is honest:
//   staging batch  -> rows written inside a transaction
//   committed      -> COMMIT returned; rows are durable
//   acked          -> batch recorded as applied; progress may advance
// A crash can land in either window. A staging batch is rolled back by SQLite;
// a committed-but-unacked batch is durable but unacknowledged, so on restart
// we verify every row against the batch manifest, then acknowledge it. That is
// the only difference between "on disk" and "counted as progress".
//
// Provisional rows: a global whose conflict is still pending is landed with
// provisional=1. When the decision flips, the row is amended and an amendment
// row records what replaced what. Final completeness therefore needs
// (a) contiguous acked coverage, (b) no provisional rows, (c) manifest match.
import http from 'node:http';
import { createHash } from 'node:crypto';
import { openDb, initEvents, insertEvent, readJson, httpJson } from './db.mjs';
import { manifestFp, shortId } from './shared.mjs';

const log = (...a) => console.log('[lander]', ...a);
const BATCH_SIZE = 24;

export class LanderCore {
  constructor({ dbPath, receiverUrl, onFatal = null, crashMode = null }) {
    this.db = openDb(dbPath);
    initEvents(this.db);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY, opened_at INTEGER NOT NULL, committed_at INTEGER,
        acked_at INTEGER, item_count INTEGER NOT NULL, state TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS batch_items (
        batch_id TEXT NOT NULL, global INTEGER NOT NULL, fp TEXT NOT NULL,
        kind TEXT NOT NULL, PRIMARY KEY (batch_id, global)
      );
      CREATE TABLE IF NOT EXISTS landed (
        global INTEGER PRIMARY KEY, fp TEXT NOT NULL, frame_id INTEGER,
        origin TEXT, batch_id TEXT NOT NULL, landed_at INTEGER NOT NULL,
        acked INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0,
        superseded_fp TEXT, provisional INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS amendments (
        global INTEGER NOT NULL, from_fp TEXT NOT NULL, to_fp TEXT NOT NULL,
        at INTEGER NOT NULL, batch_id TEXT, reason TEXT,
        PRIMARY KEY (global, at)
      );
      CREATE TABLE IF NOT EXISTS control (k TEXT PRIMARY KEY, v TEXT NOT NULL);
    `);
    this.receiverUrl = receiverUrl;
    this.onFatal = onFatal;
    this.crashMode = crashMode;
    this.timer = null;
    this.stopping = false;
    this.paused = false;
    this.busy = false;
    this.lastError = null;
    this.batchesLanded = this.getControl('batchesLanded', 0);
    this.recovered = [];
    this.recoverOnBoot();
  }

  getControl(k, fallback) {
    const row = this.db.prepare('SELECT v FROM control WHERE k=?').get(k);
    return row ? JSON.parse(row.v) : fallback;
  }

  setControl(k, v) {
    this.db
      .prepare('INSERT INTO control (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v')
      .run(k, JSON.stringify(v));
  }

  // --------------------------------------------------------------- recovery

  /**
   * Runs before any landing work. Two crash signatures are visible on disk:
   *  - a batch left in 'staging'  => transaction never committed; roll back
   *  - a batch 'committed' but not acked => durable but unacknowledged
   */
  recoverOnBoot() {
    const staging = this.db.prepare("SELECT * FROM batches WHERE state='staging'").all();
    for (const b of staging) {
      this.db.prepare("UPDATE batches SET state='rolled-back' WHERE batch_id=?").run(b.batch_id);
      this.db.prepare('DELETE FROM batch_items WHERE batch_id=?').run(b.batch_id);
      this.recovered.push({ kind: 'rolled-back', batchId: b.batch_id, items: b.item_count });
      insertEvent(this.db, {
        actor: 'lander',
        type: 'recover.rollback',
        detail: { batchId: b.batch_id, items: b.item_count, why: 'staging-batch-never-committed' },
      });
    }
    const unacked = this.db.prepare("SELECT * FROM batches WHERE state='committed'").all();
    for (const b of unacked) {
      const items = this.db.prepare('SELECT * FROM batch_items WHERE batch_id=?').all(b.batch_id);
      const rows = new Map(
        this.db
          .prepare('SELECT global, fp, provisional FROM landed WHERE batch_id=?')
          .all(b.batch_id)
          .map((r) => [r.global, r])
      );
      let missing = 0;
      let divergent = 0;
      for (const item of items) {
        const row = rows.get(item.global);
        if (!row) missing++;
        else if (row.fp !== item.fp) divergent++;
      }
      const ok = missing === 0 && divergent === 0;
      if (ok) {
        this.db.prepare('UPDATE batches SET state=\'acked\', acked_at=? WHERE batch_id=?').run(Date.now(), b.batch_id);
        this.db.prepare('UPDATE landed SET acked=1 WHERE batch_id=?').run(b.batch_id);
      }
      this.recovered.push({
        kind: ok ? 'committed-unacked' : 'committed-corrupt',
        batchId: b.batch_id,
        items: b.item_count,
        missing,
        divergent,
      });
      insertEvent(this.db, {
        actor: 'lander',
        type: ok ? 'recover.ack' : 'recover.diverged',
        detail: {
          batchId: b.batch_id,
          items: b.item_count,
          verified: items.length,
          missing,
          divergent,
          why: 'commit-succeeded-ack-did-not',
        },
      });
    }
    if (this.recovered.length) {
      log('recovered', JSON.stringify(this.recovered));
      this.recovered.forEach((r) => insertEvent(this.db, { actor: 'lander', type: 'recover.summary', detail: r }));
    }
  }

  // ------------------------------------------------------------------ loop

  start() {
    this.loop();
  }

  async loop() {
    if (this.stopping) return;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.loop(), 450);
    if (this.paused || this.busy) return;
    this.busy = true;
    try {
      await this.pullOnce();
    } catch (err) {
      this.lastError = err.message;
    } finally {
      this.busy = false;
    }
  }

  async pullOnce() {
    const body = await httpJson(`${this.receiverUrl}/decisions`);
    const decisions = body.decisions || [];
    if (!decisions.length) return;
    const landed = new Map(
      this.db.prepare('SELECT global, fp, provisional, acked FROM landed').all().map((r) => [r.global, r])
    );
    const items = [];
    for (const d of decisions) {
      const row = landed.get(d.global);
      if (!row) {
        items.push({ global: d.global, fp: d.fp, frameId: d.frameId, origin: d.origin, provisional: d.provisional, kind: 'new' });
      } else if (row.fp !== d.fp) {
        items.push({
          global: d.global,
          fp: d.fp,
          frameId: d.frameId,
          origin: d.origin,
          provisional: d.provisional,
          kind: 'amend',
          fromFp: row.fp,
        });
      } else if (!!row.provisional !== !!d.provisional) {
        items.push({
          global: d.global,
          fp: d.fp,
          frameId: d.frameId,
          origin: d.origin,
          provisional: d.provisional,
          kind: 'settle',
          fromFp: row.fp,
        });
      }
    }
    if (!items.length) return;
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const chunk = items.slice(i, i + BATCH_SIZE);
      this.landBatch(chunk);
      if (this.stopping) return;
    }
  }

  landBatch(items) {
    const batchId = `B-${Date.now().toString(36)}-${shortId('x').split('-')[1]}`;
    const openedAt = Date.now();
    this.db
      .prepare(
        `INSERT INTO batches (batch_id, opened_at, item_count, state) VALUES (?,?,?,'staging')`
      )
      .run(batchId, openedAt, items.length);
    for (const it of items) {
      this.db
        .prepare('INSERT INTO batch_items (batch_id, global, fp, kind) VALUES (?,?,?,?)')
        .run(batchId, it.global, it.fp, it.kind);
    }
    const fatal = (mode) => {
      if (this.crashMode === mode && this.onFatal) {
        this.onFatal(mode, batchId);
      }
    };

    this.db.exec('BEGIN IMMEDIATE');
    try {
      let done = 0;
      for (const it of items) {
        this.writeRow(batchId, it);
        done++;
        // crash inside the transaction: rows must not survive a restart
        if (this.crashMode === 'mid' && done === Math.ceil(items.length / 2)) {
          // leave the transaction open and die
          this.onFatal?.( 'mid', batchId);
        }
      }
      this.db
        .prepare(`UPDATE batches SET state='committed', committed_at=? WHERE batch_id=?`)
        .run(Date.now(), batchId);
      this.db.exec('COMMIT');
    } catch (err) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        /* ignore */
      }
      this.db.prepare("UPDATE batches SET state='rolled-back' WHERE batch_id=?").run(batchId);
      throw err;
    }
    this.batchesLanded++;
    this.setControl('batchesLanded', this.batchesLanded);
    insertEvent(this.db, {
      actor: 'lander',
      type: 'batch.commit',
      detail: {
        batchId,
        items: items.length,
        kinds: countBy(items, 'kind'),
        globals: [items[0].global, items[items.length - 1].global],
      },
    });
    // crash after COMMIT, before ACK: the durable-but-unacknowledged window
    if (this.crashMode === 'after-commit') {
      this.onFatal?.('after-commit', batchId);
      this.crashMode = null;
      return;
    }
    this.ackBatch(batchId, items.length);
  }

  writeRow(batchId, it) {
    const at = Date.now();
    const existing = this.db.prepare('SELECT * FROM landed WHERE global=?').get(it.global);
    if (!existing) {
      this.db
        .prepare(
          `INSERT INTO landed (global, fp, frame_id, origin, batch_id, landed_at, acked, provisional)
           VALUES (?,?,?,?,?,?,0,?)`
        )
        .run(it.global, it.fp, it.frameId, it.origin, batchId, at, it.provisional ? 1 : 0);
      return;
    }
    if (existing.fp !== it.fp) {
      this.db
        .prepare(
          `UPDATE landed SET fp=?, frame_id=?, origin=?, batch_id=?, landed_at=?, acked=0,
             revision=revision+1, superseded_fp=?, provisional=?
           WHERE global=?`
        )
        .run(it.fp, it.frameId, it.origin, batchId, at, existing.fp, it.provisional ? 1 : 0, it.global);
      this.db
        .prepare('INSERT OR REPLACE INTO amendments (global, from_fp, to_fp, at, batch_id, reason) VALUES (?,?,?,?,?,?)')
        .run(it.global, existing.fp, it.fp, at, batchId, it.kind);
      insertEvent(this.db, {
        actor: 'lander',
        type: 'land.amend',
        global: it.global,
        detail: { from: existing.fp, to: it.fp, revision: existing.revision + 1, kind: it.kind },
      });
    } else {
      this.db
        .prepare('UPDATE landed SET batch_id=?, landed_at=?, acked=0, provisional=? WHERE global=?')
        .run(batchId, at, it.provisional ? 1 : 0, it.global);
    }
  }

  ackBatch(batchId, count) {
    this.db
      .prepare(`UPDATE batches SET state='acked', acked_at=? WHERE batch_id=?`)
      .run(Date.now(), batchId);
    this.db.prepare('UPDATE landed SET acked=1 WHERE batch_id=?').run(batchId);
    const acked = this.ackedCursor();
    this.setControl('ackedCursor', acked);
    insertEvent(this.db, {
      actor: 'lander',
      type: 'batch.ack',
      detail: { batchId, items: count, ackedCursor: acked },
    });
  }

  // --------------------------------------------------------------- progress

  /**
   * Landing progress = largest N such that every global 1..N has an acked row.
   * Deliberately independent of the receiver's cursor: the receiver can be far
   * ahead (or behind) and this number only moves when bytes are on disk.
   */
  ackedCursor() {
    const has = this.db.prepare('SELECT 1 AS x FROM landed WHERE global=? AND acked=1');
    let n = 0;
    while (has.get(n + 1)) n++;
    return n;
  }

  stats() {
    const count = this.db.prepare('SELECT COUNT(*) AS n FROM landed').get().n;
    const acked = this.db.prepare('SELECT COUNT(*) AS n FROM landed WHERE acked=1').get().n;
    const provisional = this.db.prepare('SELECT COUNT(*) AS n FROM landed WHERE provisional=1').get().n;
    const revisions = this.db.prepare('SELECT COALESCE(SUM(revision),0) AS n FROM landed').get().n;
    const batches = this.db.prepare("SELECT COUNT(*) AS n FROM batches WHERE state='acked'").get().n;
    const committed = this.db.prepare("SELECT COUNT(*) AS n FROM batches WHERE state='committed'").get().n;
    const staged = this.db.prepare("SELECT COUNT(*) AS n FROM batches WHERE state='staging'").get().n;
    return { count, acked, provisional, revisions, batches, committed, staged, ackedCursor: this.ackedCursor(), maxLanded: this.db.prepare('SELECT MAX(global) AS m FROM landed').get().m || 0 };
  }

  // ------------------------------------------------------------- reconciliation

  /**
   * Compare what is on disk with the supplier's full manifest, per global, by
   * content fingerprint. Nothing here trusts the receiver's counters.
   */
  reconcile(manifest) {
    const rows = this.db.prepare('SELECT global, fp, provisional, acked FROM landed').all();
    const byGlobal = new Map(rows.map((r) => [r.global, r]));
    const mismatch = [];
    for (const m of manifest.messages) {
      const row = byGlobal.get(m.global);
      if (!row) mismatch.push({ global: m.global, kind: 'missing', expected: m.fp, actual: null });
      else if (row.fp !== m.fp) mismatch.push({ global: m.global, kind: 'content', expected: m.fp, actual: row.fp });
      else if (!row.acked) mismatch.push({ global: m.global, kind: 'unacked', expected: m.fp, actual: row.fp });
      else if (row.provisional) mismatch.push({ global: m.global, kind: 'provisional', expected: m.fp, actual: row.fp });
    }
    for (const r of rows) {
      if (r.global > manifest.total) mismatch.push({ global: r.global, kind: 'extra', expected: null, actual: r.fp });
    }
    const ordered = manifest.messages.map((m) => byGlobal.get(m.global)).filter(Boolean);
    // Recompute the whole-stream fingerprint from what is actually on disk,
    // using the same function the supplier used for its manifest.
    const hash = manifestFp(ordered);
    const stats = this.stats();
    return {
      total: manifest.total,
      landed: stats.count,
      acked: stats.acked,
      provisional: stats.provisional,
      revisions: stats.revisions,
      cursor: stats.ackedCursor,
      manifestFp: manifest.totalFp,
      landedFp: hash,
      fingerprintMatch: hash === manifest.totalFp,
      mismatchCount: mismatch.length,
      mismatch: mismatch.slice(0, 200),
      complete:
        mismatch.length === 0 &&
        stats.acked === manifest.total &&
        stats.ackedCursor === manifest.total &&
        stats.provisional === 0,
    };
  }

  state() {
    const recentBatches = this.db
      .prepare('SELECT * FROM batches ORDER BY opened_at DESC LIMIT 12')
      .all();
    const amendments = this.db
      .prepare('SELECT * FROM amendments ORDER BY at DESC LIMIT 20')
      .all();
    const events = this.db
      .prepare('SELECT * FROM events ORDER BY seq DESC LIMIT 60')
      .all()
      .reverse()
      .map((e) => ({ ...e, detail: safeJson(e.detail, null) }));
    return {
      ok: true,
      paused: this.paused,
      stats: this.stats(),
      batches: recentBatches,
      amendments,
      recovered: this.recovered,
      events,
      lastError: this.lastError,
    };
  }

  landGlobal(global) {
    const row = this.db.prepare('SELECT * FROM landed WHERE global=?').get(global);
    if (!row) return null;
    const batch = this.db.prepare('SELECT * FROM batches WHERE batch_id=?').get(row.batch_id);
    const amendments = this.db.prepare('SELECT * FROM amendments WHERE global=? ORDER BY at').all(global);
    const events = this.db
      .prepare('SELECT * FROM events WHERE global=? ORDER BY seq')
      .all(global)
      .map((e) => ({ ...e, detail: safeJson(e.detail, null) }));
    return { ...row, batch, amendments, events };
  }

  /**
   * Compact per-global landing picture for the page's landing band and for the
   * trace. Deliberately separate from the receiver's decisions: this is what is
   * actually on disk, not what was last accepted in memory.
   */
  globals() {
    return this.db
      .prepare(
        'SELECT global, fp, acked, provisional, revision, batch_id, landed_at FROM landed ORDER BY global'
      )
      .all();
  }

  setPaused(paused) {
    this.paused = !!paused;
  }

  /** Page-triggered crash: applies to the next batch. */
  armCrash(mode) {
    if (!['mid', 'after-commit'].includes(mode)) throw new Error('bad crash mode');
    this.crashMode = mode;
    insertEvent(this.db, { actor: 'desk', type: 'lander.crash-armed', detail: { mode } });
    return { armed: mode };
  }

  stop() {
    this.stopping = true;
    clearTimeout(this.timer);
  }
}

export function sha1(...parts) {
  // local import to keep the module's dependency surface small
  return createHash('sha1').update(parts.join('\u0000')).digest('hex');
}

function countBy(arr, key) {  const out = {};
  for (const a of arr) out[a[key]] = (out[a[key]] || 0) + 1;
  return out;
}

function safeJson(v, fallback) {
  if (v == null) return fallback;
  if (typeof v !== 'string') return v;
  try {
    return JSON.parse(v);
  } catch {
    return fallback;
  }
}

export async function startLander({ port = 0, dbPath, receiverUrl }) {
  const core = new LanderCore({
    dbPath,
    receiverUrl,
    onFatal: (mode, batchId) => {
      log(`crash injected (${mode}) during ${batchId}; exiting`);
      process.exit(mode === 'mid' ? 6 : 7);
    },
  });
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://local');
    const reply = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    try {
      if (req.method === 'GET' && url.pathname === '/health') return reply(200, { ok: true, port: server.address().port });
      if (req.method === 'GET' && url.pathname === '/state') return reply(200, core.state());
      if (req.method === 'GET' && url.pathname === '/globals') return reply(200, { globals: core.globals() });
      if (req.method === 'GET' && url.pathname.startsWith('/global/')) {
        const g = Number(url.pathname.split('/').pop());
        return reply(200, { global: g, land: core.landGlobal(g) });
      }
      if (req.method === 'POST') {
        const body = await readJson(req);
        if (url.pathname === '/start') {
          core.start();
          return reply(200, { ok: true });
        }
        if (url.pathname === '/pause') {
          core.setPaused(body.paused);
          return reply(200, { ok: true, paused: core.paused });
        }
        if (url.pathname === '/crash') {
          return reply(200, core.armCrash(body.mode || 'after-commit'));
        }
        if (url.pathname === '/reconcile') {
          return reply(200, core.reconcile(body.manifest));
        }
        if (url.pathname === '/quit') {
          core.stop();
          reply(200, { ok: true });
          setTimeout(() => process.exit(0), 40);
          return;
        }
      }
      reply(404, { error: 'not-found' });
    } catch (err) {
      log('http error', err.stack || err.message);
      reply(500, { error: String(err.message || err) });
    }
  });
  await new Promise((r) => server.listen(port, '127.0.0.1', r));
  const actual = server.address().port;
  return {
    core,
    port: actual,
    close: () =>
      new Promise((r) => {
        core.stop();
        server.close(() => r());
      }),
  };
}

if (process.argv[1] && /lander\.mjs$/.test(process.argv[1])) {
  const dataDir = process.env.DATA_DIR || './data';
  const { port } = await startLander({
    port: Number(process.env.PORT || 0),
    dbPath: `${dataDir}/lander.db`,
    receiverUrl: process.env.RECEIVER_URL || 'http://127.0.0.1:9101',
  });
  if (process.send) process.send({ type: 'ready', port });
  log('listening on', port);
}
