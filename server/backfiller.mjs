// Gap backfiller process. Watches the receiver's gap set, opens numbered replay
// sessions against the supplier, and feeds the returned frames back through the
// receiver's normal accept path (so epoch/basis rules still apply).
//
// Failure handling mirrors the supplier's reality: a replay session can abort
// without saying "complete"; it can reset its released numbering mid-session
// (declaring a new basis); and a range may stay incomplete after a session ends.
// Those are three distinct outcomes with three distinct retry rules.
import http from 'node:http';
import { openDb, initEvents, insertEvent, tx, readJson, httpJson, postJson } from './db.mjs';
import { WSClient } from './ws-client.mjs';
import { connStateLabel, shortId } from './shared.mjs';

const log = (...a) => console.log('[backfiller]', ...a);
const MAX_ATTEMPT = 3;
const MAX_SPAN = 48;

export class Backfiller {
  constructor({ dbPath, receiverUrl, sourceUrl }) {
    this.db = openDb(dbPath);
    initEvents(this.db);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY, from_global INTEGER NOT NULL, to_global INTEGER NOT NULL,
        attempt INTEGER NOT NULL, purpose TEXT NOT NULL DEFAULT 'gap', state TEXT NOT NULL, conn_id TEXT,
        requested_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER,
        frames INTEGER DEFAULT 0, note TEXT
      );
      CREATE TABLE IF NOT EXISTS control (k TEXT PRIMARY KEY, v TEXT NOT NULL);
    `);
    this.receiverUrl = receiverUrl;
    this.sourceUrl = sourceUrl;
    this.active = null;
    this.queue = [];
    this.paused = false;
    this.stopping = false;
    this.timer = null;
    this.serial = 0;
    this.failures = this.getControl('failures', 0);
    this.done = this.getControl('done', 0);
    this.getControl = (k, fallback) => {
      const row = this.db.prepare('SELECT v FROM control WHERE k=?').get(k);
      return row ? JSON.parse(row.v) : fallback;
    };
    this.verified = new Set(this.getControl('verified', []));
    this.lastError = null;
  }

  setControl(k, v) {
    this.db
      .prepare('INSERT INTO control (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v')
      .run(k, JSON.stringify(v));
  }

  getControl(k, fallback) {
    const row = this.db.prepare('SELECT v FROM control WHERE k=?').get(k);
    return row ? JSON.parse(row.v) : fallback;
  }

  start() {
    this.tick();
  }

  setPaused(paused) {
    this.paused = !!paused;
    if (!this.paused) this.tick();
  }

  async tick() {
    if (this.stopping) return;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.tick(), this.active ? 250 : 420);
    if (this.paused) return;
    try {
      const st = await httpJson(`${this.receiverUrl}/state`);
      const total = st.total || null;
      if (!total) return;
      // Two kinds of work, same mechanism:
      //  * gap  : nothing covers the global at all
      //  * verify: a sealed stale tail covers it; only a numbered replay can firm it
      const gapRanges = groupGaps(st.gaps).map((r) => ({ ...r, purpose: 'gap' }));
      const tentativeGlobals = (st.tentative || []).filter((t) => !this.verified.has(t.global));
      const verifyRanges = groupGaps(tentativeGlobals).map((r) => ({ ...r, purpose: 'verify' }));
      const ranges = [...gapRanges, ...verifyRanges];
      const pending = ranges.filter((r) => !this.coveredBy(r, st));
      const next = pending.find(
        (r) => !this.inFlight(r) && this.attemptsFor(r) < MAX_ATTEMPT && !this.permanentFail(r)
      );
      if (!this.active && next) await this.openSession(next);
    } catch (err) {
      this.lastError = err.message;
    }
  }

  coveredBy(range, st) {
    // Returns true when the range no longer needs this session's work.
    if (range.purpose === 'verify') {
      const tent = new Set((st.tentative || []).map((t) => t.global));
      for (let g = range.from; g <= range.to; g++) if (tent.has(g)) return false;
      return true;
    }
    const gapSet = new Set(st.gaps.map((g) => g.global));
    for (let g = range.from; g <= range.to; g++) if (gapSet.has(g)) return false;
    return true;
  }

  inFlight(range) {
    if (!this.active) return false;
    return this.active.from <= range.to && this.active.to >= range.from;
  }

  permanentFail(range) {
    const row = this.db
      .prepare(
        `SELECT * FROM sessions WHERE from_global<=? AND to_global>=? AND purpose=? AND state='abandoned'
         ORDER BY requested_at DESC LIMIT 1`
      )
      .get(range.from, range.to, range.purpose || 'gap');
    return !!row;
  }

  attemptsFor(range) {
    // Attempts are counted along the chain of sessions that overlapped this
    // range, not by exact bounds: a partial session leaves a smaller hole, and
    // that retry is still attempt 2 of the same repair.
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS n FROM sessions
         WHERE purpose=? AND from_global<=? AND to_global>=? AND state<>'requested'`
      )
      .get(range.purpose || 'gap', range.to, range.from);
    return row?.n || 0;
  }

  async openSession(range) {
    this.sessionSerial = (this.sessionSerial || 0) + 1;
    const attempt = this.attemptsFor(range) + 1;
    const sessionId = `RS-${++this.serial}-${shortId('s').split('-')[1]}`;
    this.db
      .prepare(
        `INSERT INTO sessions (session_id, from_global, to_global, attempt, purpose, state, requested_at)
         VALUES (?,?,?,?,?,'requested',?)`
      )
      .run(sessionId, range.from, range.to, attempt, range.purpose || 'gap', Date.now());
    this.active = {
      sessionId,
      from: range.from,
      to: range.to,
      purpose: range.purpose || 'gap',
      attempt,
      connId: `R${this.serial + 100}`,
      state: 'requested',
      frames: 0,
      ws: null,
      sawComplete: false,
      basis: null,
      rebases: 0,
      closed: false,
      quit: false,
    };
    const s = this.active;
    this.event({
      type: 'session.open',
      detail: {
        sessionId,
        from: s.from,
        to: s.to,
        attempt,
        connId: s.connId,
        purpose: s.purpose,
        why: attempt > 1 ? 'retry-after-failed-attempt' : s.purpose,
      },
    });
    const ws = new WSClient(
      `${this.sourceUrl}/feed?role=replay&from=${s.from}&to=${s.to}&attempt=${attempt}&purpose=${s.purpose}&sessionId=${sessionId}`
    );
    s.ws = ws;
    ws.on('message', (raw) => this.onMessage(s, raw));
    ws.on('close', () => this.onClose(s));
    try {
      await ws.connect();
      s.state = 'active';
      this.db
        .prepare(`UPDATE sessions SET state='active', conn_id=?, started_at=? WHERE session_id=?`)
        .run(s.connId, Date.now(), sessionId);
    } catch (err) {
      this.lastError = err.message;
      this.finish(s, 'failed', 'connect-failed');
    }
  }

  onMessage(s, raw) {
    let msg;
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return;
    }
    // Control messages that change identity (hello, rebase) are enqueued on the
    // same chain as frames. A rebase therefore always reaches the receiver
    // *before* any frame numbered under the new basis.
    if (msg.type === 'hello' || msg.type === 'rebase') {
      this.enqueue(s, () => this.onControl(s, msg));
      return;
    }
    if (msg.type === 'quote') {
      s.frames++;
      this.db.prepare('UPDATE sessions SET frames=frames+1 WHERE session_id=?').run(s.sessionId);
      this.enqueue(s, async () => {
        const verdict = await postJson(`${this.receiverUrl}/frames`, {
          connId: s.connId,
          epoch: msg.epoch,
          release: msg.release,
          quote: msg.quote,
          fp: msg.fp,
          origin: 'replay',
        });
        if (!verdict.ok) {
          this.event({
            type: 'session.frame-rejected',
            connId: s.connId,
            global: verdict.global ?? null,
            detail: { sessionId: s.sessionId, reason: verdict.reason, release: msg.release, epoch: msg.epoch },
          });
        } else if (verdict.contested) {
          this.event({
            type: 'session.frame-contested',
            connId: s.connId,
            global: verdict.global,
            detail: { sessionId: s.sessionId, groupId: verdict.groupId },
          });
        }
      });
      return;
    }
    if (msg.type === 'status') {
      s.lastEvent = msg.state;
      if (msg.state === 'complete') {
        s.sawComplete = true;
        // The complete signal is ordered after every quote of this session,
        // otherwise finish() could verify coverage before the last frame lands.
        this.enqueue(s, async () => {
          this.event({
            type: 'session.complete-signal',
            connId: s.connId,
            detail: { sessionId: s.sessionId, reason: msg.reason },
          });
        });
      } else if (msg.state === 'aborted') {
        s.sawComplete = false;
        this.enqueue(s, async () => {
          this.event({
            type: 'session.aborted',
            connId: s.connId,
            detail: { sessionId: s.sessionId, reason: msg.reason },
          });
        });
      }
    }
  }

  /**
   * Strict FIFO per session. Every receiver call for a session goes through
   * this chain, so register/rebase/quotes/complete can never overtake each
   * other on the wire.
   */
  enqueue(s, task) {
    const run = async () => {
      try {
        await task();
      } catch (err) {
        this.lastError = err.message;
        log('session task failed', err.message);
      }
    };
    s.chain = (s.chain || Promise.resolve()).then(run, run);
    return s.chain;
  }

  onControl(s, msg) {
    if (msg.type === 'hello') {
      s.basis = msg.basis;
      s.helloSeen = true;
      this.event({
        type: 'session.hello',
        connId: s.connId,
        detail: {
          sessionId: s.sessionId,
          epoch: msg.basis.epoch,
          releaseStart: msg.basis.releaseStart,
          globalStart: msg.basis.globalStart,
        },
      });
      return postJson(`${this.receiverUrl}/register`, {
        connId: s.connId,
        role: 'repair',
        basis: msg.basis,
        note: `session ${s.sessionId} attempt ${s.attempt}`,
      });
    }
    if (msg.type === 'rebase') {
      s.basis = msg.basis;
      s.rebases++;
      this.event({
        type: 'session.rebase',
        connId: s.connId,
        detail: {
          sessionId: s.sessionId,
          epoch: msg.basis.epoch,
          globalStart: msg.basis.globalStart,
          rebases: s.rebases,
        },
      });
      return postJson(`${this.receiverUrl}/register`, {
        connId: s.connId,
        role: 'repair',
        basis: msg.basis,
        note: `rebased in ${s.sessionId}`,
      });
    }
  }

  onClose(s) {
    if (s.closed) return;
    s.closed = true;
    // Wait for the whole forwarding chain before judging the session: a frame
    // still in flight must be landed in the receiver before we verify coverage.
    setTimeout(async () => {
      await s.chain;
      this.finish(s, s.sawComplete ? 'complete-claimed' : 'aborted', s.sawComplete ? 'session-end' : 'no-complete');
    }, 60);
  }

  async finish(s, state, note) {
    if (this.active !== s) return;
    this.active = null;
    let verdict = state;
    // A supplier "complete" is a claim. Only the receiver's coverage decides.
    let missing = [];
    try {
      const st = await httpJson(`${this.receiverUrl}/state`);
      const gapSet = new Set(st.gaps.map((g) => g.global));
      const tentativeSet = new Set((st.tentative || []).map((t) => t.global));
      for (let g = s.from; g <= s.to; g++) {
        if (gapSet.has(g)) missing.push({ global: g, kind: 'gap' });
        else if (s.purpose === 'verify' && tentativeSet.has(g)) missing.push({ global: g, kind: 'tentative' });
      }
    } catch (err) {
      this.lastError = err.message;
      this.active = s; // retry same session bookkeeping next tick
      this.active = null;
      verdict = 'unverified';
      note = `could not verify: ${err.message}`;
    }
    if (state === 'complete-claimed') {
      if (verdict === 'complete-claimed' && missing.length === 0) verdict = 'complete';
      else if (verdict === 'complete-claimed') {
        verdict = 'incomplete';
        note = `supplier said complete, receiver still lacks ${missing.length} global(s)`;
      }
    }
    const complete = verdict === 'complete';
    if (complete && s.purpose === 'verify') {
      for (let g = s.from; g <= s.to; g++) this.verified.add(g);
      this.setControl('verified', [...this.verified]);
    }
    this.db
      .prepare('UPDATE sessions SET state=?, finished_at=?, note=? WHERE session_id=?')
      .run(verdict, Date.now(), note ?? null, s.sessionId);
    const abandoned = !complete && s.attempt >= MAX_ATTEMPT;
    if (abandoned) {
      this.db
        .prepare("UPDATE sessions SET state='abandoned', note=? WHERE session_id=?")
        .run(`${note ?? ''}; attempts exhausted`, s.sessionId);
      this.failures++;
      this.setControl('failures', this.failures);
    }
    this.event({
      type: 'session.finish',
      connId: s.connId,
      detail: {
        sessionId: s.sessionId,
        from: s.from,
        to: s.to,
        attempt: s.attempt,
        purpose: s.purpose,
        verdict,
        abandoned,
        frames: s.frames,
        rebases: s.rebases,
        missing: missing.length,
        note,
      },
    });
    // A replay archive is the highest authority. Ask the receiver to settle any
    // conflict group this session disproved, and any it settled by rule.
    if (s.frames > 0) {
      try {
        await postJson(`${this.receiverUrl}/resolve`, { by: 'backfiller' });
      } catch (err) {
        log('resolve failed', err.message);
      }
    }
    if (s.ws) {
      try {
        s.ws.close();
      } catch {
        /* ignore */
      }
    }
    setTimeout(() => this.tick(), complete ? 120 : 400 * s.attempt);
  }

  event({ type, connId = null, global = null, detail = null }) {
    insertEvent(this.db, { actor: 'backfiller', type, global, connId, detail });
    postJson(`${this.receiverUrl}/event`, {
      actor: 'backfiller',
      type,
      global,
      connId,
      detail,
    }).catch(() => {});
  }

  state() {
    const sessions = this.db
      .prepare('SELECT * FROM sessions ORDER BY requested_at DESC LIMIT 40')
      .all()
      .map((s) => ({ ...s, label: connStateLabel(s.state) }));
    const active = this.active
      ? {
          sessionId: this.active.sessionId,
          from: this.active.from,
          to: this.active.to,
          attempt: this.active.attempt,
          connId: this.active.connId,
          frames: this.active.frames,
          rebases: this.active.rebases,
          state: this.active.sawComplete ? 'finalizing' : 'streaming',
        }
      : null;
    return {
      ok: true,
      paused: this.paused,
      active,
      sessions,
      done: this.done,
      failures: this.failures,
      lastError: this.lastError,
    };
  }

  stop() {
    this.stopping = true;
    clearTimeout(this.timer);
    if (this.active?.ws) {
      try {
        this.active.ws.close();
      } catch {
        /* ignore */
      }
    }
  }
}

/**
 * Gaps arrive as individual globals. Merge them into ranges so one replay
 * session can carry a whole run of holes; split huge ranges to keep a session
 * short enough to inspect.
 */
export function groupGaps(gaps) {
  const nums = [...new Set(gaps.map((g) => g.global))].sort((a, b) => a - b);
  const ranges = [];
  let cur = null;
  for (const n of nums) {
    if (!cur) cur = { from: n, to: n };
    else if (n === cur.to + 1 && n - cur.from < MAX_SPAN) cur.to = n;
    else {
      ranges.push(cur);
      cur = { from: n, to: n };
    }
  }
  if (cur) ranges.push(cur);
  return ranges;
}

export async function startBackfiller({ port = 0, dbPath, receiverUrl, sourceUrl }) {
  const engine = new Backfiller({ dbPath, receiverUrl, sourceUrl });
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://local');
    const reply = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    try {
      if (req.method === 'GET' && url.pathname === '/health') return reply(200, { ok: true, port: server.address().port });
      if (req.method === 'GET' && url.pathname === '/state') return reply(200, engine.state());
      if (req.method === 'POST') {
        const body = await readJson(req);
        if (url.pathname === '/start') {
          engine.start();
          return reply(200, { ok: true });
        }
        if (url.pathname === '/pause') {
          engine.setPaused(body.paused);
          return reply(200, { ok: true, paused: engine.paused });
        }
        if (url.pathname === '/quit') {
          engine.stop();
          reply(200, { ok: true });
          setTimeout(() => process.exit(0), 50);
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
    engine,
    port: actual,
    close: () =>
      new Promise((r) => {
        engine.stop();
        server.close(() => r());
      }),
  };
}

if (process.argv[1] && /backfiller\.mjs$/.test(process.argv[1])) {
  const dataDir = process.env.DATA_DIR || './data';
  const { port } = await startBackfiller({
    port: Number(process.env.PORT || 0),
    dbPath: `${dataDir}/backfill.db`,
    receiverUrl: process.env.RECEIVER_URL || 'http://127.0.0.1:9101',
    sourceUrl: process.env.SOURCE_URL || 'http://127.0.0.1:8899',
  });
  if (process.send) process.send({ type: 'ready', port });
  log('listening on', port);
}
