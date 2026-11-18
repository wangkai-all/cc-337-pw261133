// Receiver process — owns connection identity, frame quarantine, the global
// order, per-global coverage and the conflict ledger.
//
// Invariants:
//  1. A notice/status only ever applies to the connId that emitted it. The old
//     line closing late cannot retire the replacement line.
//  2. A released number maps to a global number only through the basis the
//     supplier declared for that socket+epoch. No basis => quarantine, never a
//     guessed position.
//  3. Coverage is per global number. "Received completely" means the cursor is
//     contiguous from 1..total; a duplicate or a replay frame cannot inflate it.
//  4. A conflict is never settled by arrival order. Variants are sealed, then a
//     recorded decision picks a winner by source class (replay > live > tail).
import http from 'node:http';
import { openDb, initEvents, insertEvent, tx, readJson } from './db.mjs';
import { WSClient } from './ws-client.mjs';
import { contentFp, connStateLabel, deriveGlobal } from './shared.mjs';

const log = (...a) => console.log('[receiver]', ...a);
const ARBITRATION_WINDOW_MS = 1400;

/** Source class authority. Archive > live > stalled-tail. */
export function classRank(frame) {
  return frame.origin === 'replay' ? 3 : frame.origin === 'live' ? 2 : 1;
}

export class ReceiverCore {
  constructor({ dbPath }) {
    this.db = openDb(dbPath);
    initEvents(this.db);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS connections (
        conn_id TEXT PRIMARY KEY, role TEXT NOT NULL, epoch INTEGER,
        basis_key TEXT, basis_json TEXT, state TEXT NOT NULL,
        opened_at INTEGER NOT NULL, closed_at INTEGER, note TEXT,
        global_first INTEGER, global_last INTEGER, frames_accepted INTEGER DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS frames (
        frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
        conn_id TEXT NOT NULL, epoch INTEGER, release INTEGER, global INTEGER,
        symbol TEXT, fp TEXT, payload TEXT, origin TEXT NOT NULL,
        decision TEXT NOT NULL, conflict_group TEXT, at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS frames_global ON frames(global);
      CREATE TABLE IF NOT EXISTS coverage (
        global INTEGER PRIMARY KEY, fp TEXT NOT NULL, frame_id INTEGER NOT NULL,
        at INTEGER NOT NULL, grade TEXT NOT NULL DEFAULT 'firm'
      );
      CREATE TABLE IF NOT EXISTS gaps (
        global INTEGER PRIMARY KEY, opened_at INTEGER NOT NULL, filled_at INTEGER, note TEXT
      );
      CREATE TABLE IF NOT EXISTS conflicts (
        group_id TEXT PRIMARY KEY, global INTEGER NOT NULL, symbol TEXT,
        decision TEXT NOT NULL, winner_fp TEXT, winner_frame INTEGER,
        decided_by TEXT, decided_at INTEGER, note TEXT, opened_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS control (k TEXT PRIMARY KEY, v TEXT NOT NULL);
    `);
    this.lines = [];
    this.current = null;
    this.total = null;
    this.cursor = Number(this.getControl('cursor', 0));
    this.paused = this.getControl('paused', false);
    this.policy = this.getControl('policy', 'overlap');
    this.attempt = 0;
    this.stopping = false;
    this.reconnectTimer = null;
    this.sourceUrl = null;
    this.autoRun = true;
    this.quarantine = 0;
    this.reconnects = 0; // identity of live lines opened this run
    this.lastCloseAt = 0;
  }

  // --------------------------------------------------------------- control

  getControl(k, fallback) {
    const row = this.db.prepare('SELECT v FROM control WHERE k=?').get(k);
    return row ? JSON.parse(row.v) : fallback;
  }

  setControl(k, v) {
    this.db
      .prepare('INSERT INTO control (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v')
      .run(k, JSON.stringify(v));
  }

  // ------------------------------------------------------------- connection

  start({ sourceUrl, total = null }) {
    this.sourceUrl = sourceUrl;
    if (total) this.total = Number(total);
    this.openLiveLine();
    this.settleTimer = setInterval(() => this.settleTick(), 400);
  }

  /**
   * Arbitration window. A conflict whose variants already contain an
   * unambiguously higher source class is decided after a short, visible pause
   * (so the page can show the pending state) unless the desk pauses the run.
   * Genuinely ambiguous groups stay pending until a person decides.
   */
  settleTick() {
    if (this.paused || this.stopping) return;
    const pending = this.pendingConflicts();
    for (const g of pending) {
      const note = safeJson(g.note, { variants: [] });
      const variants = note.variants
        .map((id) => this.db.prepare('SELECT * FROM frames WHERE frame_id=?').get(id))
        .filter(Boolean);
      if (variants.length < 2) continue;
      const best = Math.max(...variants.map((v) => classRank(v)));
      const bestVariants = variants.filter((v) => classRank(v) === best);
      const bestFps = new Set(bestVariants.map((v) => v.fp));
      // unambiguous = one source class strictly above the others, one content
      const lower = variants.filter((v) => classRank(v) < best);
      if (lower.length === 0 || bestFps.size !== 1) continue;
      if (Date.now() - g.opened_at < ARBITRATION_WINDOW_MS) continue;
      this.resolveConflict(g.group_id, 'rule');
    }
  }

  openLiveLine() {
    if (this.stopping) return;
    if (this.paused) return;
    const connId = `L${String(++this.reconnects).padStart(2, '0')}`;
    // Ask the supplier to resume at the first global we still lack. The supplier
    // only honours that for the initial subscription; later lines are live-only,
    // which is why the tail and numbered replay both matter.
    const start = this.cursor + 1;
    const line = {
      connId,
      start,
      ws: new WSClient(`${this.sourceUrl}/feed?role=live&connId=${encodeURIComponent(connId)}`),
      basis: null,
      state: 'connecting',
      closed: false,
      isFirst: this.reconnects === 1,
      supersededBy: null,
    };
    this.lines.push(line);
    this.current = line;
    this.db
      .prepare(
        `INSERT INTO connections (conn_id, role, state, opened_at, note) VALUES (?, 'live', 'connecting', ?, ?)`
      )
      .run(connId, Date.now(), `start=${start}`);
    insertEvent(this.db, {
      actor: 'receiver',
      type: 'line.connect',
      connId,
      detail: { start, attempt: this.reconnects, policy: this.policy },
    });
    line.ws.on('message', (raw) => this.onMessage(line, raw));
    line.ws.on('close', () => this.onClose(line));
    line.ws
      .connect()
      .then(() => {
        if (line.closed) return;
        line.ws.send(JSON.stringify({ type: 'attach', start, globalStart: start }));
      })
      .catch((err) => {
        log('live connect failed:', err.message);
        line.closed = true;
        this.setState(connId, 'closed', 'connect-failed');
        if (line === this.current) this.scheduleReconnect(400);
      });
  }

  scheduleReconnect(delay = 300) {
    if (this.stopping || this.paused || !this.autoRun) return;
    if (this.reconnectTimer) return;
    const since = Date.now() - this.lastCloseAt;
    const wait = Math.max(delay, since < 500 ? 600 - since : 0);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.stopping && !this.paused && this.autoRun) this.openLiveLine();
    }, Math.max(50, wait));
  }

  onMessage(line, raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (msg.connId && msg.connId !== line.connId) {
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'notice.mismatch',
        connId: line.connId,
        detail: { claimed: msg.connId, type: msg.type },
      });
      return;
    }
    if (msg.type === 'hello') {
      if (line.basis) return;
      line.basis = msg.basis;
      line.state = 'live';
      if (msg.total) this.total = Number(msg.total);
      this.db
        .prepare(`UPDATE connections SET epoch=?, basis_key=?, basis_json=?, state='live' WHERE conn_id=?`)
        .run(msg.basis.epoch, msg.basis.key, JSON.stringify(msg.basis), line.connId);
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'line.hello',
        connId: line.connId,
        detail: { epoch: msg.basis.epoch, releaseStart: msg.basis.releaseStart, globalStart: msg.basis.globalStart },
      });
      // A socket that starts above our cursor cannot deliver the numbers in
      // between; say so now instead of leaving a silent hole until the end.
      this.markMissing(msg.basis.globalStart - 1);
      return;
    }
    if (msg.type === 'quote') {
      const verdict = this.ingest({
        connId: line.connId,
        epoch: msg.epoch,
        release: msg.release,
        quote: msg.quote,
        fp: msg.fp,
        tail: !!msg.tail,
        origin: msg.tail ? 'tail' : 'live',
      });
      if (!verdict.ok) {
        this.quarantine++;
        insertEvent(this.db, {
          actor: 'receiver',
          type: 'frame.quarantined',
          connId: line.connId,
          global: verdict.global ?? null,
          detail: { ...verdict, release: msg.release, epoch: msg.epoch },
        });
      }
      return;
    }
    if (msg.type === 'status') this.onStatus(line, msg);
  }

  onStatus(line, msg) {
    insertEvent(this.db, {
      actor: 'source',
      type: 'line.status',
      connId: line.connId,
      detail: { state: msg.state, reason: msg.reason },
    });
    line.state = msg.state;
    if (msg.state === 'degraded' || msg.state === 'drop') {
      this.setState(line.connId, 'suspect', msg.reason);
      // A degraded old line is still worth harvesting. In 'overlap' the new
      // line starts immediately; in 'hold' we retire first, so the old tail is
      // rejected and the hole is fixed by numbered replay.
      if (this.policy === 'hold') this.retire(line, 'policy-hold');
      else if (line === this.current && !line.supersededBy) this.spawnReplacement(line);
    } else if (msg.state === 'suspect') {
      this.setState(line.connId, 'suspect', msg.reason);
    } else if (msg.state === 'complete') {
      this.setState(line.connId, 'complete', msg.reason);
      line.state = 'complete';
    } else if (msg.state === 'closed' || msg.state === 'aborted') {
      this.setState(line.connId, 'closed', msg.reason);
    }
  }

  spawnReplacement(oldLine) {
    oldLine.supersededBy = 'newer-line';
    // the new line must not inherit the old line's epoch or close notice
    this.openLiveLine();
  }

  onClose(line) {
    if (line.closed) return;
    line.closed = true;
    this.lastCloseAt = Date.now();
    // A retirement is a decision about identity, not a transport accident; the
    // socket closing afterwards must not rewrite it as merely 'closed'.
    if (line.state !== 'complete' && line.state !== 'retired') {
      this.setState(line.connId, 'closed', 'socket-closed');
    }
    insertEvent(this.db, { actor: 'receiver', type: 'line.closed', connId: line.connId });
    // Only the current line may trigger reconnection. A late close of a
    // superseded/already-retired line must never disturb its replacement.
    if (line === this.current && !line.supersededBy) this.scheduleReconnect(250);
  }

  retire(line, why) {
    if (line.state === 'retired') return;
    line.state = 'retired';
    line.supersededBy = line.supersededBy || 'retired';
    this.setState(line.connId, 'retired', why);
    insertEvent(this.db, { actor: 'receiver', type: 'line.retired', connId: line.connId, detail: { why } });
    try {
      line.ws.close();
    } catch {
      /* ignore */
    }
    if (line === this.current) this.scheduleReconnect(120);
  }

  setState(connId, state, note) {
    this.db
      .prepare(
        `UPDATE connections SET state=?,
           closed_at=CASE WHEN ? IN ('closed','retired','complete') THEN ? ELSE closed_at END,
           note=COALESCE(?, note) WHERE conn_id=?`
      )
      .run(state, state, Date.now(), note ?? null, connId);
  }

  connection(connId) {
    const row = this.db.prepare('SELECT * FROM connections WHERE conn_id=?').get(connId);
    if (!row) return null;
    return { ...row, basis: row.basis_json ? JSON.parse(row.basis_json) : null };
  }

  /**
   * Repair sessions live in the backfiller process, but their identity must be
   * registered here so their frames obey the same epoch/basis quarantine. The
   * backfiller never gets to reuse a live line's identity for a replay socket.
   */
  registerConnection({ connId, role, basis, note }) {
    const existing = this.db.prepare('SELECT conn_id FROM connections WHERE conn_id=?').get(connId);
    if (existing) {
      if (basis) {
        this.db
          .prepare(`UPDATE connections SET epoch=?, basis_key=?, basis_json=?, state='live' WHERE conn_id=?`)
          .run(basis.epoch, basis.key, JSON.stringify(basis), connId);
      }
      return this.connection(connId);
    }
    this.db
      .prepare(
        `INSERT INTO connections (conn_id, role, epoch, basis_key, basis_json, state, opened_at, note)
         VALUES (?, ?, ?, ?, ?, 'live', ?, ?)`
      )
      .run(
        connId,
        role || 'repair',
        basis?.epoch ?? null,
        basis?.key ?? null,
        basis ? JSON.stringify(basis) : null,
        Date.now(),
        note ?? null
      );
    insertEvent(this.db, {
      actor: 'receiver',
      type: 'conn.register',
      connId,
      detail: { role: role || 'repair', epoch: basis?.epoch ?? null, note },
    });
    return this.connection(connId);
  }

  // ----------------------------------------------------------------- ingest

  /**
   * Central accept path, used by live lines AND by repair sessions (which post
   * here after reading a replay socket). Returns an explicit verdict.
   */
  ingest({ connId, epoch, release, quote, fp, tail, origin }) {
    const at = Date.now();
    const conn = this.connection(connId);
    if (!conn) return { ok: false, reason: 'unknown-connection' };
    if (!conn.basis) {
      this.quarantine++;
      insertEvent(this.db, { actor: 'receiver', type: 'frame.quarantined', connId, detail: { reason: 'no-basis', epoch, release } });
      return { ok: false, reason: 'no-basis' };
    }
    if (conn.state === 'retired') {
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'frame.rejected',
        connId,
        detail: { reason: 'retired-connection', epoch, release, origin },
      });
      return { ok: false, reason: 'retired-connection' };
    }
    if (epoch !== conn.basis.epoch) {
      this.quarantine++;
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'frame.quarantined',
        connId,
        detail: { reason: 'epoch-mismatch', epoch, release, expectedEpoch: conn.basis.epoch },
      });
      return { ok: false, reason: 'epoch-mismatch' };
    }
    const global = deriveGlobal(release, conn.basis);
    if (global == null) {
      this.quarantine++;
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'frame.quarantined',
        connId,
        detail: { reason: 'release-below-basis', epoch, release },
      });
      return { ok: false, reason: 'release-below-basis' };
    }
    if (!quote || typeof quote.symbol !== 'string' || !Number.isFinite(quote.px)) {
      insertEvent(this.db, { actor: 'receiver', type: 'frame.rejected', connId, detail: { reason: 'malformed' } });
      return { ok: false, reason: 'malformed' };
    }
    const computed = contentFp(quote);
    if (fp && fp !== computed) {
      insertEvent(this.db, { actor: 'receiver', type: 'frame.rejected', connId, detail: { reason: 'fp-mismatch', claimed: fp, computed } });
      return { ok: false, reason: 'fp-mismatch' };
    }
    const originClass = origin || (tail ? 'tail' : conn.role === 'replay' ? 'replay' : 'live');
    // A tail is what a stalled socket flushed after it stopped following the
    // publish clock. Its content is useful but not trustworthy on its own, so it
    // lands as tentative coverage that a numbered replay must confirm.
    const grade = originClass === 'tail' ? 'tentative' : 'firm';

    // Idempotency is scoped to the socket+epoch+release (a retry, not a fact).
    const dup = this.db
      .prepare('SELECT 1 AS x FROM frames WHERE conn_id=? AND epoch=? AND release=? AND decision<>\'rejected\'')
      .get(connId, epoch, release);
    if (dup) return { ok: true, duplicate: true, global };

    const existing = this.db.prepare('SELECT * FROM coverage WHERE global=?').get(global);
    if (!existing) {
      const info = this.db
        .prepare(
          `INSERT INTO frames (conn_id, epoch, release, global, symbol, fp, payload, origin, decision, at)
           VALUES (?,?,?,?,?,?,?,?,'accepted',?)`
        )
        .run(connId, epoch, release, global, quote.symbol, computed, JSON.stringify(quote), originClass, at);
      const frameId = Number(info.lastInsertRowid);
      this.db
        .prepare('INSERT INTO coverage (global, fp, frame_id, at, grade) VALUES (?,?,?,?,?)')
        .run(global, computed, frameId, at, grade);
      this.bumpConn(connId, global);
      this.openGaps(global);
      this.advance();
      const gap = this.db.prepare('SELECT * FROM gaps WHERE global=?').get(global);
      if (gap && gap.filled_at == null) {
        this.db.prepare('UPDATE gaps SET filled_at=? WHERE global=?').run(at, global);
        insertEvent(this.db, { actor: 'receiver', type: 'gap.filled', global, connId, detail: { by: originClass, grade } });
      }
      return { ok: true, accepted: true, global, frameId, fp: computed, origin: originClass, symbol: quote.symbol, grade };
    }

    if (existing.fp === computed) {
      const info = this.db
        .prepare(
          `INSERT INTO frames (conn_id, epoch, release, global, symbol, fp, payload, origin, decision, at)
           VALUES (?,?,?,?,?,?,?,?,'confirmed',?)`
        )
        .run(connId, epoch, release, global, quote.symbol, computed, JSON.stringify(quote), originClass, at);
      // A replay/live confirmation promotes a tentative tail row to firm.
      if (existing.grade === 'tentative' && grade === 'firm') {
        this.db.prepare('UPDATE coverage SET grade=? WHERE global=?').run('firm', global);
        insertEvent(this.db, {
          actor: 'receiver',
          type: 'coverage.firmed',
          global,
          connId,
          detail: { fp: computed, by: originClass },
        });
      }
      this.db.prepare('UPDATE gaps SET filled_at=? WHERE global=? AND filled_at IS NULL').run(at, global);
      this.bumpConn(connId, global);
      return { ok: true, confirmed: true, global, frameId: Number(info.lastInsertRowid), fp: computed, origin: originClass, grade };
    }

    // Same global, different content: seal both, decide later.
    const groupId = `CG-${global}`;
    let group = this.db.prepare('SELECT * FROM conflicts WHERE group_id=?').get(groupId);
    const info = this.db
      .prepare(
        `INSERT INTO frames (conn_id, epoch, release, global, symbol, fp, payload, origin, decision, conflict_group, at)
         VALUES (?,?,?,?,?,?,?,?,'contested',?,?)`
      )
      .run(connId, epoch, release, global, quote.symbol, computed, JSON.stringify(quote), originClass, groupId, at);
    const frameId = Number(info.lastInsertRowid);
    if (!group) {
      const note = { variants: [existing.frame_id, frameId], rule: 'replay>live>tail' };
      this.db
        .prepare(
          `INSERT INTO conflicts (group_id, global, symbol, decision, winner_fp, winner_frame, note, opened_at)
           VALUES (?,?,?,'pending',?,?,?,?)`
        )
        .run(groupId, global, quote.symbol, existing.fp, existing.frame_id, JSON.stringify(note), at);
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'conflict.open',
        global,
        connId,
        detail: {
          groupId,
          held: { fp: existing.fp, frameId: existing.frame_id, origin: this.frameOrigin(existing.frame_id) },
          challenger: { fp: computed, frameId, origin: originClass, connId },
        },
      });
      // A tentative tail never silently replaces firm content, and firm content
      // never silently replaces a tail: both stay in the group.
    } else {
      const note = safeJson(group.note, { variants: [] });
      note.variants = [...new Set([...(note.variants || []), frameId])];
      this.db.prepare('UPDATE conflicts SET note=? WHERE group_id=?').run(JSON.stringify(note), groupId);
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'conflict.extend',
        global,
        connId,
        detail: { groupId, frameId, fp: computed, origin: originClass },
      });
    }
    this.bumpConn(connId, global);
    return { ok: true, contested: true, global, frameId, fp: computed, origin: originClass, groupId };
  }

  frameOrigin(frameId) {
    const r = this.db.prepare('SELECT origin FROM frames WHERE frame_id=?').get(frameId);
    return r?.origin ?? null;
  }

  bumpConn(connId, global) {
    this.db
      .prepare(
        `UPDATE connections SET
           global_first=CASE WHEN global_first IS NULL OR ?<global_first THEN ? ELSE global_first END,
           global_last=CASE WHEN global_last IS NULL OR ?>global_last THEN ? ELSE global_last END,
           frames_accepted=frames_accepted+1
         WHERE conn_id=?`
      )
      .run(global, global, global, global, connId);
  }

  openGaps(global) {
    // We just saw a later global: everything from cursor+1 up to global-1 that
    // has no coverage is, by definition, a hole.
    if (global <= this.cursor + 1) return;
    const stmt = this.db.prepare(
      'INSERT OR IGNORE INTO gaps (global, opened_at) VALUES (?, ?)'
    );
    const cov = this.db.prepare('SELECT 1 AS x FROM coverage WHERE global=?');
    for (let g = this.cursor + 1; g < global; g++) {
      if (!cov.get(g)) stmt.run(g, Date.now());
    }
  }

  /**
   * Everything below the supplier's live frontier that we do not cover is a
   * hole. A socket that attaches at globalStart only ever carries numbers from
   * globalStart onward (history is reachable only through numbered replay), so
   * on a resume or a reconnect that skipped ahead, the range cursor+1..limit
   * will never arrive on that socket. Marking it lets the backfiller decide
   * between replay and waiting — the same rule as a skipped frame, discovered
   * from identity instead of from a later global.
   */
  markMissing(until) {
    const wanted = Math.min(Number(until) || 0, this.total ?? Infinity);
    if (!Number.isFinite(wanted) || wanted <= this.cursor) return 0;
    const stmt = this.db.prepare('INSERT OR IGNORE INTO gaps (global, opened_at) VALUES (?, ?)');
    const cov = this.db.prepare('SELECT 1 AS x FROM coverage WHERE global=?');
    const at = Date.now();
    let opened = 0;
    for (let g = this.cursor + 1; g <= wanted; g++) {
      if (cov.get(g)) continue;
      const info = stmt.run(g, at);
      if (Number(info.changes) > 0) opened++;
    }
    if (opened) {
      insertEvent(this.db, {
        actor: 'receiver',
        type: 'gaps.marked',
        detail: { from: this.cursor + 1, to: wanted, opened, why: 'live-frontier' },
      });
    }
    return opened;
  }

  advance() {
    const has = this.db.prepare('SELECT 1 AS x FROM coverage WHERE global=?');
    let moved = 0;
    while (has.get(this.cursor + 1)) {
      this.cursor++;
      moved++;
    }
    if (moved) this.setControl('cursor', this.cursor);
  }

  recomputeCursor() {
    this.cursor = 0;
    const has = this.db.prepare('SELECT 1 AS x FROM coverage WHERE global=?');
    while (has.get(this.cursor + 1)) this.cursor++;
    this.setControl('cursor', this.cursor);
  }

  // ------------------------------------------------------------ arbitration

  resolveConflict(groupId, by = 'auto') {
    const group = this.db.prepare('SELECT * FROM conflicts WHERE group_id=?').get(groupId);
    if (!group) return { ok: false, reason: 'unknown-group' };
    if (group.decision !== 'pending') return { ok: true, skipped: true, groupId };
    const note = safeJson(group.note, { variants: [] });
    const variants = note.variants
      .map((id) => this.db.prepare('SELECT * FROM frames WHERE frame_id=?').get(id))
      .filter(Boolean);
    const rank = (f) => [classRank(f), f.epoch ?? 0, f.frame_id];
    const winner = variants.slice().sort((a, b) => {
      const ra = rank(a);
      const rb = rank(b);
      if (ra[0] !== rb[0]) return rb[0] - ra[0];
      if (ra[1] !== rb[1]) return rb[1] - ra[1];
      return rb[2] - ra[2];
    })[0];
    const at = Date.now();
    tx(this.db, () => {
      this.db
        .prepare(
          `UPDATE conflicts SET decision='decided', winner_fp=?, winner_frame=?, decided_by=?, decided_at=? WHERE group_id=?`
        )
        .run(winner.fp, winner.frame_id, by, at, groupId);
      this.db
        .prepare(`UPDATE frames SET decision=CASE WHEN frame_id=? THEN 'accepted' ELSE 'rejected' END WHERE conflict_group=?`)
        .run(winner.frame_id, groupId);
      this.db
        .prepare('UPDATE coverage SET fp=?, frame_id=?, at=?, grade=? WHERE global=?')
        .run(winner.fp, winner.frame_id, at, winner.origin === 'tail' ? 'tentative' : 'firm', group.global);
    });
    insertEvent(this.db, {
      actor: 'receiver',
      type: 'conflict.decided',
      global: group.global,
      connId: winner.conn_id,
      detail: {
        groupId,
        rule: 'replay>live>tail',
        winner: { frameId: winner.frame_id, origin: winner.origin, connId: winner.conn_id, fp: winner.fp },
        losers: variants.filter((v) => v.frame_id !== winner.frame_id).map((v) => ({ frameId: v.frame_id, origin: v.origin, fp: v.fp })),
        by,
      },
    });
    return {
      ok: true,
      groupId,
      global: group.global,
      winnerFrame: winner.frame_id,
      via: winner.origin,
      changed: winner.fp !== group.winner_fp,
    };
  }

  pendingConflicts() {
    return this.db.prepare("SELECT * FROM conflicts WHERE decision='pending' ORDER BY global").all();
  }

  resolveAll(by = 'auto') {
    return this.pendingConflicts().map((c) => this.resolveConflict(c.group_id, by));
  }

  // ---------------------------------------------------------------- queries

  state() {
    const connections = this.db
      .prepare('SELECT * FROM connections ORDER BY opened_at, conn_id')
      .all()
      .map((c) => ({ ...c, basis: c.basis_json ? JSON.parse(c.basis_json) : null, label: connStateLabel(c.state) }));
    const gaps = this.db
      .prepare(
        `SELECT g.global, g.opened_at FROM gaps g
         LEFT JOIN coverage c ON c.global=g.global WHERE c.global IS NULL ORDER BY g.global`
      )
      .all();
    // Coverage that exists but is only a stale-tail guess. It counts toward the
    // cursor (so the live line can move on) but not toward "verified complete".
    const tentative = this.db
      .prepare('SELECT global, fp, frame_id, at FROM coverage WHERE grade=\'tentative\' ORDER BY global')
      .all();
    const conflicts = this.db.prepare('SELECT * FROM conflicts ORDER BY global').all();
    const pendingConflicts = conflicts.filter((c) => c.decision === 'pending').map((c) => c.global);
    const covered = this.db.prepare('SELECT COUNT(*) AS n FROM coverage').get().n;
    const total = this.total ?? covered;
    // Cost ledger for the two reconnect policies: how many bytes each source
    // class actually contributed, and how many were refused by identity rules.
    const origins = Object.fromEntries(
      this.db
        .prepare('SELECT origin, COUNT(*) AS n FROM frames GROUP BY origin')
        .all()
        .map((r) => [r.origin, r.n])
    );
    const rejected = this.db
      .prepare("SELECT COUNT(*) AS n FROM events WHERE type='frame.rejected'")
      .get().n;
    const gapsOpened = this.db.prepare('SELECT COUNT(*) AS n FROM gaps').get().n;
    const gapsFilled = this.db
      .prepare('SELECT COUNT(*) AS n FROM gaps WHERE filled_at IS NOT NULL')
      .get().n;
    return {
      ok: true,
      cursor: this.cursor,
      total: this.total,
      covered,
      origins,
      rejected,
      gapsOpened,
      gapsFilled,
      gaps,
      tentative,
      conflicts,
      connections,
      policy: this.policy,
      paused: this.paused,
      quarantine: this.quarantine,
      reconnects: this.reconnects,
      lines: this.lines.map((l) => ({ connId: l.connId, state: l.state, basis: l.basis, closed: l.closed, supersededBy: l.supersededBy })),
      // two different completion claims, deliberately separate:
      //  receivedComplete = every global has some frame (tail included)
      //  verifiedComplete = every global is firm (no tail-only, no pending conflict)
      receivedComplete: this.total != null && this.cursor >= this.total && gaps.length === 0,
      verifiedComplete:
        this.total != null &&
        this.cursor >= this.total &&
        gaps.length === 0 &&
        tentative.length === 0 &&
        pendingConflicts.length === 0,
      pendingConflicts,
    };
  }

  decisions() {
    return this.db
      .prepare(
        `SELECT c.global, c.fp, c.frame_id, c.grade, f.conn_id, f.epoch, f.origin, f.payload, f.decision,
                COALESCE(k.decision, 'none') AS conflict_decision
         FROM coverage c
         JOIN frames f ON f.frame_id=c.frame_id
         LEFT JOIN conflicts k ON k.global=c.global
         ORDER BY c.global`
      )
      .all()
      .map((r) => ({
        global: r.global,
        fp: r.fp,
        frameId: r.frame_id,
        connId: r.conn_id,
        epoch: r.epoch,
        origin: r.origin,
        grade: r.grade,
        quote: JSON.parse(r.payload),
        decision: r.decision,
        // Landed provisional rows are honest: the desk can see the value is
        // currently-winning but not yet arbitrated.
        provisional: r.conflict_decision === 'pending',
      }));
  }

  trace(global) {
    const coverage = this.db.prepare('SELECT * FROM coverage WHERE global=?').get(global) || null;
    const conflict = this.db.prepare('SELECT * FROM conflicts WHERE global=?').get(global) || null;
    const frames = this.db
      .prepare('SELECT * FROM frames WHERE global=? ORDER BY at, frame_id')
      .all(global)
      .map((f) => ({ ...f, payload: JSON.parse(f.payload) }));
    const events = this.db
      .prepare('SELECT * FROM events WHERE global=? ORDER BY seq')
      .all(global)
      .map((e) => ({ ...e, detail: safeJson(e.detail, null) }));
    return { global, coverage, conflict, frames, events };
  }

  recentEvents(limit = 120) {
    return this.db
      .prepare('SELECT * FROM events ORDER BY seq DESC LIMIT ?')
      .all(limit)
      .reverse()
      .map((e) => ({ ...e, detail: safeJson(e.detail, null) }));
  }

  setPolicy(policy) {
    if (!['overlap', 'hold'].includes(policy)) throw new Error('invalid policy');
    this.policy = policy;
    this.setControl('policy', policy);
    insertEvent(this.db, { actor: 'desk', type: 'desk.policy', detail: { policy } });
  }

  setPaused(paused) {
    this.paused = !!paused;
    this.setControl('paused', this.paused);
    insertEvent(this.db, { actor: 'desk', type: this.paused ? 'desk.paused' : 'desk.resumed' });
    if (this.paused) {
      if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
      // Freeze the receive side without retiring identity: on resume the same
      // line objects can reconnect under a fresh epoch when needed.
      for (const line of this.lines) {
        if (!line.closed) {
          line.supersededBy = 'paused';
          try {
            line.ws.close();
          } catch {
            /* ignore */
          }
        }
      }
    } else {
      // Resume exactly from the durable cursor: a fresh socket asks the
      // supplier for the first global we still lack.
      this.openLiveLine();
    }
  }

  /**
   * Explicitly allow/deny a connection's frames. Needed when the page decides
   * to stop the old line before backfilling ("hold" policy).
   */
  setConnectionState(connId, state, note) {
    this.setState(connId, state, note);
    return this.connection(connId);
  }

  stop() {
    this.stopping = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    for (const line of this.lines) {
      try {
        line.ws.close();
      } catch {
        /* ignore */
      }
    }
  }
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

// -------------------------------------------------------------------- process

export async function startReceiver({ port = 0, dbPath, sourceUrl, policy = 'overlap', total = null }) {
  const core = new ReceiverCore({ dbPath });
  if (policy && ['overlap', 'hold'].includes(policy)) core.setPolicy(policy);
  if (total) core.total = Number(total);
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://local');
    const reply = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    try {
      if (req.method === 'GET' && url.pathname === '/health') return reply(200, { ok: true, port: server.address().port });
      if (req.method === 'GET' && url.pathname === '/state') return reply(200, core.state());
      if (req.method === 'GET' && url.pathname === '/decisions') return reply(200, { decisions: core.decisions() });
      if (req.method === 'GET' && url.pathname === '/events') {
        const limit = Math.min(400, Math.max(1, Number(url.searchParams.get('limit') || '120')));
        return reply(200, { events: core.recentEvents(limit) });
      }
      if (req.method === 'GET' && url.pathname.startsWith('/trace/')) {
        return reply(200, core.trace(Number(url.pathname.slice('/trace/'.length))));
      }
      if (req.method === 'POST') {
        const body = await readJson(req);
        switch (url.pathname) {
          case '/start':
            core.start({ sourceUrl: body.sourceUrl || sourceUrl });
            return reply(200, { ok: true });
          case '/frames':
            return reply(200, core.ingest(body));
          case '/register': // repair socket identity is created by the backfiller
            return reply(200, core.registerConnection(body));
          case '/resolve':
            return reply(200, body.groupId ? core.resolveConflict(body.groupId, body.by) : { ok: true, results: core.resolveAll(body.by) });
          case '/policy':
            core.setPolicy(body.policy);
            return reply(200, { ok: true, policy: core.policy });
          case '/pause':
            core.setPaused(body.paused);
            return reply(200, { ok: true, paused: core.paused });
          case '/connection/state':
            return reply(200, { ok: true, connection: core.setConnectionState(body.connId, body.state, body.note) });
          case '/total':
            core.total = Number(body.total);
            return reply(200, { ok: true, total: core.total });
          case '/event':
            insertEvent(core.db, {
              actor: body.actor || 'external',
              type: body.type || 'external.event',
              global: body.global ?? null,
              connId: body.connId ?? null,
              detail: body.detail ?? null,
            });
            return reply(200, { ok: true });
          case '/quit':
            core.stop();
            reply(200, { ok: true });
            setTimeout(() => process.exit(0), 50);
            return;
          default:
            return reply(404, { error: 'not-found' });
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

if (process.argv[1] && /receiver\.mjs$/.test(process.argv[1])) {
  const dataDir = process.env.DATA_DIR || './data';
  const { port } = await startReceiver({
    port: Number(process.env.PORT || 0),
    dbPath: `${dataDir}/receiver.db`,
    sourceUrl: process.env.SOURCE_URL || 'http://127.0.0.1:8899',
    policy: process.env.POLICY || 'overlap',
  });
  if (process.send) process.send({ type: 'ready', port });
  log('listening on', port);
}
