// Simulated exchange feed — a fixture, not a production component.
//
// Model: one publish clock emits globals 1..N at a fixed cadence after a
// prefill burst. Each live socket attaches at a global and then follows the
// clock. A socket can stall: it stops following the clock but stays open, and
// later flushes a stale tail. That is what makes "the old connection is still
// sending while a new one is live" a real overlap, not a staged one.
//
// Wire protocol (JSON text frames):
//   c -> s  {type:'attach', start, globalStart} | {type:'ack', release}
//   s -> c  {type:'welcome'|'hello'|'quote'|'status'|'rebase'}
//
// Every live socket and every replay session gets its own epoch and its own
// released numbering. A release number means nothing without the socket+epoch
// that carried it, so the desk maps it through the basis in 'hello'.
import { WebSocketServer } from './ws.mjs';
import {
  buildFeed,
  contentFp,
  manifestFp,
  planFor,
  quoteOf,
  sha1,
  shortId,
  SYMBOLS,
} from './shared.mjs';

const log = (...a) => console.log('[source]', ...a);

export class MarketSource {
  constructor({ presetKind = 'steady' } = {}) {
    this.presetKind = presetKind;
    this.plan = planFor(presetKind);
    this.feed = this.plan.feed;
    this.total = this.feed.total;
    this.epoch = 5000;
    this.published = 0;
    this.clock = null;
    this.startedAt = null;
    this.live = [];
    this.replays = new Map();
    this.faultTimers = new Set();
    this.stopping = false;
    this.autoChaos = true;
    this.flags = { renumber: null, silent: null, break: null, conflict: null };
    this.presetReplayCount = 0;
    this.stats = { liveLines: 0, replaySessions: 0, tailFrames: 0, conflictFrames: 0, renumbers: 0, sessionBreaks: 0 };
    this.eventLog = [];
  }

  nextEpoch() {
    return this.epoch++;
  }

  makeBasis({ epoch, releaseStart, globalStart, source }) {
    return {
      sourceRun: sha1('run', this.presetKind, this.total).slice(0, 12),
      epoch,
      releaseStart,
      globalStart,
      source,
      key: sha1('basis', epoch, releaseStart, globalStart, source),
    };
  }

  // ------------------------------------------------------------------- clock

  start() {
    if (this.clock) return;
    this.startedAt = Date.now();
    this.published = 0;
    this.clockPaused = true; // wait for the desk to attach before publishing live
    // A fixed 1-global-per-interval clock is simple, but the prefill must exist
    // for replay-from-1 to be possible. Publish history immediately, then pace.
    this.published = this.feed.prefill;
    this.clock = setInterval(() => {
      if (this.stopping || this.clockPaused) return;
      if (this.published < this.total) this.published++;
      this.pump();
    }, this.feed.intervalMs);
  }

  releaseClock() {
    this.clockPaused = false;
  }

  publishNow(n) {
    this.published = Math.max(this.published, Math.min(this.total, n));
    this.pump();
  }

  pump() {
    for (const rec of this.live) {
      if (rec.closed || !rec.basis || rec.paused) continue;
      this.feedLine(rec);
    }
  }

  feedLine(rec) {
    while (rec.next <= Math.min(this.published, this.total)) {
      if (rec.stallAt && rec.next >= rec.stallAt) {
        this.stallLine(rec);
        return;
      }
      const g = rec.next++;
      if (rec.skip?.has(g)) continue;
      this.sendQuote(rec, g);
    }
  }

  sendQuote(rec, global, { tail = false, quote = null } = {}) {
    const m = this.feed.messages[global - 1];
    if (!m) return;
    const q = quote || quoteOf(m);
    rec.send({
      type: 'quote',
      connId: rec.connId,
      epoch: rec.basis.epoch,
      release: rec.basis.releaseStart + (global - rec.basis.globalStart),
      quote: q,
      fp: contentFp(q),
      tail,
      serverTime: Date.now(),
    });
  }

  // --------------------------------------------------------------- live line

  handleConnection(ws, req) {
    const url = new URL(req.url, 'http://local');
    const role = url.searchParams.get('role') || 'live';
    const connId = url.searchParams.get('connId') || shortId('src');
    if (role === 'replay') this.serveReplay(ws, url, connId);
    else this.serveLive(ws, url, connId);
  }

  serveLive(ws, url, connId) {
    const rec = {
      ws,
      connId,
      closed: false,
      basis: null,
      next: 1,
      paused: false,
      skip: null,
      lineIndex: this.live.length,
      send: (obj) => {
        if (!rec.closed && ws.readyState === 1) ws.send(JSON.stringify(obj));
      },
    };
    this.live.push(rec);
    this.stats.liveLines++;
    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(String(raw));
      } catch {
        return;
      }
      if (msg.type === 'attach' && !rec.basis) this.attach(rec, msg);
      else if (msg.type === 'ack') rec.acked = msg.release;
    });
    ws.on('close', () => {
      rec.closed = true;
      this.logEvent('live-closed', { connId });
    });
    rec.send({
      type: 'welcome',
      role: 'live',
      connId,
      preset: this.presetKind,
      total: this.total,
      published: this.published,
      hint: 'attach',
    });
  }

  attach(rec, msg) {
    const requestedStart = Number(msg.globalStart ?? msg.start ?? 1);
    const releaseStart = Number(msg.start ?? requestedStart);
    const firstLine = this.live.length === 1;
    // The first live line is the initial subscription: it gets the stored
    // history burst. Any later line is a *live* subscription that starts at the
    // publish frontier. History is then only reachable through numbered replay,
    // which is exactly the cost trade-off the desk is deciding about.
    const globalStart = firstLine ? requestedStart : Math.max(requestedStart, this.published + 1);
    rec.basis = this.makeBasis({
      epoch: this.nextEpoch(),
      releaseStart: releaseStart + (globalStart - requestedStart),
      globalStart,
      source: 'live',
    });
    rec.next = globalStart;
    rec.send({
      type: 'hello',
      connId: rec.connId,
      role: 'live',
      basis: rec.basis,
      total: this.total,
      intervalMs: this.feed.intervalMs,
      serverTime: Date.now(),
    });
    this.logEvent('live-attached', {
      connId: rec.connId,
      epoch: rec.basis.epoch,
      globalStart,
      requestedStart,
      liveOnly: !firstLine,
    });
    this.clockPaused = false;
    if (firstLine && this.plan.hasFaults && this.autoChaos) this.armAutoChaos(rec);
    else if (firstLine) this.prefill(rec);
    this.prepareLine(rec, globalStart);
  }

  /** Initial subscription gets the stored history in a fast burst. */
  prefill(rec) {
    let n = 0;
    while (rec.next <= Math.min(this.published, this.total) && n < 200) {
      const g = rec.next++;
      this.sendQuote(rec, g);
      n++;
    }
    if (n) this.logEvent('prefill-sent', { connId: rec.connId, count: n, to: rec.next - 1 });
  }

  prepareLine(rec, globalStart) {
    if (!this.plan.hasFaults || !this.autoChaos) return;
    // The replacement line silently loses a run of frames, so the hole can only
    // be closed by a numbered replay. Armed once per run.
    if (this.live.length > 1 && !this.dropsArmed) {
      this.dropsArmed = true;
      const dropFrom = globalStart + 8;
      rec.skip = rec.skip || new Set();
      for (let i = 0; i < 6; i++) rec.skip.add(dropFrom + i);
      this.logEvent('auto-drops-armed', { connId: rec.connId, from: dropFrom, to: dropFrom + 5 });
    }
  }

  // --------------------------------------------------------- stall + old tail

  /**
   * Page-triggered: the live socket stops following the clock. The receiver
   * sees "degraded" and opens a replacement. The stall is per socket, so the
   * replacement is unaffected.
   */
  stall(connId = null, opts = {}) {
    const rec = connId
      ? this.live.find((r) => r.connId === connId)
      : [...this.live].reverse().find((r) => !r.closed && !r.paused);
    if (!rec) return { ok: false, reason: 'no-live-line' };
    const at = rec.next;
    rec.stallAt = opts.atGlobal && opts.atGlobal > rec.basis.globalStart ? opts.atGlobal : at;
    const conflictOffset = Number(opts.conflictOffset ?? 3);
    if (opts.tailCount != null) rec.tailCount = Number(opts.tailCount);
    this.logEvent('stall', { connId: rec.connId, atGlobal: rec.stallAt, conflictOffset, tailCount: rec.tailCount ?? null });
    if (rec.stallAt <= this.published) this.stallLine(rec);
    return { ok: true, connId: rec.connId, atGlobal: rec.stallAt, conflictOffset };
  }

  stallLine(rec) {
    if (rec.paused) return;
    rec.paused = true;
    const G = rec.stallAt;
    rec.send({
      type: 'status',
      connId: rec.connId,
      epoch: rec.basis.epoch,
      state: 'degraded',
      reason: 'publish-pause',
      serverTime: Date.now(),
    });
    this.logEvent('stalled', { connId: rec.connId, atGlobal: G });
    // The page arms how much of the stale tail the socket will flush. Default
    // comes from the preset so a scripted run stays reproducible.
    const f = this.plan.faults;
    const tailCount = rec.tailCount ?? f?.tailCount ?? 6;
    const conflictOffset = rec.conflictOffset ?? f?.conflictTailOffset ?? 3;
    const delay = rec.tailDelayMs ?? f?.tailDelayMs ?? 700;
    this.faultTimer(() => {
      if (rec.closed) return;
      for (let i = 0; i < tailCount; i++) {
        const g = G + i;
        if (g > this.published) break;
        const m = this.feed.messages[g - 1];
        let q = quoteOf(m);
        if (i === conflictOffset) {
          q = { ...q, px: Number((q.px + 2.75).toFixed(4)), qty: q.qty + 9 };
          this.stats.conflictFrames++;
          this.logEvent('tail-conflict', { connId: rec.connId, global: g });
        }
        rec.send({
          type: 'quote',
          connId: rec.connId,
          epoch: rec.basis.epoch,
          release: rec.basis.releaseStart + (g - rec.basis.globalStart),
          quote: q,
          fp: contentFp(q),
          tail: true,
          serverTime: Date.now(),
        });
        this.stats.tailFrames++;
      }
      rec.send({
        type: 'status',
        connId: rec.connId,
        epoch: rec.basis.epoch,
        state: 'suspect',
        reason: 'tail-after-pause',
        serverTime: Date.now(),
      });
      this.logEvent('tail-flushed', { connId: rec.connId, from: G, to: G + tailCount - 1 });
    }, delay);
    this.faultTimer(() => {
      rec.send({
        type: 'status',
        connId: rec.connId,
        epoch: rec.basis.epoch,
        state: 'drop',
        reason: 'carrier-loss',
        serverTime: Date.now(),
      });
    }, delay + 260);
    this.faultTimer(() => {
      rec.send({
        type: 'status',
        connId: rec.connId,
        epoch: rec.basis.epoch,
        state: 'closed',
        reason: 'peer-timeout',
        serverTime: Date.now(),
      });
      this.faultTimer(() => {
        rec.closed = true;
        try {
          rec.ws.close();
        } catch {
          /* ignore */
        }
      }, 30);
    }, delay + 2200);
  }

  /** Page-triggered: drop a run of globals on the current live line. */
  drop(connId = null, from = 0, count = 4) {
    const rec = connId
      ? this.live.find((r) => r.connId === connId)
      : [...this.live].reverse().find((r) => !r.closed);
    if (!rec) return { ok: false, reason: 'no-live-line' };
    const start = Math.max(rec.next, from || rec.next);
    rec.skip = rec.skip || new Set();
    for (let i = 0; i < count; i++) rec.skip.add(start + i);
    this.logEvent('drops-armed', { connId: rec.connId, from: start, to: start + count - 1 });
    return { ok: true, connId: rec.connId, from: start, to: start + count - 1 };
  }

  faultTimer(fn, ms) {
    const t = setTimeout(() => {
      this.faultTimers.delete(t);
      if (!this.stopping) fn();
    }, ms);
    this.faultTimers.add(t);
    return t;
  }

  // ------------------------------------------------------------- replay path

  serveReplay(ws, url, connId) {
    const from = Math.max(1, Number(url.searchParams.get('from') || '1'));
    const to = Math.min(this.total, Number(url.searchParams.get('to') || String(this.total)));
    const sessionId = url.searchParams.get('sessionId') || shortId('rpl');
    const attempt = Number(url.searchParams.get('attempt') || '1');
    const purpose = url.searchParams.get('purpose') || 'gap';
    const rec = { ws, connId, sessionId, closed: false, timers: new Set(), sent: 0, attempt };
    this.replays.set(sessionId, rec);
    this.stats.replaySessions++;
    const send = (obj) => {
      if (!rec.closed && ws.readyState === 1) ws.send(JSON.stringify(obj));
    };
    const later = (fn, ms) => {
      const t = setTimeout(() => {
        rec.timers.delete(t);
        if (!rec.closed && !this.stopping) fn();
      }, ms);
      rec.timers.add(t);
    };
    let epoch = this.nextEpoch();
    let release = 1;
    let basis = this.makeBasis({ epoch, releaseStart: 1, globalStart: from, source: 'replay' });
    rec.basis = basis;
    send({ type: 'welcome', role: 'replay', connId, sessionId, attempt, purpose, from, to, preset: this.presetKind });
    send({ type: 'hello', connId, epoch, basis, from, to, intervalMs: 22, serverTime: Date.now() });

    const fault = this.takeReplayFault(attempt, from, to);
    rec.applied = fault;
    this.logEvent('replay-open', { sessionId, from, to, attempt, fault });

    let g = from;
    let sentThisBasis = 0;
    let silent = false;
    const push = () => {
      if (rec.closed) return;
      if (g > to) {
        send({ type: 'status', connId, epoch, state: 'complete', reason: 'session-end', serverTime: Date.now() });
        this.logEvent('replay-complete', { sessionId, frames: rec.sent, fault });
        later(() => {
          rec.closed = true;
          try {
            ws.close();
          } catch {
            /* ignore */
          }
        }, 40);
        return;
      }
      if (fault.breakAfter && rec.sent >= fault.breakAfter) {
        this.stats.sessionBreaks++;
        send({ type: 'status', connId, epoch, state: 'aborted', reason: 'replay-session-reset', serverTime: Date.now() });
        this.logEvent('replay-aborted', { sessionId, afterFrames: rec.sent });
        later(() => {
          rec.closed = true;
          try {
            ws.close();
          } catch {
            /* ignore */
          }
        }, 15);
        return;
      }
      if (fault.renumberAfter && sentThisBasis >= fault.renumberAfter) {
        epoch = this.nextEpoch();
        release = 1;
        sentThisBasis = 0;
        basis = this.makeBasis({ epoch, releaseStart: 1, globalStart: g, source: 'replay-renumber' });
        rec.basis = basis;
        this.stats.renumbers++;
        send({ type: 'rebase', connId, epoch, basis, atGlobal: g, serverTime: Date.now() });
        this.logEvent('replay-rebased', { sessionId, atGlobal: g, newEpoch: epoch });
        fault.renumberAfter = null;
      }
      if (fault.silentAfter && sentThisBasis >= fault.silentAfter && !silent) {
        // Supplier restarts numbering WITHOUT declaring it. The frame is
        // impossible to place; the desk must quarantine, not guess.
        silent = true;
        epoch = this.nextEpoch();
        release = 1;
        sentThisBasis = 0;
        basis = this.makeBasis({ epoch, releaseStart: 1, globalStart: g, source: 'replay-silent' });
        rec.basis = basis;
        this.logEvent('replay-silent-renumber', { sessionId, atGlobal: g, newEpoch: epoch });
      }
      const m = this.feed.messages[g - 1];
      if (m) {
        const q = quoteOf(m);
        if (fault.conflictAt && g === fault.conflictAt) {
          q.px = Number((q.px - 4.5).toFixed(4));
          q.qty = q.qty + 21;
          this.stats.conflictFrames++;
          this.logEvent('replay-conflict', { sessionId, global: g });
        }
        send({ type: 'quote', connId, epoch, release, quote: q, fp: contentFp(q), serverTime: Date.now() });
        release++;
        sentThisBasis++;
        rec.sent++;
        g++;
      } else {
        g++;
      }
      later(push, 22);
    };
    ws.on('close', () => {
      rec.closed = true;
      for (const t of rec.timers) clearTimeout(t);
    });
    later(push, 15);
  }

  /**
   * Suppplier faults in the scripted preset are a one-shot script: the first
   * replay session of the run aborts, the second renumbers its releases. A
   * retry is not "attempt 1 of a range" - tying faults to the request would
   * make every retry fail forever, which is not how a supplier behaves.
   */
  takeReplayFault(attempt, from, to) {
    const fault = { breakAfter: 0, renumberAfter: 0, silentAfter: 0, conflictAt: 0 };
    this.presetReplayCount = (this.presetReplayCount || 0) + 1;
    const n = this.presetReplayCount;
    const rf = this.plan.faults?.replay;
    if (rf && n === rf.failAttempt) fault.breakAfter = rf.failAfterFrames;
    if (rf && n === rf.renumberAttempt) fault.renumberAfter = rf.renumberAfterFrames;
    if (this.flags.break) {
      fault.breakAfter = this.flags.break;
      this.flags.break = null;
    }
    if (this.flags.renumber) {
      fault.renumberAfter = this.flags.renumber;
      this.flags.renumber = null;
    }
    if (this.flags.silent) {
      fault.silentAfter = this.flags.silent;
      this.flags.silent = null;
    }
    if (this.flags.conflict) {
      const at = Number(this.flags.conflict);
      fault.conflictAt = at >= from && at <= to ? at : Math.min(to, Math.max(from, at));
      this.flags.conflict = null;
    }
    return fault;
  }

  // ------------------------------------------------------------ page triggers

  trigger(kind, arg = {}) {
    switch (kind) {
      case 'stall': {
        const res = this.stall(null, {
          conflictOffset: arg.conflictOffset,
          atGlobal: arg.atGlobal,
          tailCount: arg.tailCount,
        });
        return res;
      }
      case 'drop': {
        return this.drop(null, Number(arg.from) || 0, Number(arg.count) || 4);
      }
      case 'renumber':
        this.flags.renumber = Number(arg.after ?? 2);
        this.logEvent('armed', { kind: 'renumber', after: this.flags.renumber });
        return { armed: 'renumber', after: this.flags.renumber };
      case 'silent':
        this.flags.silent = Number(arg.after ?? 2);
        this.logEvent('armed', { kind: 'silent', after: this.flags.silent });
        return { armed: 'silent', after: this.flags.silent };
      case 'break':
        this.flags.break = Number(arg.after ?? 2);
        this.logEvent('armed', { kind: 'break', after: this.flags.break });
        return { armed: 'break', after: this.flags.break };
      case 'conflict': {
        const g = Number(arg.global);
        if (!Number.isFinite(g)) throw new Error('conflict trigger needs a global');
        this.flags.conflict = g;
        this.logEvent('armed', { kind: 'conflict', global: g });
        return { armed: 'conflict', global: g };
      }
      default:
        throw new Error(`unknown trigger ${kind}`);
    }
  }

  // ------------------------------------------------------------- scripted run

  /**
   * In the 'chaos' preset the first live line stalls on its own and the
   * replacement starts with dropped frames. Everything else stays page-driven.
   */
  armAutoChaos(first) {
    first.tailCount = this.plan.faults.tailCount;
    first.conflictOffset = this.plan.faults.conflictTailOffset;
    first.tailDelayMs = this.plan.faults.tailDelayMs;
    first.stallAt = this.plan.stallGlobal;
    first.skip = new Set();
    // History up to the stall point is available to the initial subscription.
    this.publishNow(this.plan.stallGlobal - 1);
    this.prefill(first);
    this.logEvent('auto-stall-armed', { connId: first.connId, atGlobal: this.plan.stallGlobal });
    if (first.next >= first.stallAt) this.stallLine(first);
  }

  // ---------------------------------------------------------------- manifest

  manifest() {
    return {
      preset: this.presetKind,
      total: this.total,
      symbols: SYMBOLS,
      totalFp: manifestFp(this.feed.messages),
      messages: this.feed.messages.map((m) => ({
        global: m.global,
        fp: m.fp,
        symbol: m.symbol,
        tick: m.tick,
        px: m.px,
        qty: m.qty,
        side: m.side,
      })),
    };
  }

  logEvent(type, detail = {}) {
    const entry = { at: Date.now(), type, ...detail };
    this.eventLog.push(entry);
    if (this.eventLog.length > 300) this.eventLog.shift();
    log(type, JSON.stringify(detail));
  }

  summary() {
    return {
      preset: this.presetKind,
      total: this.total,
      prefill: this.feed.prefill,
      intervalMs: this.feed.intervalMs,
      published: this.published,
      stallGlobal: this.plan.stallGlobal,
      hasFaults: this.plan.hasFaults,
      flags: this.flags,
      stats: this.stats,
      startedAt: this.startedAt,
      live: this.live.map((r) => ({
        connId: r.connId,
        epoch: r.basis?.epoch ?? null,
        next: r.next,
        paused: r.paused,
        closed: r.closed,
        skip: r.skip ? [...r.skip] : [],
      })),
      sessions: [...this.replays.values()].map((r) => ({
        sessionId: r.sessionId,
        attempt: r.attempt,
        sent: r.sent,
        applied: r.applied,
        closed: r.closed,
      })),
      events: this.eventLog.slice(-40),
    };
  }

  stop() {
    this.stopping = true;
    clearInterval(this.clock);
    for (const t of this.faultTimers) clearTimeout(t);
    for (const rec of this.live) {
      rec.closed = true;
      try {
        rec.ws.close();
      } catch {
        /* ignore */
      }
    }
    for (const rec of this.replays.values()) {
      rec.closed = true;
      for (const t of rec.timers) clearTimeout(t);
      try {
        rec.ws.close();
      } catch {
        /* ignore */
      }
    }
  }
}

export function attachSource(server, source) {
  const wss = new WebSocketServer({ server, path: '/feed' });
  wss.on('connection', (ws, req) => source.handleConnection(ws, req));
  return wss;
}
