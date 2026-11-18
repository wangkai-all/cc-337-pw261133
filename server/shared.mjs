// Shared domain logic: identity, ordering, content fingerprints, run presets.
// Imported by both the supplier process and the ingest processes.
import { createHash } from 'node:crypto';

export const DESK_TICK_MS = 55;
export const DEFAULT_POLICY = 'overlap';
export const POLICIES = ['overlap', 'hold'];

export function sha1(...parts) {
  const h = createHash('sha1');
  for (const p of parts) h.update(String(p));
  return h.digest('hex');
}

export function shortId(prefix) {
  return `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
}

export function stableStringify(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(stableStringify).join(',')}]`;
  const keys = Object.keys(v).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(v[k])}`).join(',')}}`;
}

// Content identity deliberately excludes epoch / release / global numbering.
// Two suppliers numbering the stream differently can still be compared.
export function contentFp(q) {
  return sha1('content/v1', q.symbol, q.tick, q.px.toFixed(4), q.qty, q.side);
}

/**
 * The whole-stream fingerprint. Both the supplier's manifest and the lander's
 * reconciliation use this exact function so a match means the same bytes, not
 * just the same length.
 */
export function manifestFp(messages) {
  return sha1(
    'manifest/v1',
    stableStringify(messages.map((m) => [m.global, m.fp ?? contentFp(m)]))
  );
}

// The basis is the supplier's declaration of how *this* socket's released
// numbers map onto the global order. It is never inferred from numbering.
export function basisKey(basis) {
  return sha1(
    'basis/v1',
    basis.sourceRun,
    basis.epoch,
    basis.releaseStart,
    basis.globalStart,
    basis.source ?? 'live'
  );
}

export function deriveGlobal(release, basis) {
  if (!basis) return null;
  if (release < basis.releaseStart) return null;
  return basis.globalStart + (release - basis.releaseStart);
}

// ---------------------------------------------------------------- feed model

function mulberry32(a) {
  return function rand() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOG', 'TSLA', 'SPY'];

/**
 * Each tick publishes updates for 1-3 symbols. Every update is one message
 * with one global number and one released number on the socket that carries it.
 */
export function buildFeed({ seed = 11, ticks = 110, intervalMs = DESK_TICK_MS } = {}) {
  const rand = mulberry32(seed);
  const px = new Map(SYMBOLS.map((s, i) => [s, 90 + i * 17.25]));
  const messages = [];
  for (let tick = 0; tick < ticks; tick++) {
    const burst = 1 + Math.floor(rand() * 3);
    const picked = new Set();
    while (picked.size < burst) picked.add(SYMBOLS[Math.floor(rand() * SYMBOLS.length)]);
    for (const symbol of picked) {
      const next = Math.max(1, Number((px.get(symbol) + (rand() - 0.5) * 1.4).toFixed(4)));
      px.set(symbol, next);
      const quote = {
        symbol,
        tick,
        px: next,
        qty: 100 * (1 + Math.floor(rand() * 40)),
        side: rand() < 0.5 ? 'bid' : 'ask',
      };
      messages.push({
        global: messages.length + 1,
        ...quote,
        ts: tick * intervalMs,
        fp: contentFp(quote),
      });
    }
  }
  const prefill = Math.max(10, Math.round(messages.length * 0.2));
  return {
    messages,
    total: messages.length,
    prefill,
    intervalMs,
    ticks,
    availableAt(global) {
      return global <= prefill ? 0 : (global - prefill) * intervalMs;
    },
  };
}

export function quoteOf(msg) {
  return { symbol: msg.symbol, tick: msg.tick, px: msg.px, qty: msg.qty, side: msg.side };
}

// ------------------------------------------------------------------- presets

export const PRESET_ORDER = ['steady', 'chaos'];

export function preset(kind) {
  const table = {
    steady: {
      kind: 'steady',
      title: '平稳接入',
      blurb: '单路长连接、无故障、无补取：用于建立“只有接收与落盘”的基线。',
      seed: 11,
      ticks: 110,
    },
    chaos: {
      kind: 'chaos',
      title: '重连 · 补取 · 裁决',
      blurb:
        '旧连接沉默后迟到补发，新连接以重置序号接管；新连接前两帧丢失；旧连接尾部与新连接在一条数据上冲突；' +
        '后续丢帧由补取器按序号补齐；补取第一次中断、第二次中途重编号；落盘器中途崩溃。',
      seed: 29,
      ticks: 120,
      faults: {
        tailCount: 6,
        tailDelayMs: 360,
        dropNoticeDelayMs: 520,
        lateCloseMs: 1700,
        conflictTailOffset: 3, // tail global offset that disagrees with the live line
        dropOffsets: [
          [0, 1],
          [8, 13],
        ],
        replay: {
          failAttempt: 1,
          failAfterFrames: 2,
          renumberAttempt: 2,
          renumberAfterFrames: 2,
        },
        autoCrashMs: 4200,
      },
    },
  };
  const p = table[kind];
  if (!p) throw new Error(`unknown preset ${kind}`);
  return p;
}

/**
 * Resolve the fault plan against the concrete feed length. Everything is
 * expressed in global numbers; wall-clock times are derived so the demo is
 * reproducible regardless of pacing.
 */
export function planFor(kind) {
  const spec = preset(kind);
  // FEED_INTERVAL_MS lets tests (and slow demos) change the publish cadence
  // without touching the fixture itself.
  const intervalMs = Number(process.env.FEED_INTERVAL_MS) || undefined;
  const feed = buildFeed({ seed: spec.seed, ticks: spec.ticks, intervalMs });
  const faults = spec.faults || null;
  const total = feed.total;
  // Stall at ~34% of the stream, but always after prefill.
  const stallGlobal = faults ? Math.max(feed.prefill + 6, Math.round(total * 0.34)) : total + 1;
  const stallAt = feed.availableAt(stallGlobal);
  return {
    ...spec,
    feed,
    total,
    prefill: feed.prefill,
    intervalMs: feed.intervalMs,
    stallGlobal,
    hasFaults: !!faults,
    // All wall-clock delays scale with the publish cadence so a fast test run
    // keeps the same story ordering as the default demo pace.
    faultTimes: faults
      ? (() => {
          const k = feed.intervalMs / DESK_TICK_MS;
          return {
            stallAt,
            tailAt: stallAt + faults.tailDelayMs * k,
            dropNoticeAt: stallAt + faults.dropNoticeDelayMs * k,
            closeAt: stallAt + faults.lateCloseMs * k,
            autoCrashMs: faults.autoCrashMs * k,
          };
        })()
      : { stallAt: Infinity, tailAt: Infinity, dropNoticeAt: Infinity, closeAt: Infinity, autoCrashMs: Infinity },
  };
}

// ------------------------------------------------------------------- labels

export function connStateLabel(s) {
  return (
    {
      connecting: '连接中',
      live: '在线',
      suspect: '可疑',
      degraded: '降级',
      retired: '已退休',
      closed: '已关闭',
      complete: '已推完',
      aborted: '会话中断',
      claimed: '已认领',
      requested: '待执行',
      active: '补取中',
      failed: '失败',
      abandoned: '放弃',
      pending: '待裁决',
      decided: '已裁决',
    }[s] || s
  );
}
