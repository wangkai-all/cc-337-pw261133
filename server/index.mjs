// Supervisor + control API + static host.
//
// Four OS processes, each with its own SQLite file and its own HTTP port:
//   source      (fixture supplier, WebSocket /feed)
//   receiver    identity + order + coverage + arbitration  -> receiver.db
//   backfiller  gap -> numbered replay sessions            -> backfill.db
//   lander      durable landing + recovery                 -> lander.db
//
// They are separate so that pausing, crashing or restarting one is observable
// on the page without the others resetting.
import http from 'node:http';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { mkdirSync, rmSync, existsSync, readFileSync } from 'node:fs';
import { openDb, readJson, postJson, httpJson } from './db.mjs';
import { attachSource, MarketSource } from './source.mjs';
import { planFor, POLICIES } from './shared.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const PUBLIC = path.join(ROOT, 'public');
// DATA_DIR lets tests run a desk in a temp directory instead of ./data.
const DATA = path.resolve(process.env.DATA_DIR || path.join(ROOT, 'data'));
const log = (...a) => console.log('[desk]', ...a);

const PORT_SOURCE = Number(process.env.SOURCE_PORT || 8899);
const PORT_CONTROL = Number(process.env.PORT || 8787);

class Supervisor {
  constructor() {
    this.source = new MarketSource({ presetKind: 'steady' });
    this.props = {}; // child name -> {proc, port, restarts}
    this.runId = null;
    this.policy = 'overlap';
  }

  async start({ presetKind = 'steady' } = {}) {
    mkdirSync(DATA, { recursive: true });
    if (presetKind !== 'steady') this.source = new MarketSource({ presetKind });
    // source runs in-process: it is the fixture, not part of what we inspect
    const server = http.createServer((req, res) => this.handleSourceHttp(req, res));
    this.sourceServer = server;
    attachSource(server, this.source);
    await new Promise((r) => server.listen(PORT_SOURCE, '127.0.0.1', r));
    this.source.start();
    log('source listening', PORT_SOURCE);
    await this.resetRun(presetKind, { quiet: true });
  }

  /** Restart the web control plane with a different preset in place. */
  async choosePreset(presetKind) {
    return this.resetRun(presetKind);
  }

  handleSourceHttp(req, res) {
    const url = new URL(req.url, 'http://local');
    const reply = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    (async () => {
      if (url.pathname === '/health') return reply(200, { ok: true });
      if (url.pathname === '/manifest') return reply(200, this.source.manifest());
      if (url.pathname === '/summary') return reply(200, this.source.summary());
      if (req.method === 'POST') {
        const body = await readJson(req);
        if (url.pathname === '/trigger') return reply(200, this.source.trigger(body.kind, body));
        if (url.pathname === '/publish') {
          this.source.publishNow(Number(body.to));
          return reply(200, { ok: true, published: this.source.published });
        }
      }
      reply(404, { error: 'not-found' });
    })().catch((err) => reply(500, { error: err.message }));
  }

  // ------------------------------------------------------------- child procs

  spawnChild(name, script, env) {
    const proc = spawn(process.execPath, [path.join(__dirname, script)], {
      env: { ...process.env, ...env },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    });
    const rec = { proc, name, port: null, restarts: 0, exited: null };
    this.props[name] = rec;
    proc.stderr.on('data', (d) => process.stderr.write(`[${name}] ${d}`));
    proc.stdout.on('data', (d) => process.stdout.write(`[${name}] ${d}`));
    proc.on('message', (msg) => {
      if (msg?.type === 'ready') rec.port = msg.port;
    });
    proc.on('exit', (code, signal) => {
      rec.exited = { code, signal, at: Date.now() };
      log(`child ${name} exited`, code, signal);
      // A crashed lander is restarted automatically so the recovery is
      // observable without an operator. The receiver and backfiller are not
      // auto-restarted: pausing them is a deliberate desk action.
      if (name === 'lander' && !this.resetting) {
        setTimeout(() => {
          if (this.props.lander === rec) this.restartLanderAfterCrash().catch((e) => log('lander restart failed', e.message));
        }, 300);
      }
    });
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`${name} did not start`)), 8000);
      const iv = setInterval(() => {
        if (rec.port) {
          clearInterval(iv);
          clearTimeout(t);
          resolve(rec.port);
        }
      }, 30);
    });
  }

  async killChild(name) {
    const rec = this.props[name];
    if (!rec?.proc) return;
    rec.proc.removeAllListeners('exit');
    rec.proc.kill('SIGKILL');
    await new Promise((r) => setTimeout(r, 120));
    this.props[name] = null;
  }

  async startReceiver(total) {
    const port = await this.spawnChild('receiver', 'receiver.mjs', {
      PORT: '0',
      DATA_DIR: DATA,
      SOURCE_URL: `http://127.0.0.1:${PORT_SOURCE}`,
      POLICY: this.policy,
    });
    const receiverUrl = `http://127.0.0.1:${port}`;
    await postJson(`${receiverUrl}/start`, {
      sourceUrl: `http://127.0.0.1:${PORT_SOURCE}`,
      total,
    });
    return port;
  }

  async startBackfiller(receiverUrl) {
    const port = await this.spawnChild('backfiller', 'backfiller.mjs', {
      PORT: '0',
      DATA_DIR: DATA,
      RECEIVER_URL: receiverUrl,
      SOURCE_URL: `http://127.0.0.1:${PORT_SOURCE}`,
    });
    const backfillUrl = `http://127.0.0.1:${port}`;
    await postJson(`${backfillUrl}/start`, {});
    return port;
  }

  async startLander(receiverUrl, { keepData = true } = {}) {
    if (!keepData) {
      for (const f of ['lander.db', 'lander.db-wal', 'lander.db-shm']) {
        const p = path.join(DATA, f);
        if (existsSync(p)) rmSync(p, { force: true });
      }
    }
    const port = await this.spawnChild('lander', 'lander.mjs', {
      PORT: '0',
      DATA_DIR: DATA,
      RECEIVER_URL: receiverUrl,
    });
    const landerUrl = `http://127.0.0.1:${port}`;
    await postJson(`${landerUrl}/start`, {});
    return port;
  }

  /** Restarts a crashed lander on the same DB file, which triggers recovery. */
  async restartLanderAfterCrash() {
    const rec = this.props.lander;
    if (!rec || !rec.exited) return { ok: false, reason: 'lander-not-exited' };
    const receiverUrl = this.receiverUrl();
    if (!receiverUrl) return { ok: false, reason: 'receiver-down' };
    log('auto-restarting lander after exit', rec.exited);
    const port = await this.startLander(receiverUrl);
    this.lastLanderRecovery = { at: Date.now(), exit: rec.exited, newPort: port };
    return { ok: true, port };
  }

  // ------------------------------------------------------------------- runs

  async resetRun(presetKind, { quiet = false, policy = null } = {}) {
    log('resetting run', presetKind);
    if (policy && POLICIES.includes(policy)) this.policy = policy;
    this.resetting = true;
    await Promise.all(['receiver', 'backfiller', 'lander'].map((n) => this.killChild(n)));
    // stop old source clock and make a new one for the preset
    this.source.stop();
    this.source = new MarketSource({ presetKind });
    // swap the connection handler onto the same HTTP server
    const server = this.sourceServer;
    server.removeAllListeners('upgrade');
    attachSource(server, this.source);
    this.source.start();
    for (const f of ['receiver.db', 'receiver.db-wal', 'receiver.db-shm', 'backfill.db', 'backfill.db-wal', 'backfill.db-shm', 'lander.db', 'lander.db-wal', 'lander.db-shm']) {
      const p = path.join(DATA, f);
      if (existsSync(p)) rmSync(p, { force: true });
    }
    const plan = planFor(presetKind);
    const receiverPort = await this.startReceiver(plan.total);
    const receiverUrl = `http://127.0.0.1:${receiverPort}`;
    const backfillPort = await this.startBackfiller(receiverUrl);
    const landerPort = await this.startLander(receiverUrl);
    this.runId = `run-${Date.now().toString(36)}`;
    this.presetKind = presetKind;
    this.plan = plan;
    this.ports = { receiver: receiverPort, backfiller: backfillPort, lander: landerPort };
    this.resetting = false;
    if (this.plan.hasFaults && this.plan.faultTimes.autoCrashMs !== Infinity) {
      // Scripted chaos run: the lander is armed once, at the time the preset
      // promises, so a fresh page load finds a recovery in the ledger without
      // anyone clicking a button. The page can still arm its own crash later.
      clearTimeout(this.autoCrashTimer);
      this.autoCrashTimer = setTimeout(() => {
        if (this.presetKind !== presetKind || !this.props.lander?.port) return;
        postJson(`http://127.0.0.1:${this.props.lander.port}/crash`, { mode: 'mid' })
          .then(() => log('auto-armed lander crash (mid)'))
          .catch((e) => log('auto crash arm failed', e.message));
      }, Math.max(400, this.plan.faultTimes.autoCrashMs));
    }
    if (!quiet) log('run ready', this.ports);
    return this.runStatus();
  }

  runStatus() {
    return {
      runId: this.runId,
      preset: this.presetKind,
      policy: this.policy,
      ports: this.ports || {},
      sourcePort: PORT_SOURCE,
      plan: this.plan
        ? {
            total: this.plan.total,
            prefill: this.plan.prefill,
            intervalMs: this.plan.intervalMs,
            stallGlobal: this.plan.stallGlobal,
            hasFaults: this.plan.hasFaults,
            faultTimes: this.plan.faultTimes,
          }
        : null,
      children: Object.fromEntries(
        Object.entries(this.props)
          .filter(([, v]) => v)
          .map(([k, v]) => [k, { port: v.port, exited: v.exited }])
      ),
    };
  }

  receiverUrl() {
    return this.props.receiver?.port ? `http://127.0.0.1:${this.props.receiver.port}` : null;
  }

  async aggregate() {
    const out = { run: this.runStatus(), source: this.source.summary() };
    const receiverUrl = this.receiverUrl();
    const landerUrl = this.props.lander?.port ? `http://127.0.0.1:${this.props.lander.port}` : null;
    const [receiver, backfiller, lander, landerGlobals, decisions] = await Promise.all([
      receiverUrl ? httpJson(`${receiverUrl}/state`).catch((e) => ({ ok: false, error: e.message })) : null,
      this.props.backfiller?.port
        ? httpJson(`http://127.0.0.1:${this.props.backfiller.port}/state`).catch((e) => ({ ok: false, error: e.message }))
        : null,
      landerUrl ? httpJson(`${landerUrl}/state`).catch((e) => ({ ok: false, error: e.message })) : null,
      landerUrl ? httpJson(`${landerUrl}/globals`).catch(() => ({ globals: [] })) : { globals: [] },
      receiverUrl ? httpJson(`${receiverUrl}/decisions`).catch(() => ({ decisions: [] })) : { decisions: [] },
    ]);
    out.receiver = receiver;
    out.backfiller = backfiller;
    out.lander = lander;
    // The per-global bands on the page need three independent answers for every
    // number: what the receiver accepted, what is on disk, and (below) what the
    // supplier's manifest says it should be.
    out.ledger = {
      decisions: decisions.decisions || [],
      landed: landerGlobals.globals || [],
    };
    return out;
  }

  async overview() {
    const out = await this.aggregate();
    const receiverUrl = this.receiverUrl();
    // The desk-level event stream is the receiver's table: every process posts
    // its events there, so one query gives the merged timeline.
    out.events = receiverUrl
      ? (await httpJson(`${receiverUrl}/events?limit=160`).catch(() => ({ events: [] }))).events
      : [];
    return out;
  }

  async reconcile() {
    const manifest = this.source.manifest();
    if (!this.props.lander?.port) return { ok: false, error: 'lander-down' };
    return httpJson(`${this.props.lander.port ? `http://127.0.0.1:${this.props.lander.port}` : ''}/reconcile`, {
      method: 'POST',
      body: JSON.stringify({ manifest }),
    });
  }

  async trace(global) {
    const receiverUrl = this.receiverUrl();
    const out = { global };
    out.receiver = receiverUrl
      ? await httpJson(`${receiverUrl}/trace/${global}`).catch((e) => ({ error: e.message }))
      : null;
    out.pendingConflict = out.receiver?.conflict;
    if (this.props.lander?.port) {
      out.lander = await httpJson(`http://127.0.0.1:${this.props.lander.port}/global/${global}`).catch((e) => ({ error: e.message }));
    }
    const m = this.source.feed.messages[global - 1];
    out.manifest = m
      ? { global: m.global, fp: m.fp, symbol: m.symbol, tick: m.tick, px: m.px, qty: m.qty, side: m.side }
      : null;
    out.sourceEvents = this.source.eventLog.filter((e) => e.global === global);
    return out;
  }

  async stop() {
    await Promise.all(['receiver', 'backfiller', 'lander'].map((n) => this.killChild(n)));
    this.source.stop();
    this.sourceServer?.close();
  }
}

// ----------------------------------------------------------------- control API

const sup = new Supervisor();
await sup.start({ presetKind: process.env.PRESET === 'chaos' ? 'chaos' : 'steady' });

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://local');
  const reply = (code, obj) => {
    res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify(obj));
  };
  try {
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      return serveFile(res, path.join(PUBLIC, 'index.html'));
    }
    if (req.method === 'GET' && url.pathname.startsWith('/static/')) {
      const p = path.join(PUBLIC, url.pathname.slice('/static/'.length));
      if (!p.startsWith(PUBLIC)) return reply(403, { error: 'forbidden' });
      return serveFile(res, p);
    }
    if (req.method === 'GET' && url.pathname === '/api/overview') return reply(200, await sup.overview());
    if (req.method === 'GET' && url.pathname === '/api/manifest') return reply(200, sup.source.manifest());
    if (req.method === 'GET' && url.pathname === '/api/decisions') {
      const receiverUrl = sup.receiverUrl();
      if (!receiverUrl) return reply(503, { error: 'receiver-down' });
      return reply(200, await httpJson(`${receiverUrl}/decisions`));
    }
    if (req.method === 'GET' && url.pathname === '/api/source') return reply(200, sup.source.summary());
    if (req.method === 'GET' && url.pathname === '/api/reconcile') return reply(200, await sup.reconcile());
    if (req.method === 'GET' && url.pathname.startsWith('/api/trace/')) {
      return reply(200, await sup.trace(Number(url.pathname.slice('/api/trace/'.length))));
    }
    if (req.method === 'POST') {
      const body = await readJson(req);
      const receiverUrl = sup.receiverUrl();
      switch (url.pathname) {
        case '/api/reset':
          return reply(
            200,
            await sup.resetRun(body.preset === 'chaos' ? 'chaos' : 'steady', {
              policy: body.policy && POLICIES.includes(body.policy) ? body.policy : sup.policy,
            })
          );
        case '/api/pause': {
          const paused = !!body.paused;
          const calls = [];
          if (receiverUrl) calls.push(postJson(`${receiverUrl}/pause`, { paused }));
          if (sup.props.backfiller?.port) calls.push(postJson(`http://127.0.0.1:${sup.props.backfiller.port}/pause`, { paused }));
          if (sup.props.lander?.port) calls.push(postJson(`http://127.0.0.1:${sup.props.lander.port}/pause`, { paused }));
          await Promise.allSettled(calls);
          return reply(200, { ok: true, paused, services: calls.length });
        }
        case '/api/policy': {
          if (!POLICIES.includes(body.policy)) return reply(400, { error: 'bad policy' });
          // Supervisor-level too, so the next reset starts with the chosen policy.
          sup.policy = body.policy;
          if (receiverUrl) await postJson(`${receiverUrl}/policy`, { policy: body.policy });
          return reply(200, { ok: true, policy: body.policy });
        }
        case '/api/trigger': {
          const out = sup.source.trigger(body.kind, body);
          return reply(200, { ok: true, ...out });
        }
        case '/api/receiver': {
          if (!receiverUrl) return reply(503, { error: 'receiver-down' });
          const out = await postJson(`${receiverUrl}${body.path}`, body.body || {});
          return reply(200, out);
        }
        case '/api/backfiller': {
          if (!sup.props.backfiller?.port) return reply(503, { error: 'backfiller-down' });
          const out = await postJson(`http://127.0.0.1:${sup.props.backfiller.port}${body.path}`, body.body || {});
          return reply(200, out);
        }
        case '/api/lander': {
          if (!sup.props.lander?.port) return reply(503, { error: 'lander-down' });
          const out = await postJson(`http://127.0.0.1:${sup.props.lander.port}${body.path}`, body.body || {});
          return reply(200, out);
        }
        case '/api/lander/kill': {
          // Simulates an external kill (OOM, operator) with no crash injection.
          const rec = sup.props.lander;
          if (!rec?.proc) return reply(503, { error: 'lander-down' });
          rec.proc.kill('SIGKILL');
          return reply(200, { ok: true, killed: true });
        }
        case '/api/lander/restart': {
          if (!receiverUrl) return reply(503, { error: 'receiver-down' });
          const port = await sup.startLander(receiverUrl);
          return reply(200, { ok: true, port });
        }
        case '/api/reconcile': {
          return reply(200, await sup.reconcile());
        }
        case '/api/settle': {
          if (!receiverUrl) return reply(503, { error: 'receiver-down' });
          return reply(200, await postJson(`${receiverUrl}/resolve`, { by: 'desk' }));
        }
        case '/api/quit':
          reply(200, { ok: true });
          await sup.stop();
          setTimeout(() => process.exit(0), 60);
          return;
        default:
          return reply(404, { error: 'not-found' });
      }
    }
    reply(404, { error: 'not-found' });
  } catch (err) {
    log('api error', err.stack || err.message);
    reply(500, { error: String(err.message || err) });
  }
});

function serveFile(res, file) {
  try {
    const body = readFileSync(file);
    const ext = path.extname(file);
    const types = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'text/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.svg': 'image/svg+xml',
      '.json': 'application/json',
    };
    res.writeHead(200, { 'content-type': types[ext] || 'application/octet-stream', 'cache-control': 'no-store' });
    res.end(body);
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  }
}

await new Promise((r) => server.listen(PORT_CONTROL, '127.0.0.1', r));
log('desk listening on', PORT_CONTROL, '— open http://127.0.0.1:' + PORT_CONTROL);

process.on('SIGINT', async () => {
  await sup.stop();
  process.exit(0);
});
process.on('SIGTERM', async () => {
  await sup.stop();
  process.exit(0);
});
