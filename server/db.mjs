// SQLite helpers shared by every process. Each store is a separate file on
// purpose: identity, order and landing progress fail independently.
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';

export function openDb(path, { readonly = false } = {}) {
  mkdirSync(dirname(path), { recursive: true });
  const db = new DatabaseSync(path, { readOnly: readonly });
  db.exec('PRAGMA busy_timeout = 5000');
  if (!readonly) {
    db.exec('PRAGMA journal_mode = WAL');
    db.exec('PRAGMA synchronous = NORMAL');
  }
  return db;
}

export function tx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const out = fn();
    db.exec('COMMIT');
    return out;
  } catch (err) {
    try {
      db.exec('ROLLBACK');
    } catch {
      /* already rolled back */
    }
    throw err;
  }
}

export function insertEvent(db, { actor, type, global = null, connId = null, detail = null }) {
  db.prepare(
    'INSERT INTO events (at, actor, type, global, conn_id, detail) VALUES (?, ?, ?, ?, ?, ?)'
  ).run(Date.now(), actor, type, global, connId, detail == null ? null : JSON.stringify(detail));
}

export function initEvents(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS events (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      at INTEGER NOT NULL,
      actor TEXT NOT NULL,
      type TEXT NOT NULL,
      global INTEGER,
      conn_id TEXT,
      detail TEXT
    );
    CREATE INDEX IF NOT EXISTS events_at ON events(at);
    CREATE INDEX IF NOT EXISTS events_global ON events(global);
  `);
}

export function now() {
  return Date.now();
}

export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export async function httpJson(url, opts = {}) {
  const res = await fetch(url, {
    ...opts,
    headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`${opts.method || 'GET'} ${url} -> ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

export function readJson(req) {
  return new Promise((resolve, reject) => {
    let buf = '';
    req.on('data', (c) => {
      buf += c;
      if (buf.length > 4_000_000) reject(new Error('body too large'));
    });
    req.on('end', () => {
      try {
        resolve(buf ? JSON.parse(buf) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

export async function postJson(url, body = {}) {
  return httpJson(url, { method: 'POST', body: JSON.stringify(body) });
}

export function runId() {
  return createHash('sha1').update(String(Date.now()) + Math.random()).digest('hex').slice(0, 10);
}


