// Minimal WebSocket client (RFC 6455) for Node, no dependencies.
import { EventEmitter } from 'node:events';
import { createHash, randomBytes } from 'node:crypto';
import http from 'node:http';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  const mask = randomBytes(4);
  if (len < 126) {
    header = Buffer.alloc(6);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | len;
    mask.copy(header, 2);
  } else if (len < 65536) {
    header = Buffer.alloc(8);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
    mask.copy(header, 4);
  } else {
    header = Buffer.alloc(14);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(len), 2);
    mask.copy(header, 10);
  }
  const body = Buffer.allocUnsafe(len);
  for (let i = 0; i < len; i++) body[i] = payload[i] ^ mask[i & 3];
  return Buffer.concat([header, body]);
}

export class WSClient extends EventEmitter {
  constructor(url) {
    super();
    this.url = new URL(url);
    this.readyState = 0; // CONNECTING
    this.buf = Buffer.alloc(0);
    this.fragments = [];
  }

  connect() {
    return new Promise((resolve, reject) => {
      const key = randomBytes(16).toString('base64');
      const req = http.request({
        hostname: this.url.hostname,
        port: this.url.port || 80,
        path: this.url.pathname + this.url.search,
        method: 'GET',
        headers: {
          Connection: 'Upgrade',
          Upgrade: 'websocket',
          'Sec-WebSocket-Version': '13',
          'Sec-WebSocket-Key': key,
        },
      });
      let settled = false;
      req.on('upgrade', (res, socket, head) => {
        const expected = createHash('sha1').update(key + GUID).digest('base64');
        if (res.headers['sec-websocket-accept'] !== expected) {
          socket.destroy();
          const err = new Error('bad websocket accept');
          if (!settled) reject(err);
          return;
        }
        this.socket = socket;
        this.readyState = 1;
        socket.setNoDelay(true);
        socket.on('data', (chunk) => this.onData(chunk));
        socket.on('close', () => this.markClosed());
        socket.on('error', () => this.markClosed());
        if (head && head.length) this.onData(head);
        if (!settled) {
          settled = true;
          resolve(this);
        }
        this.emit('open');
      });
      req.on('response', (res) => {
        const err = new Error(`upgrade refused: ${res.statusCode}`);
        if (!settled) {
          settled = true;
          reject(err);
        }
      });
      req.on('error', (err) => {
        if (!settled) {
          settled = true;
          reject(err);
        }
      });
      req.end();
    });
  }

  markClosed() {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.emit('close');
  }

  onData(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    while (this.buf.length >= 2) {
      const b0 = this.buf[0];
      const b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      let len = b1 & 0x7f;
      let offset = 2;
      if (len === 126) {
        if (this.buf.length < 4) return;
        len = this.buf.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.buf.length < 10) return;
        len = Number(this.buf.readBigUInt64BE(2));
        offset = 10;
      }
      if (this.buf.length < offset + len) return;
      const payload = this.buf.subarray(offset, offset + len);
      this.buf = this.buf.subarray(offset + len);
      if (opcode === 0x8) {
        this.close();
        return;
      }
      if (opcode === 0x9) {
        this.raw(0xa, payload);
        continue;
      }
      if (opcode === 0xa) continue;
      if (opcode === 0x0) {
        this.fragments.push(Buffer.from(payload));
        if (fin) {
          const full = Buffer.concat(this.fragments);
          this.fragments = [];
          this.emit('message', full.toString('utf8'));
        }
        continue;
      }
      if (opcode === 0x1 || opcode === 0x2) {
        if (fin) this.emit('message', payload.toString('utf8'));
        else this.fragments = [Buffer.from(payload)];
      }
    }
  }

  raw(opcode, payload) {
    if (this.readyState !== 1) return;
    try {
      this.socket.write(encodeFrame(opcode, payload));
    } catch {
      this.markClosed();
    }
  }

  send(str) {
    this.raw(0x1, Buffer.from(str, 'utf8'));
  }

  close() {
    if (this.readyState !== 1) return;
    const body = Buffer.alloc(2);
    body.writeUInt16BE(1000, 0);
    this.raw(0x8, body);
    this.readyState = 2;
    setTimeout(() => {
      try {
        this.socket.end();
      } catch {
        /* ignore */
      }
      this.markClosed();
    }, 20);
  }
}
