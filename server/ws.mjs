// Minimal RFC 6455 WebSocket server: enough for JSON text frames, ping/pong and
// close on one HTTP server. No dependency install needed.
import { EventEmitter } from 'node:events';
import { createHash } from 'node:crypto';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function acceptKey(key) {
  return createHash('sha1').update(key + GUID).digest('base64');
}

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

export class WSConnection extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.readyState = 1; // OPEN
    this.buf = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentOpcode = 0;
    socket.on('data', (chunk) => this.onData(chunk));
    socket.on('close', () => this.markClosed());
    socket.on('error', () => this.markClosed());
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
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;
      if (len === 126) {
        if (this.buf.length < 4) return;
        len = this.buf.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.buf.length < 10) return;
        const big = this.buf.readBigUInt64BE(2);
        if (big > 8n * 1024n * 1024n) {
          this.close(1009);
          return;
        }
        len = Number(big);
        offset = 10;
      }
      const maskLen = masked ? 4 : 0;
      if (this.buf.length < offset + maskLen + len) return;
      let payload = this.buf.subarray(offset + maskLen, offset + maskLen + len);
      if (masked) {
        const mask = this.buf.subarray(offset, offset + 4);
        const copy = Buffer.allocUnsafe(len);
        for (let i = 0; i < len; i++) copy[i] = payload[i] ^ mask[i & 3];
        payload = copy;
      }
      this.buf = this.buf.subarray(offset + maskLen + len);
      this.onFrame(fin, opcode, payload);
    }
  }

  onFrame(fin, opcode, payload) {
    if (opcode === 0x8) {
      this.close(1000);
      return;
    }
    if (opcode === 0x9) {
      this.socket.write(encodeFrame(0xa, payload));
      return;
    }
    if (opcode === 0xa) return;
    if (opcode === 0x0) {
      this.fragments.push(payload);
      if (fin) {
        const full = Buffer.concat(this.fragments);
        this.fragments = [];
        this.emit('message', full.toString('utf8'));
      }
      return;
    }
    if (opcode === 0x1 || opcode === 0x2) {
      if (fin) {
        this.emit('message', payload.toString('utf8'));
      } else {
        this.fragmentOpcode = opcode;
        this.fragments = [payload];
      }
    }
  }

  send(str) {
    if (this.readyState !== 1) return false;
    try {
      this.socket.write(encodeFrame(0x1, Buffer.from(str, 'utf8')));
      return true;
    } catch {
      this.markClosed();
      return false;
    }
  }

  close(code = 1000, reason = '') {
    if (this.readyState !== 1) return;
    const body = Buffer.alloc(2 + Buffer.byteLength(reason));
    body.writeUInt16BE(code, 0);
    body.write(reason, 2);
    try {
      this.socket.write(encodeFrame(0x8, body));
    } catch {
      /* ignore */
    }
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

export class WebSocketServer extends EventEmitter {
  constructor({ server, path = '/feed' }) {
    super();
    this.path = path;
    this.clients = new Set();
    server.on('upgrade', (req, socket) => {
      const url = new URL(req.url, 'http://local');
      if (url.pathname !== this.path) {
        socket.destroy();
        return;
      }
      const key = req.headers['sec-websocket-key'];
      if (!key) {
        socket.destroy();
        return;
      }
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
          'Upgrade: websocket\r\n' +
          'Connection: Upgrade\r\n' +
          `Sec-WebSocket-Accept: ${acceptKey(key)}\r\n\r\n`
      );
      const conn = new WSConnection(socket);
      this.clients.add(conn);
      conn.on('close', () => this.clients.delete(conn));
      this.emit('connection', conn, req);
    });
  }
}
