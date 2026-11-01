'use strict';
/* Servidor central de Rodando: emparejamiento, salas (hasta 7) y relevo de entradas por WebSocket.
   Sin dependencias: solo Node 18+.  Inicio:  node server.js   (usa la variable PORT si existe) */
const http = require('http'), crypto = require('crypto');
const PORT = +process.env.PORT || 8080, MAXP = 7, FRAME_MS = 1000 / 60, MAX_CONNS = 400;
const rooms = new Map();
let nextRoom = 1, conns = 0;

function wsFrame(op, payload) {
  const n = payload.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, payload]);
}

class Conn {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.frags = []; this.fop = 0;
    this.room = null; this.pid = -1; this.rtt = 0; this.alive = true; this.seen = Date.now();
    sock.setNoDelay(true); sock.setKeepAlive(true, 20000);
    sock.on('data', (d) => this.onData(d));
    sock.on('close', () => this.close());
    sock.on('error', () => this.close());
  }
  write(b) {
    if (!this.alive || this.sock.destroyed) return;
    if (this.sock.writableLength > 2 * 1024 * 1024) { this.sock.destroy(); return; }   // cliente lentísimo: se corta
    this.sock.write(b);
  }
  text(o) { this.write(wsFrame(1, Buffer.from(JSON.stringify(o)))); }
  onData(d) {
    this.seen = Date.now();
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = b[0] & 128, op = b[0] & 15, masked = b[1] & 128;
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { this.sock.destroy(); return; }
      if (b.length < off + (masked ? 4 : 0) + len) return;
      const mask = masked ? b.slice(off, off + 4) : null; off += masked ? 4 : 0;
      const p = Buffer.from(b.slice(off, off + len));
      if (mask) for (let i = 0; i < p.length; i++) p[i] ^= mask[i & 3];
      this.buf = b.slice(off + len);
      if (op === 8) { this.sock.end(wsFrame(8, Buffer.alloc(0))); return; }
      if (op === 9) { this.write(wsFrame(10, p)); continue; }
      if (op === 10) continue;
      if (op === 0) { this.frags.push(p); if (fin) { const full = Buffer.concat(this.frags); this.frags = []; this.handle(this.fop, full); } continue; }
      if (!fin) { this.fop = op; this.frags = [p]; continue; }
      this.handle(op, p);
    }
  }
  handle(op, p) {
    if (op === 2) {                       // binario = entradas de juego: se reenvían a los demás de la sala
      const r = this.room; if (!r || !r.started) return;
      const fr = wsFrame(2, p);
      r.members.forEach((c) => { if (c !== this) c.write(fr); });
      return;
    }
    if (op !== 1) return;
    let m; try { m = JSON.parse(p.toString()); } catch (e) { return; }
    switch (m.t) {
      case 'find': join(this); break;
      case 'go': if (this.room && !this.room.started && this.room.members.size >= 2) startRoom(this.room); break;
      case 'p': {
        this.rtt = +m.r || 0;
        let mx = 0; if (this.room) this.room.members.forEach((c) => { if (c !== this && c.rtt > mx) mx = c.rtt; });
        this.text({ t: 'o', c: m.c, h: Date.now(), m: mx });
        break;
      }
      case 'ctl': {
        const r = this.room; if (!r || !m.m) return;
        const out = JSON.stringify({ t: 'ctl', from: this.pid, m: m.m });
        const fr = wsFrame(1, Buffer.from(out));
        if (m.to === undefined || m.to === null) r.members.forEach((c) => { if (c !== this) c.write(fr); });
        else if (m.to === 'leader') { const l = leaderOf(r); if (l && l !== this) l.write(fr); }
        else { const c = r.members.get(+m.to); if (c && c !== this) c.write(fr); }
        break;
      }
      case 'leave': this.sock.end(wsFrame(8, Buffer.alloc(0))); break;
    }
  }
  close() {
    if (!this.alive) return; this.alive = false; conns--;
    try { this.sock.destroy(); } catch (e) {}
    const r = this.room; if (!r) return;
    const wasLeader = leaderOf(r) === this;
    r.members.delete(this.pid); this.room = null;
    if (r.members.size === 0) { rooms.delete(r.id); return; }
    if (!r.started) { roster(r); return; }
    const f = Math.max(0, Math.floor((Date.now() - r.t0) / FRAME_MS)) + 10;      // el reloj del servidor decide cuándo "se fue"
    const gone = JSON.stringify({ t: 'ctl', from: -1, m: { t: 'gone', pid: this.pid, f } });
    const l = leaderOf(r);
    r.members.forEach((c) => { c.write(wsFrame(1, Buffer.from(gone))); if (wasLeader) c.text({ t: 'leader', leader: l.pid }); });
  }
}

function leaderOf(r) { let best = null; r.members.forEach((c) => { if (!best || c.pid < best.pid) best = c; }); return best; }
function pids(r) { return Array.from(r.members.keys()).sort((a, b) => a - b); }
function roster(r) { const l = leaderOf(r); r.members.forEach((c) => c.text({ t: 'roster', pids: pids(r), leader: l ? l.pid : 0 })); }
function join(c) {
  if (c.room) return;
  let room = null;
  for (const r of rooms.values()) if (!r.started && r.members.size < MAXP) { room = r; break; }
  if (!room) { room = { id: nextRoom++, members: new Map(), started: false, t0: 0 }; rooms.set(room.id, room); }
  let pid = 0; while (room.members.has(pid)) pid++;
  c.room = room; c.pid = pid; room.members.set(pid, c);
  c.text({ t: 'welcome', pid, room: room.id });
  roster(room);
}
function startRoom(r) {
  r.started = true; r.t0 = Date.now() + 3600;
  const l = leaderOf(r);
  r.members.forEach((c) => c.text({ t: 'start', pids: pids(r), t0: r.t0, leader: l.pid }));
}

const server = http.createServer((q, s) => {
  if (q.url.startsWith('/status')) { s.setHeader('Content-Type', 'application/json'); s.end(JSON.stringify({ ok: true, rooms: rooms.size, players: conns })); return; }
  s.setHeader('Content-Type', 'text/plain; charset=utf-8'); s.end('Rodando servidor OK\n');
});
server.on('upgrade', (req, sock) => {
  if (!req.url.startsWith('/ws') || conns >= MAX_CONNS || !req.headers['sec-websocket-key']) { sock.destroy(); return; }
  const key = req.headers['sec-websocket-key'];
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' +
    crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64') + '\r\n\r\n');
  conns++; new Conn(sock);
});
setInterval(() => { /* corta conexiones muertas (sin datos en 25 s) */
  const now = Date.now();
  rooms.forEach((r) => r.members.forEach((c) => { if (now - c.seen > 25000) c.sock.destroy(); }));
}, 5000);
server.listen(PORT, '0.0.0.0', () => console.log('Rodando servidor escuchando en :' + PORT));
module.exports = { server };
