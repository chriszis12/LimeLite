import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUB = path.join(__dirname, 'public');
const PORT = +process.env.PORT || 8080;
const MAX_CLIENTS = +process.env.MAX_CLIENTS || 400;
const ID_RE = /^limelite-yard-[A-Z0-9]{4,10}$/;
const TYPES = { '.html': 'text/html; charset=utf-8', '.json': 'application/json', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.mp3': 'audio/mpeg', '.ico': 'image/x-icon' };

// ---- static files, compressed once and cached in memory ----
const cache = new Map();
function load(file) {
  const st = fs.statSync(file);
  const c = cache.get(file);
  if (c && c.mtime === st.mtimeMs) return c;
  const raw = fs.readFileSync(file);
  const ext = path.extname(file);
  const comp = /\.(html|json|js|css)$/.test(ext);
  const e = {
    mtime: st.mtimeMs, raw,
    br: comp ? zlib.brotliCompressSync(raw, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 9 } }) : null,
    gz: comp ? zlib.gzipSync(raw, { level: 9 }) : null,
    etag: '"' + crypto.createHash('sha1').update(raw).digest('hex').slice(0, 16) + '"',
    type: TYPES[ext] || 'application/octet-stream',
  };
  cache.set(file, e);
  return e;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
  let p = decodeURIComponent(url.pathname);
  if (p.endsWith('/')) p += 'index.html';
  const file = path.normalize(path.join(PUB, p));
  if (!file.startsWith(PUB + path.sep) || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); return res.end('not found'); }
  const e = load(file);
  const h = { 'content-type': e.type, etag: e.etag, vary: 'accept-encoding',
    'cache-control': url.searchParams.has('v') ? 'public, max-age=31536000, immutable' : 'no-cache' };
  if (req.headers['if-none-match'] === e.etag) { res.writeHead(304, h); return res.end(); }
  const ae = req.headers['accept-encoding'] || '';
  let body = e.raw;
  if (e.br && /\bbr\b/.test(ae)) { h['content-encoding'] = 'br'; body = e.br; }
  else if (e.gz && /\bgzip\b/.test(ae)) { h['content-encoding'] = 'gzip'; body = e.gz; }
  h['content-length'] = body.length;
  res.writeHead(200, h);
  res.end(req.method === 'HEAD' ? undefined : body);
});

// ---- switchboard: routes messages between registered ids, nothing else ----
const wss = new WebSocketServer({ noServer: true, maxPayload: 128 * 1024 });
const peers = new Map();   // id -> ws
const links = new Map();   // cid -> {a,b}  (ids)

server.on('upgrade', (req, sock, head) => {
  if (new URL(req.url, 'http://x').pathname !== '/ws' || wss.clients.size >= MAX_CLIENTS) { sock.destroy(); return; }
  wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req));
});

const send = (ws, m) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); };

function dropLinks(id) {
  for (const [cid, l] of links) {
    if (l.a !== id && l.b !== id) continue;
    links.delete(cid);
    const other = l.a === id ? l.b : l.a;
    send(peers.get(other), { k: 'x', from: id, cid });
  }
}

wss.on('connection', ws => {
  ws.alive = true; ws.id = null; ws.tokens = 200; ws.last = Date.now();
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    ws.alive = true;
    // token bucket: ~120 msg/s sustained, 200 burst
    const now = Date.now();
    ws.tokens = Math.min(200, ws.tokens + (now - ws.last) * 0.12); ws.last = now;
    if (--ws.tokens < 0) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.k !== 'string') return;
    if (m.k === 'p') return;
    if (m.k === 'reg') {
      if (ws.id || typeof m.id !== 'string' || !ID_RE.test(m.id)) return ws.close();
      const old = peers.get(m.id);
      if (old && old.readyState === 1 && old.alive) return send(ws, { k: 'taken' });
      if (old) { old.id = null; old.terminate(); }
      ws.id = m.id; peers.set(m.id, ws);
      return send(ws, { k: 'ok' });
    }
    if (!ws.id) return;
    const cid = typeof m.cid === 'string' ? m.cid.slice(0, 32) : '';
    if (m.k === 'c') {
      const t = typeof m.to === 'string' && m.to !== ws.id && peers.get(m.to);
      if (!t) return send(ws, { k: 'nf', cid });
      links.set(cid, { a: ws.id, b: m.to });
      return send(t, { k: 'c', from: ws.id, cid });
    }
    if (m.k === 'a' || m.k === 'd' || m.k === 'x') {
      const l = links.get(cid);
      if (!l || (l.a !== ws.id && l.b !== ws.id)) return;
      const other = l.a === ws.id ? l.b : l.a;
      if (m.k === 'x') links.delete(cid);
      send(peers.get(other), { k: m.k, from: ws.id, cid, d: m.d });
    }
  });
  ws.on('close', () => {
    if (!ws.id) return;
    if (peers.get(ws.id) === ws) { peers.delete(ws.id); dropLinks(ws.id); }
  });
  ws.on('error', () => {});
});

// heartbeat: kill dead sockets so ids free up
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; ws.ping();
  }
}, 25000).unref();

server.listen(PORT, '0.0.0.0', () => console.log('limelite on :' + PORT));
