'use strict';

const { WebSocketServer, WebSocket } = require('ws');

const { HEARTBEAT_MS, log, warn } = require('./config');
const { snapshot } = require('./store');

function attachWsServer(server) {
  const wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    // Клиента подписываем по адресу и User-Agent: так в логе сразу видно, это Unity, браузерный
    // дашборд или чей-то curl.
    const ip = (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';
    ws.label = `${ip} "${(req.headers['user-agent'] || 'no-ua').slice(0, 60)}"`;
    ws.connectedAt = Date.now();

    // Send the current state immediately so a fresh client renders real numbers instead of
    // dashes for up to a full tick.
    send(ws, snapshot());
    log('WS', `+ connected ${ws.label} — ${wss.clients.size} client(s)`);

    ws.on('close', (code, reason) => {
      const heldFor = Math.round((Date.now() - ws.connectedAt) / 1000);
      log('WS', `- gone ${ws.label} after ${heldFor}s (code ${code}${reason && reason.length ? ` ${reason}` : ''}) — ${wss.clients.size} left`);
    });
    ws.on('error', (err) => warn('WS', `socket error ${ws.label}: ${err.message}`));
  });

  wss.on('error', (err) => warn('WS', `server error: ${err.message}`));

  // Render's proxy can drop a connection without ever delivering a close frame, which leaves a
  // zombie in wss.clients that we would keep serializing snapshots for. Ping and reap.
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        warn('WS', `reaping unresponsive client ${ws.label} — missed heartbeat`);
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      try { ws.ping(); } catch (e) { /* terminated between the check and the ping */ }
    }
  }, HEARTBEAT_MS);
  heartbeat.unref();

  function broadcast(payload) {
    if (!wss.clients.size) return;
    const frame = JSON.stringify(payload); // serialize once, not once per client
    for (const ws of wss.clients) {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.send(frame); } catch (e) { /* closing mid-broadcast; the reaper will clean up */ }
      }
    }
  }

  return { wss, broadcast };
}

function send(ws, payload) {
  try { ws.send(JSON.stringify(payload)); } catch (e) { /* closed before the first frame */ }
}

module.exports = { attachWsServer };
