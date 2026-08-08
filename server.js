'use strict';

const http = require('http');

const { PORT, TICK_MS, DEVICE_KEY, REV, log } = require('./src/config');
const { tick } = require('./src/store');
const { createApp } = require('./src/httpRoutes');
const { attachWsServer } = require('./src/wsServer');

// The HTTP app needs to push to sockets, but the socket server needs the HTTP server to attach to.
// Hand the app an indirection now and point it at the real broadcast once the ws server exists.
let broadcast = () => {};
const app = createApp((payload) => broadcast(payload));
const server = http.createServer(app);
const ws = attachWsServer(server);
broadcast = ws.broadcast;

// One clock drives everything: advance the simulator (unless a real board is reporting) and push
// the resulting snapshot to every client.
const ticker = setInterval(() => {
  try {
    broadcast(tick());
  } catch (err) {
    log('Tick', `failed (kept running): ${(err && err.stack) || err}`);
  }
}, TICK_MS);

let shuttingDown = false;
function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log('Server', `${signal} received — shutting down`);
  clearInterval(ticker);
  try { server.close(); } catch (e) { /* already closing */ }
  for (const client of ws.wss.clients) {
    try { client.close(1001, 'server_restarting'); } catch (e) { /* already gone */ }
  }
  setTimeout(() => process.exit(0), 250);
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// A stray throw from a timer or a late promise must not take the process down and disconnect every
// client. Log it and keep serving.
process.on('uncaughtException', (err) => log('Fatal', `uncaughtException (kept running): ${(err && err.stack) || err}`));
process.on('unhandledRejection', (reason) => log('Fatal', `unhandledRejection (kept running): ${(reason && reason.stack) || reason}`));

server.listen(PORT, () => {
  log('Server', `esp-server listening on :${PORT} (rev ${REV}, tick ${TICK_MS}ms)`);
  if (!DEVICE_KEY) log('Server', 'WARNING: DEVICE_KEY not set — POST /api/sensors accepts readings from anyone');
});
