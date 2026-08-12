'use strict';

const http = require('http');

const {
  PORT, TICK_MS, DEVICE_KEY, DEVICE_TIMEOUT_MS, HEARTBEAT_MS,
  HISTORY_WINDOW_MS, HISTORY_EVERY_MS, STATUS_EVERY_MS, REV, log, warn,
} = require('./src/config');
const { CATALOG } = require('./src/catalog');
const { tick, summary } = require('./src/store');
const { createApp } = require('./src/httpRoutes');
const { attachWsServer } = require('./src/wsServer');

// The HTTP app needs to push to sockets, but the socket server needs the HTTP server to attach to.
// Hand the app an indirection now and point it at the real broadcast once the ws server exists.
let broadcast = () => {};
const app = createApp((payload) => broadcast(payload));
const server = http.createServer(app);
const ws = attachWsServer(server);
broadcast = ws.broadcast;

// Один таймер на всё: собрать текущее состояние и разослать его клиентам.
const ticker = setInterval(() => {
  try {
    broadcast(tick());
  } catch (err) {
    warn('Tick', `failed (kept running): ${(err && err.stack) || err}`);
  }
}, TICK_MS);

// На Render единственное окно в работающий сервер — поток логов. Без периодической строки
// невозможно отличить «всё хорошо, просто тихо» от «процесс висит».
const statusTimer = setInterval(() => {
  log('Status', summary(ws.wss.clients.size));
}, STATUS_EVERY_MS);
statusTimer.unref();

let shuttingDown = false;
function gracefulShutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log('Server', `${signal} received — closing ${ws.wss.clients.size} client(s) and shutting down`);
  clearInterval(ticker);
  clearInterval(statusTimer);
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
process.on('uncaughtException', (err) => warn('Fatal', `uncaughtException (kept running): ${(err && err.stack) || err}`));
process.on('unhandledRejection', (reason) => warn('Fatal', `unhandledRejection (kept running): ${(reason && reason.stack) || reason}`));

server.listen(PORT, () => {
  // Полный дамп конфига на старте: почти каждый «почему оно себя так ведёт» на Render — это
  // переменная окружения, выставленная не так, как ожидалось.
  log('Server', `esp-server up — rev=${REV} port=${PORT} node=${process.version}`);
  log('Server', `config tick=${TICK_MS}ms deviceTimeout=${DEVICE_TIMEOUT_MS}ms heartbeat=${HEARTBEAT_MS}ms status=${STATUS_EVERY_MS}ms`);
  const step = Math.max(HISTORY_EVERY_MS, TICK_MS);
  log('Server', `history window=${HISTORY_WINDOW_MS / 60000}min every=${step / 1000}s -> up to ${Math.floor(HISTORY_WINDOW_MS / step)} points per sensor`);
  // Историю пишет тик, так что просьба семплировать чаще тика тихо не исполнится.
  if (HISTORY_EVERY_MS < TICK_MS) {
    warn('Server', `HISTORY_EVERY_MS=${HISTORY_EVERY_MS}ms is finer than TICK_MS=${TICK_MS}ms — history samples at ${step}ms`);
  }
  log('Server', `sensors (${CATALOG.length}): ${CATALOG.map((s) => `${s.id}[${s.unit || 'bool'}]`).join(' ')}`);
  log('Server', 'routes: GET /health /api/sensors /api/history · POST /api/sensors · WS /ws · dashboard /');
  if (DEVICE_KEY) log('Server', 'DEVICE_KEY set — POST /api/sensors requires the X-Device-Key header');
  else warn('Server', 'DEVICE_KEY not set — POST /api/sensors accepts readings from anyone');
  // Никаких выдуманных цифр: до первого POST снапшот уходит с нулями и source=none, история пуста.
  log('Server', 'waiting for a board; until one reports, readings are zeros (source=none)');
});
