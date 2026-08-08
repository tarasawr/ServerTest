'use strict';

const path = require('path');
const express = require('express');

const { DEVICE_KEY, REV, warn } = require('./config');
const { snapshot, ingest, deviceIsLive, historyPayload } = require('./store');

// Плата за NAT-ом, поэтому в логе полезнее видеть внешний адрес, который проставил прокси Render,
// чем адрес самого прокси.
function clientIp(req) {
  return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';
}

// Guard for the one route a device writes through. An unset DEVICE_KEY leaves it open, which is
// deliberate for local bring-up — the startup log warns loudly when that happens in production.
function requireDeviceKey(req, res, next) {
  if (!DEVICE_KEY) return next();
  if (req.headers['x-device-key'] === DEVICE_KEY) return next();
  // Молчаливый 401 — худший вариант для отладки прошивки: плата шлёт, сервер отвечает, а понять,
  // что дело в ключе, неоткуда.
  const got = req.headers['x-device-key'];
  warn('Device', `POST rejected from ${clientIp(req)} — ${got ? 'wrong' : 'missing'} X-Device-Key`);
  return res.status(401).json({ error: 'bad_device_key' });
}

function createApp(broadcast) {
  const app = express();

  // Unity on desktop/mobile does not need CORS, but the browser dashboard and curl-from-anywhere
  // debugging do, and there is nothing secret behind these reads.
  app.use((req, res, next) => {
    res.set('Access-Control-Allow-Origin', '*');
    res.set('Access-Control-Allow-Headers', 'Content-Type, X-Device-Key');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
  });

  // Liveness for Render's health check — status code is what matters, body is for humans.
  app.get('/health', (req, res) => {
    res.type('text/plain').send(`ok rev=${REV} source=${deviceIsLive() ? 'device' : 'fake'}`);
  });

  // The same payload the WebSocket pushes. Handy for curl, and a fallback if a client cannot
  // hold a socket open.
  app.get('/api/sensors', (req, res) => res.json(snapshot()));

  // Последний час, старое первым — бэкфилл для графика. ?ids=a,b обрезает до нужных рядов, чтобы
  // клиент не тянул все датчики ради двух линий.
  app.get('/api/history', (req, res) => {
    const ids = String(req.query.ids || '').split(',').map((s) => s.trim()).filter(Boolean);
    res.json(historyPayload(ids));
  });

  // Where the ESP8266 will POST. Pushing straight to the sockets (rather than waiting for the next
  // tick) keeps Unity's latency at the device's send interval instead of interval + tick.
  app.post('/api/sensors', requireDeviceKey, express.json({ limit: '16kb' }), (req, res) => {
    const applied = ingest(req.body);
    if (!applied.length) {
      // Тело показываем целиком: это единственная зацепка, когда прошивка шлёт не тот JSON.
      warn('Device', `POST from ${clientIp(req)} carried no known sensor values — body: ${JSON.stringify(req.body).slice(0, 300)}`);
      return res.status(400).json({ error: 'no_known_sensor_values' });
    }
    broadcast(snapshot());
    res.json({ ok: true, applied });
  });

  app.use(express.static(path.join(__dirname, '..', 'public')));

  app.use((err, req, res, next) => {
    if (err && err.type === 'entity.parse.failed') {
      warn('HTTP', `invalid JSON from ${clientIp(req)} on ${req.method} ${req.path}: ${err.message}`);
      return res.status(400).json({ error: 'invalid_json' });
    }
    warn('HTTP', `unhandled error on ${req.method} ${req.path}: ${(err && err.stack) || err}`);
    return res.status(500).json({ error: 'internal' });
  });

  return app;
}

module.exports = { createApp };
