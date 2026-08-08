'use strict';

const path = require('path');
const express = require('express');

const { DEVICE_KEY, REV, log } = require('./config');
const { state, snapshot, ingest, deviceIsLive } = require('./store');

// Guard for the one route a device writes through. An unset DEVICE_KEY leaves it open, which is
// deliberate for local bring-up — the startup log warns loudly when that happens in production.
function requireDeviceKey(req, res, next) {
  if (!DEVICE_KEY) return next();
  if (req.headers['x-device-key'] === DEVICE_KEY) return next();
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

  // Recent snapshots, oldest first — for charts later.
  app.get('/api/history', (req, res) => res.json({ history: state.history }));

  // Where the ESP8266 will POST. Pushing straight to the sockets (rather than waiting for the next
  // tick) keeps Unity's latency at the device's send interval instead of interval + tick.
  app.post('/api/sensors', requireDeviceKey, express.json({ limit: '16kb' }), (req, res) => {
    const applied = ingest(req.body);
    if (!applied.length) return res.status(400).json({ error: 'no_known_sensor_values' });
    broadcast(snapshot());
    res.json({ ok: true, applied });
  });

  app.use(express.static(path.join(__dirname, '..', 'public')));

  app.use((err, req, res, next) => {
    if (err && err.type === 'entity.parse.failed') return res.status(400).json({ error: 'invalid_json' });
    log('HTTP', `unhandled error: ${(err && err.stack) || err}`);
    return res.status(500).json({ error: 'internal' });
  });

  return app;
}

module.exports = { createApp };
