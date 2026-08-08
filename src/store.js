'use strict';

const { CATALOG, BY_ID, simulate, round, describe } = require('./catalog');
const { DEVICE_TIMEOUT_MS, HISTORY_LEN, log } = require('./config');

const startedAt = Date.now();

const state = {
  deviceId: 'esp8266-sim',
  rssi: -58,
  lastDeviceAt: 0, // epoch ms of the newest real device report; 0 = a board has never reported
  values: new Map(CATALOG.map((s) => [s.id, s.sim.start])),
  history: [], // newest last, capped at HISTORY_LEN
};

// A board that reported inside the timeout owns the numbers; otherwise the simulator does. This is
// what lets the firmware take over later without a flag flip or a redeploy — it just starts POSTing.
function deviceIsLive() {
  return state.lastDeviceAt > 0 && Date.now() - state.lastDeviceAt < DEVICE_TIMEOUT_MS;
}

function snapshot() {
  const live = deviceIsLive();
  return {
    type: 'snapshot',
    // Пока плата не на связи, её опознавательные данные — мусор из прошлого сеанса, поэтому
    // отдаём их только вместе с живым источником.
    deviceId: live ? state.deviceId : 'esp8266-sim',
    source: live ? 'device' : 'fake',
    online: live,
    ts: Date.now(),
    uptimeSec: Math.floor((Date.now() - startedAt) / 1000),
    rssi: live ? state.rssi : 0,
    sensors: CATALOG.map((s) => ({
      ...describe(s),
      value: round(state.values.get(s.id) ?? 0, s.decimals),
    })),
  };
}

// Applies a device report. Accepts both the array shape the server emits
// ({ sensors: [{ id, value }] }) and a flat object ({ temperature: 23.4, humidity: 48 }), because
// a tiny firmware is far happier building the flat one. Unknown ids are ignored, not an error —
// a board running older firmware than the catalog should still report what it does have.
function ingest(payload) {
  if (!payload || typeof payload !== 'object') return [];

  const pairs = Array.isArray(payload.sensors)
    ? payload.sensors.map((s) => [s && s.id, s && s.value])
    : Object.entries(payload);

  const applied = [];
  for (const [id, value] of pairs) {
    if (!BY_ID.has(id)) continue;
    const num = typeof value === 'boolean' ? (value ? 1 : 0) : Number(value);
    if (!Number.isFinite(num)) continue;
    state.values.set(id, num);
    applied.push(id);
  }

  if (typeof payload.deviceId === 'string' && payload.deviceId) state.deviceId = payload.deviceId;
  if (Number.isFinite(Number(payload.rssi))) state.rssi = Number(payload.rssi);

  // Only a report that actually carried a known reading counts as proof of life — otherwise an
  // empty or malformed POST would keep the simulator suppressed while nothing real arrives.
  if (applied.length) {
    const wasLive = deviceIsLive();
    state.lastDeviceAt = Date.now();
    if (!wasLive) log('Device', `${state.deviceId} took over (${applied.join(', ')})`);
  }
  return applied;
}

// Advances the world by one tick and returns the snapshot to broadcast.
function tick() {
  if (!deviceIsLive()) simulate(state.values);
  const snap = snapshot();
  state.history.push({ ts: snap.ts, values: Object.fromEntries(snap.sensors.map((s) => [s.id, s.value])) });
  if (state.history.length > HISTORY_LEN) state.history.shift();
  return snap;
}

module.exports = { state, snapshot, ingest, tick, deviceIsLive };
