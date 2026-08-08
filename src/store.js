'use strict';

const { CATALOG, BY_ID, simulate, round, describe } = require('./catalog');
const { DEVICE_TIMEOUT_MS, HISTORY_LEN, log, warn } = require('./config');

const startedAt = Date.now();

// Поля верхнего уровня, которые плата шлёт рядом с показаниями — чтобы не ругаться на них как на
// неизвестные датчики.
const META_KEYS = new Set(['deviceId', 'rssi', 'sensors', 'type', 'ts']);

const state = {
  deviceId: 'esp8266-sim',
  rssi: -58,
  lastDeviceAt: 0, // epoch ms of the newest real device report; 0 = a board has never reported
  values: new Map(CATALOG.map((s) => [s.id, s.sim.start])),
  history: [], // newest last, capped at HISTORY_LEN
};

let wasLive = false;      // была ли плата живой на прошлом тике — чтобы поймать момент обрыва
let reportCount = 0;      // сколько POST приняли с тех пор, как плата вышла на связь
let lastIgnoredKeys = ''; // чтобы ругаться на незнакомые поля один раз, а не с частотой прошивки

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

/** Однострочная сводка состояния — для периодического лога. */
function summary(clientCount) {
  const snap = snapshot();
  const readings = snap.sensors.map((s) => `${s.id}=${s.value}${s.unit}`).join(' ');
  const source = snap.source === 'device'
    ? `device(${snap.deviceId} rssi=${snap.rssi} reports=${reportCount})`
    : 'simulator';
  return `clients=${clientCount} source=${source} uptime=${snap.uptimeSec}s | ${readings}`;
}

// Applies a device report. Accepts both the array shape the server emits
// ({ sensors: [{ id, value }] }) and a flat object ({ temperature1: 23.4, humidity: 48 }), because
// a tiny firmware is far happier building the flat one. Unknown ids are ignored, not an error —
// a board running older firmware than the catalog should still report what it does have.
function ingest(payload) {
  if (!payload || typeof payload !== 'object') return [];

  const pairs = Array.isArray(payload.sensors)
    ? payload.sensors.map((s) => [s && s.id, s && s.value])
    : Object.entries(payload);

  const applied = [];
  const ignored = [];
  for (const [id, value] of pairs) {
    if (!BY_ID.has(id)) {
      if (id && !META_KEYS.has(id)) ignored.push(id);
      continue;
    }
    const num = typeof value === 'boolean' ? (value ? 1 : 0) : Number(value);
    if (!Number.isFinite(num)) {
      ignored.push(`${id}=${JSON.stringify(value)}`);
      continue;
    }
    state.values.set(id, num);
    applied.push(id);
  }

  // Опечатка в имени поля прошивки иначе тонет молча: значение просто не доедет, и никто об этом
  // не узнает. Ругаемся — но один раз на каждый новый набор, плата ведь шлёт это в цикле.
  if (ignored.length) {
    const key = ignored.join(',');
    if (key !== lastIgnoredKeys) {
      lastIgnoredKeys = key;
      warn('Device', `ignoring unknown field(s): ${key} | known ids: ${CATALOG.map((s) => s.id).join(', ')}`);
    }
  } else {
    lastIgnoredKeys = '';
  }

  const previousId = state.deviceId;
  if (typeof payload.deviceId === 'string' && payload.deviceId) state.deviceId = payload.deviceId;
  if (Number.isFinite(Number(payload.rssi))) state.rssi = Number(payload.rssi);

  // Only a report that actually carried a known reading counts as proof of life — otherwise an
  // empty or malformed POST would keep the simulator suppressed while nothing real arrives.
  if (applied.length) {
    const live = deviceIsLive();
    state.lastDeviceAt = Date.now();
    if (!live) {
      reportCount = 1;
      const missing = CATALOG.filter((s) => !applied.includes(s.id)).map((s) => s.id);
      log('Device', `${state.deviceId} ONLINE — simulator off, reporting ${applied.length}/${CATALOG.length}: ${applied.join(', ')}`
        + (missing.length ? ` | still simulated: ${missing.join(', ')}` : ''));
    } else {
      reportCount++;
      if (previousId !== state.deviceId) log('Device', `board changed: ${previousId} -> ${state.deviceId}`);
    }
  }
  return applied;
}

// Advances the world by one tick and returns the snapshot to broadcast.
function tick() {
  const live = deviceIsLive();

  // Уход платы иначе не виден вообще: симулятор бесшумно подхватывает цифры, и в логе тишина.
  if (wasLive && !live) {
    const silentFor = Math.round((Date.now() - state.lastDeviceAt) / 1000);
    warn('Device', `${state.deviceId} OFFLINE — silent for ${silentFor}s after ${reportCount} report(s), simulator resumed`);
    reportCount = 0;
  }
  wasLive = live;

  if (!live) simulate(state.values);

  const snap = snapshot();
  state.history.push({ ts: snap.ts, values: Object.fromEntries(snap.sensors.map((s) => [s.id, s.value])) });
  if (state.history.length > HISTORY_LEN) state.history.shift();
  return snap;
}

module.exports = { state, snapshot, summary, ingest, tick, deviceIsLive };
