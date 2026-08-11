'use strict';

const { CATALOG, BY_ID, simulate, round, describe } = require('./catalog');
const { DEVICE_TIMEOUT_MS, HISTORY_WINDOW_MS, HISTORY_EVERY_MS, TICK_MS, log, warn } = require('./config');

// Историю пишет тик, поэтому чаще тика точки не появятся, сколько ни проси. Клиенту сообщаем
// фактический шаг, а не заказанный — иначе он растянет ось времени не туда.
const HISTORY_STEP_MS = Math.max(HISTORY_EVERY_MS, TICK_MS);

const startedAt = Date.now();

// Поля верхнего уровня, которые плата шлёт рядом с показаниями — чтобы не ругаться на них как на
// неизвестные датчики.
const META_KEYS = new Set(['deviceId', 'rssi', 'sensors', 'type', 'ts', 'device']);

// Паспорт платы (`device`) сервер не разбирает по полям: что прошивка положила, то Unity и покажет.
// Добавить строчку про новый датчик или напряжение питания можно правкой одной прошивки.
// Ограничения — чтобы кто угодно с доступом к POST не смог раздуть снапшот до мегабайта.
const DEVICE_INFO_MAX_KEYS = 32;
const DEVICE_INFO_MAX_VALUE = 96;

const state = {
  deviceId: 'esp8266-sim',
  rssi: -58,
  device: {},      // произвольные сведения о плате из последнего POST
  lastDeviceAt: 0, // epoch ms of the newest real device report; 0 = a board has never reported
  values: new Map(CATALOG.map((s) => [s.id, s.sim.start])),
  history: [], // newest last, trimmed to HISTORY_WINDOW_MS
};

// Пропускаем только плоские скаляры: вложенные объекты и массивы Unity всё равно не разберёт
// своим JsonUtility, а строки режем по длине, чтобы одно кривое поле не распухло на весь снапшот.
function sanitizeDeviceInfo(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const out = {};
  for (const [key, value] of Object.entries(raw)) {
    if (Object.keys(out).length >= DEVICE_INFO_MAX_KEYS) break;
    if (typeof value === 'string') out[key] = value.slice(0, DEVICE_INFO_MAX_VALUE);
    else if (typeof value === 'boolean') out[key] = value;
    else if (Number.isFinite(value)) out[key] = value;
  }
  return Object.keys(out).length ? out : null;
}

let wasLive = false;      // была ли плата живой на прошлом тике — чтобы поймать момент обрыва
let reportCount = 0;      // сколько POST приняли с тех пор, как плата вышла на связь
let lastIgnoredKeys = ''; // чтобы ругаться на незнакомые поля один раз, а не с частотой прошивки
let lastSampleAt = 0;     // когда последний раз клали точку в историю

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
    // Пустой объект, а не null: клиенту так не нужно проверять на null перед каждым полем.
    device: live ? state.device : {},
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

  const info = sanitizeDeviceInfo(payload.device);
  if (info) state.device = info;

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
      // Паспорт платы в логе — быстрый ответ на «а какая прошивка сейчас в железке и где она стоит».
      const { board, fw, ip } = state.device;
      if (board || fw || ip) log('Device', `  ${board || 'плата'} · fw ${fw || '?'} · ${ip || 'ip ?'}`);
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
  recordHistory(snap);
  return snap;
}

// Точка в историю кладётся по своему, куда более редкому расписанию, чем идут снапшоты, а всё
// старше окна выбрасывается — так буфер сам держит ровно последний час без ограничения по длине.
function recordHistory(snap) {
  if (snap.ts - lastSampleAt < HISTORY_STEP_MS) return;
  lastSampleAt = snap.ts;

  state.history.push({ ts: snap.ts, values: Object.fromEntries(snap.sensors.map((s) => [s.id, s.value])) });

  const cutoff = snap.ts - HISTORY_WINDOW_MS;
  while (state.history.length && state.history[0].ts < cutoff) state.history.shift();
}

/**
 * История для графика. Метки времени общие для всех рядов (замеры берутся одновременно), а
 * значения — плоскими массивами: так вдвое меньше байт, чем массивом объектов, и, главное,
 * Unity JsonUtility это разбирает, а словарь `{temperature1: ...}` — нет.
 */
function historyPayload(ids) {
  const wanted = ids && ids.length ? CATALOG.filter((s) => ids.includes(s.id)) : CATALOG;
  return {
    type: 'history',
    windowSec: Math.round(HISTORY_WINDOW_MS / 1000),
    everySec: Math.round(HISTORY_STEP_MS / 1000),
    ts: state.history.map((h) => h.ts),
    series: wanted.map((s) => ({
      ...describe(s),
      values: state.history.map((h) => h.values[s.id] ?? 0),
    })),
  };
}

module.exports = { state, snapshot, summary, ingest, tick, deviceIsLive, historyPayload };
