'use strict';

const { CATALOG, BY_ID, round, describe } = require('./catalog');
const {
  DEVICE_TIMEOUT_MS, HISTORY_WINDOW_MS, HISTORY_EVERY_MS,
  DEVICE_CLOCK_TOLERANCE_MS, LOG_READINGS, TICK_MS, log, warn,
} = require('./config');

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

// Цифры здесь только настоящие. Пока плата не отчиталась ни разу, в значениях нули — так и уедут
// клиенту: ноль, подписанный source=none, честнее правдоподобной выдумки, которую невозможно
// отличить от настоящего замера.
const state = {
  deviceId: 'esp8266',
  rssi: 0,
  device: {},      // произвольные сведения о плате из последнего POST
  lastDeviceAt: 0, // epoch ms of the newest real device report; 0 = a board has never reported
  deviceTs: 0,     // часы самой платы из последнего POST (NTP, UTC мс); 0 = не синхронизированы
  deviceTsAt: 0,   // когда этот отчёт приняли по серверным часам — по нему часы платы «дотикивают»
  values: new Map(CATALOG.map((s) => [s.id, 0])),
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
let lastSampleAt = 0;     // когда последний раз клали точку в историю (по серверным часам)
let clockComplained = false; // про разъехавшиеся часы платы ругаемся один раз, а не каждый POST

// Плата, отчитавшаяся не позже таймаута, считается живой. Замолчала — цифры остаются на последних
// принятых: подменять их нечем и незачем.
function deviceIsLive() {
  return state.lastDeviceAt > 0 && Date.now() - state.lastDeviceAt < DEVICE_TIMEOUT_MS;
}

/** Приходила ли вообще хоть одна настоящая цифра с момента запуска сервера. */
function hasReadings() {
  return state.lastDeviceAt > 0;
}

/**
 * Когда сняты те показания, что лежат в state.values. Пока плата живая — это её часы «сейчас»,
 * после обрыва — момент последнего отчёта: время замера не должно продолжать идти вперёд вместе
 * с серверными часами, иначе замер часовой давности выглядел бы свежим.
 */
function readingTs() {
  if (!hasReadings()) return 0;
  if (deviceIsLive()) return boardClockMs() || state.lastDeviceAt;
  return state.deviceTs || state.lastDeviceAt;
}

/**
 * Часы платы «сейчас»: последняя присланная метка плюс то, что натикало с момента её приёма.
 * Плата отчитывается раз в SEND_INTERVAL_MS, а снапшоты уходят каждый тик — без досчёта время
 * в интерфейсе дёргалось бы рывками по десять секунд. 0 = часов нет: плата молчит или её NTP
 * ещё не синхронизировался.
 */
function boardClockMs() {
  if (!state.deviceTs) return 0;
  const elapsed = Date.now() - state.deviceTsAt;
  if (elapsed > DEVICE_TIMEOUT_MS) return 0;  // плата пропала — досчитывать нечего
  return state.deviceTs + elapsed;
}

function snapshot() {
  const live = deviceIsLive();
  const now = Date.now();
  const board = live ? boardClockMs() : 0;
  return {
    type: 'snapshot',
    deviceId: state.deviceId,
    // device — плата на связи прямо сейчас; stale — связи нет, показания последние принятые;
    // none — плата не отчитывалась ни разу, в значениях нули.
    source: live ? 'device' : (hasReadings() ? 'stale' : 'none'),
    online: live,
    // Основная метка времени — когда сняты показания, а не когда собран этот снапшот: график
    // должен быть подписан временем того, кто мерил. Пока плата живая, это её NTP-часы; после
    // обрыва метка застывает на последнем отчёте, поэтому по ней видно, насколько цифры устарели.
    ts: readingTs() || now,
    serverTs: now,
    deviceTs: board,
    uptimeSec: Math.floor((now - startedAt) / 1000),
    rssi: live ? state.rssi : 0,
    // Паспорт платы остаётся и после обрыва: показания на экране её, значит и подпись под ними
    // должна быть её. Пустой объект, а не null — клиенту не нужно проверять на null каждое поле.
    device: state.device,
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
  let source;
  if (snap.source === 'device') source = `device(${snap.deviceId} rssi=${snap.rssi} reports=${reportCount})`;
  else if (snap.source === 'stale') source = `stale(last reading ${Math.round((Date.now() - snap.ts) / 1000)}s ago)`;
  else source = 'none (no board has reported yet)';
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
  const clockAccepted = acceptDeviceClock(Number(payload.ts));

  const info = sanitizeDeviceInfo(payload.device);
  if (info) state.device = info;

  // Only a report that actually carried a known reading counts as proof of life — otherwise an
  // empty or malformed POST would mark a mute board as online.
  if (applied.length) {
    const live = deviceIsLive();
    state.lastDeviceAt = Date.now();
    if (!live) {
      reportCount = 1;
      // Датчики, которых в отчёте не было, остаются на своих прошлых значениях (или на нулях,
      // если их не присылали ни разу) — подставлять туда нечего.
      const missing = CATALOG.filter((s) => !applied.includes(s.id)).map((s) => s.id);
      log('Device', `${state.deviceId} ONLINE — reporting ${applied.length}/${CATALOG.length}: ${applied.join(', ')}`
        + (missing.length ? ` | no data for: ${missing.join(', ')}` : ''));
      // Паспорт платы в логе — быстрый ответ на «а какая прошивка сейчас в железке и где она стоит».
      const { board, fw, ip } = state.device;
      if (board || fw || ip) log('Device', `  ${board || 'плата'} · fw ${fw || '?'} · ${ip || 'ip ?'}`);
    } else {
      reportCount++;
      if (previousId !== state.deviceId) log('Device', `board changed: ${previousId} -> ${state.deviceId}`);
    }
    logReading(applied, clockAccepted);
  }
  return applied;
}

/**
 * Строка в лог на каждый принятый замер: что именно доехало и с каким временем. Печатаются только
 * поля из этого отчёта, а не весь каталог, — иначе датчики, которых на плате нет, каждые десять
 * секунд напоминали бы о себе нулями.
 *
 * Возраст метки времени показываем рядом со значениями: расхождение часов платы и сервера иначе
 * замечаешь, только когда уже недоумеваешь, почему график съехал.
 */
function logReading(applied, clockAccepted) {
  if (!LOG_READINGS) return;

  const values = applied.map((id) => {
    const sensor = BY_ID.get(id);
    const value = round(state.values.get(id), sensor.decimals);
    return sensor.kind === 'bool'
      ? `${id}=${value ? 'да' : 'нет'}`
      : `${id}=${value}${sensor.unit}`;
  }).join(' ');

  // Часы показываем только те, что приехали в ЭТОМ отчёте. Иначе замер без метки времени
  // унаследовал бы время предыдущего и выглядел бы подписанным платой, хотя это не так.
  const clock = clockAccepted
    ? `часы платы ${new Date(state.deviceTs).toISOString().slice(11, 19)}`
    : 'без метки времени (подписан серверными часами)';

  log('Reading', `#${reportCount} ${values} | rssi=${state.rssi} | ${clock}`);
}

/**
 * Метка времени из POST. Плата присылает 0, пока NTP не ответил, — это не ошибка, а «часов ещё
 * нет». А вот время, разъехавшееся с серверным больше чем на допуск, отбрасываем: с ним график
 * уехал бы в позапрошлый год или в будущее, и понять, что виноваты часы платы, было бы неоткуда.
 *
 * @returns {boolean} приняли ли метку из этого отчёта
 */
function acceptDeviceClock(ts) {
  if (!Number.isFinite(ts) || ts <= 0) return false;

  const now = Date.now();
  const drift = ts - now;
  if (Math.abs(drift) > DEVICE_CLOCK_TOLERANCE_MS) {
    if (!clockComplained) {
      clockComplained = true;
      warn('Device', `board clock is off by ${Math.round(drift / 1000)}s (${new Date(ts).toISOString()}) `
        + '— ignoring its timestamps, chart falls back to server time');
    }
    return false;
  }

  if (!state.deviceTs) log('Device', `board clock in sync: ${new Date(ts).toISOString()} (${drift >= 0 ? '+' : ''}${drift}ms vs server)`);
  clockComplained = false;
  state.deviceTs = ts;
  state.deviceTsAt = now;
  return true;
}

// Собирает снапшот для рассылки. Ничего не «продвигает»: без платы состояние мира не меняется.
function tick() {
  const live = deviceIsLive();

  if (wasLive && !live) {
    const silentFor = Math.round((Date.now() - state.lastDeviceAt) / 1000);
    warn('Device', `${state.deviceId} OFFLINE — silent for ${silentFor}s after ${reportCount} report(s), holding last readings`);
    reportCount = 0;
  }
  wasLive = live;

  const snap = snapshot();
  // В историю попадают только настоящие замеры. Пока плата молчит, повторять последнее значение
  // раз в десять секунд нельзя: на графике это ровная линия, неотличимая от «датчик стабилен».
  if (live) recordHistory(snap);
  return snap;
}

// Точка в историю кладётся по своему, куда более редкому расписанию, чем идут снапшоты, а всё
// старше окна выбрасывается — так буфер сам держит ровно последний час без ограничения по длине.
//
// Расписание считается по серверным часам, а подписывается точка временем из снапшота (то есть
// платы). Иначе уход платы в офлайн — а с ним и переход метки с её часов на серверные — сдвинул
// бы расписание на разницу часов, и запись истории замерла бы на эти секунды.
function recordHistory(snap) {
  const now = Date.now();
  if (now - lastSampleAt < HISTORY_STEP_MS) return;
  lastSampleAt = now;

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

module.exports = { state, snapshot, summary, ingest, tick, deviceIsLive, hasReadings, historyPayload };
