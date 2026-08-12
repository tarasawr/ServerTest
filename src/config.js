'use strict';

// Render injects PORT; 3000 is the local default.
const PORT = Number(process.env.PORT) || 3000;

// A board that hasn't POSTed within this window counts as gone: readings freeze at their last
// values and the snapshot says so. Set it to a few times the firmware's send interval.
const DEVICE_TIMEOUT_MS = Number(process.env.DEVICE_TIMEOUT_MS) || 15000;

// How often a fresh snapshot is generated and pushed to every connected client.
const TICK_MS = Number(process.env.TICK_MS) || 1000;

// Shared secret the firmware sends as the X-Device-Key header. Empty = accept any POST, which is
// fine on a LAN but means anyone can spoof readings once this is public.
const DEVICE_KEY = process.env.DEVICE_KEY || '';

// Глубина графика. Точку в историю кладём заметно реже, чем шлём снапшоты: час по секунде — это
// 3600 точек на датчик, которые незачем ни хранить, ни гонять по сети, ни рисовать. Раз в 10 с
// даёт 360 точек на час — как раз под ширину графика в пикселях.
const HISTORY_WINDOW_MS = Number(process.env.HISTORY_WINDOW_MS) || 60 * 60 * 1000;
const HISTORY_EVERY_MS = Number(process.env.HISTORY_EVERY_MS) || 10000;

// Насколько часам платы позволено расходиться с серверными, прежде чем её метку времени считать
// мусором. И плата, и сервер синхронизируются по NTP, так что реальная разница — доли секунды;
// пять минут — это запас на кривой NTP и на плату, которая шлёт время до первой синхронизации.
const DEVICE_CLOCK_TOLERANCE_MS = Number(process.env.DEVICE_CLOCK_TOLERANCE_MS) || 5 * 60 * 1000;

// Drop a socket that misses two heartbeats — Render's proxy does not always deliver a close frame.
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS) || 30000;

// Сводка «всё живо, вот текущие цифры» раз в столько мс. На Render единственное окно в сервер —
// поток логов, и без периодической строки непонятно, работает он вообще или висит.
const STATUS_EVERY_MS = Number(process.env.STATUS_EVERY_MS) || 60000;

const REV = (process.env.RENDER_GIT_COMMIT || process.env.GIT_SHA || 'dev').slice(0, 7);

// Тег выравнивается по ширине, чтобы столбец сообщений не прыгал и лог читался глазами.
function log(tag, msg) {
  console.log(`${new Date().toISOString()} ${tag.padEnd(7)} ${msg}`);
}

function warn(tag, msg) {
  console.warn(`${new Date().toISOString()} ${tag.padEnd(7)} WARN ${msg}`);
}

module.exports = {
  PORT, DEVICE_TIMEOUT_MS, TICK_MS, DEVICE_KEY, HEARTBEAT_MS,
  HISTORY_WINDOW_MS, HISTORY_EVERY_MS, DEVICE_CLOCK_TOLERANCE_MS,
  STATUS_EVERY_MS, REV, log, warn,
};
