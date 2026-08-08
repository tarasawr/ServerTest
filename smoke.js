'use strict';
// Manual smoke check: `npm start` in one shell, `node smoke.js` in another.
// Verifies the socket pushes snapshots, and that a device POST takes the numbers over from the sim.

const WebSocket = require('ws');

const BASE = process.argv[2] || 'http://localhost:3000';
const WS_URL = BASE.replace(/^http/, 'ws') + '/ws';

const snapshots = [];
const ws = new WebSocket(WS_URL);

ws.on('open', () => console.log(`connected to ${WS_URL}`));
ws.on('message', (raw) => {
  const m = JSON.parse(raw);
  snapshots.push(m);
  const t = m.sensors.find((s) => s.id === 'temperature1');
  console.log(`  #${snapshots.length} source=${m.source} online=${m.online} temperature=${t.value}${t.unit}`);
});
ws.on('error', (e) => { console.error('ws error:', e.message); process.exit(1); });

setTimeout(async () => {
  console.log('\nPOSTing a device reading (flat shape, as the firmware will)…');
  const res = await fetch(`${BASE}/api/sensors`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ deviceId: 'esp8266-kitchen', rssi: -61, temperature1: 41.5, temperature2: 38.0, humidity: 12.5, motion: true }),
  });
  console.log('  ->', res.status, JSON.stringify(await res.json()));
}, 3000);

setTimeout(() => {
  const last = snapshots.at(-1);
  const temp = last.sensors.find((s) => s.id === 'temperature1');
  const motion = last.sensors.find((s) => s.id === 'motion');
  const ok = last.source === 'device' && last.online === true
    && last.deviceId === 'esp8266-kitchen' && temp.value === 41.5 && motion.value === 1;
  console.log(`\n${snapshots.length} snapshots received.`);
  console.log(ok ? 'PASS — device readings took over the simulator' : 'FAIL — ' + JSON.stringify(last, null, 2));
  process.exit(ok ? 0 : 1);
}, 6000);
