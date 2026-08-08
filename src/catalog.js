'use strict';

// One entry per physical sensor. This array IS the client contract: it ships inside every snapshot
// and Unity builds one UI row per element, so adding a sensor here (and later in the firmware)
// needs no C# change at all. `sim` only feeds the fake generator and is stripped before sending.
const CATALOG = [
  {
    id: 'temperature1', label: 'Температура 1', unit: '°C', kind: 'number',
    min: -20, max: 60, decimals: 1,
    sim: { start: 23.0, drift: 0.12, lo: 18, hi: 29 },
  },
  {
    id: 'temperature2', label: 'Температура 2', unit: '°C', kind: 'number',
    min: -20, max: 60, decimals: 1,
    sim: { start: 25.5, drift: 0.12, lo: 20, hi: 32 },
  },
  {
    id: 'humidity', label: 'Влажность', unit: '%', kind: 'number',
    min: 0, max: 100, decimals: 1,
    sim: { start: 47.0, drift: 0.4, lo: 30, hi: 70 },
  },
  {
    id: 'pressure', label: 'Давление', unit: 'гПа', kind: 'number',
    min: 900, max: 1100, decimals: 1,
    sim: { start: 1013.0, drift: 0.15, lo: 995, hi: 1030 },
  },
  {
    id: 'light', label: 'Освещённость', unit: 'лк', kind: 'number',
    min: 0, max: 1024, decimals: 0,
    sim: { start: 420, drift: 22, lo: 0, hi: 1024 },
  },
  {
    id: 'motion', label: 'Движение', unit: '', kind: 'bool',
    min: 0, max: 1, decimals: 0,
    sim: { start: 0 },
  },
];

const BY_ID = new Map(CATALOG.map((s) => [s.id, s]));

// Random walk clamped to [lo, hi], so the numbers drift like a real sensor instead of jumping
// around like noise. Booleans instead flip on a coin weighted to keep them mostly idle.
function simulate(values) {
  for (const s of CATALOG) {
    const current = values.get(s.id);
    if (s.kind === 'bool') {
      // Motion sits at 0 most of the time, then stays high for a few ticks once it trips.
      const flipChance = current ? 0.25 : 0.02;
      if (Math.random() < flipChance) values.set(s.id, current ? 0 : 1);
      continue;
    }
    const next = current + (Math.random() * 2 - 1) * s.sim.drift;
    values.set(s.id, Math.min(s.sim.hi, Math.max(s.sim.lo, next)));
  }
}

function round(value, decimals) {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

// The wire shape of a sensor definition — `sim` is deliberately left out.
function describe(s) {
  return { id: s.id, label: s.label, unit: s.unit, kind: s.kind, min: s.min, max: s.max, decimals: s.decimals };
}

module.exports = { CATALOG, BY_ID, simulate, round, describe };
