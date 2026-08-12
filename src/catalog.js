'use strict';

// One entry per physical sensor. This array IS the client contract: it ships inside every snapshot
// and Unity builds one UI row per element, so adding a sensor here (and later in the firmware)
// needs no C# change at all.
const CATALOG = [
  {
    id: 'temperature1', label: 'Температура 1', unit: '°C', kind: 'number',
    min: -20, max: 60, decimals: 1,
  },
  {
    id: 'temperature2', label: 'Температура 2', unit: '°C', kind: 'number',
    min: -20, max: 60, decimals: 1,
  },
  {
    id: 'humidity', label: 'Влажность', unit: '%', kind: 'number',
    min: 0, max: 100, decimals: 1,
  },
  {
    id: 'pressure', label: 'Давление', unit: 'гПа', kind: 'number',
    min: 900, max: 1100, decimals: 1,
  },
  {
    id: 'light', label: 'Освещённость', unit: 'лк', kind: 'number',
    min: 0, max: 1024, decimals: 0,
  },
  {
    id: 'motion', label: 'Движение', unit: '', kind: 'bool',
    min: 0, max: 1, decimals: 0,
  },
];

const BY_ID = new Map(CATALOG.map((s) => [s.id, s]));

function round(value, decimals) {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

// The wire shape of a sensor definition.
function describe(s) {
  return { id: s.id, label: s.label, unit: s.unit, kind: s.kind, min: s.min, max: s.max, decimals: s.decimals };
}

module.exports = { CATALOG, BY_ID, round, describe };
