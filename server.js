const http = require('http');
const dgram = require('dgram');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const LEGACY_INVITE = '__legacy__';
const BOT_COUNT = 3; // bots auto-spawned per session (0 to disable)

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/sessions') {
    const list = [];
    for (const [code, s] of sessions) {
      if (code === LEGACY_INVITE) continue;
      list.push({
        id: s.id,
        inviteCode: code,
        players: s.players.size,
        linkPermission: s.linkPermission
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ sessions: list }));
    return;
  }

  // GET /sessions/latest/project — returns projectXml of the first active session
  if (url.pathname === '/sessions/latest/project') {
    for (const [code, s] of sessions) {
      if (code === LEGACY_INVITE) continue;
      if (s.players.size > 0) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ inviteCode: code, projectXml: s.projectXml }));
        log('HTTP', `Served project XML for session ${s.id} (${s.projectXml.length} chars)`);
        return;
      }
    }
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'No active session' }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Multiplayer server OK. Sessions: ${sessions.size}, Clients: ${clients.size}`);
});

const wss = new WebSocket.Server({ server });

// --- State ---

let nextPlayerId = 1;
const sessions = new Map();  // inviteCode -> Session
const clients = new Map();   // WebSocket -> ClientState

// --- Player colors (10 distinct colors from design) ---

const PLAYER_COLORS = [
  '#8C5CF6', // purple
  '#F04545', // red
  '#FAD93D', // yellow
  '#22C55E', // green
  '#ED4A99', // magenta
  '#737D8C', // slate gray
  '#4285F4', // blue
  '#F28D28', // orange
  '#66BAE9', // light blue
  '#99A626', // olive
];

function pickColor(session) {
  const usedColors = new Set();
  for (const [, p] of session.players) {
    if (p.color) usedColors.add(p.color);
  }
  const available = PLAYER_COLORS.filter(c => !usedColors.has(c));
  const pool = available.length > 0 ? available : PLAYER_COLORS;
  return pool[Math.floor(Math.random() * pool.length)];
}

// --- Helpers ---

function ts() {
  return new Date().toISOString().slice(11, 23);
}

function log(tag, msg) {
  console.log(`${ts()} [${tag}] ${msg}`);
}

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
  let code = '';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

function fmtPos(p) {
  return p ? `(${p.x?.toFixed(2)},${p.y?.toFixed(2)},${p.z?.toFixed(2)})` : '?';
}

function fmtChanged(mask) {
  const parts = [];
  if (mask & PROP_POSITION)     parts.push('pos');
  if (mask & PROP_ROTATION)     parts.push('rot');
  if (mask & PROP_SCALE)        parts.push('scl');
  if (mask & PROP_PLANE_OFFSET) parts.push('off');
  return parts.join('+') || 'none';
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) sendPossiblyChunked(ws, obj);
}

function sendError(ws, code, message) {
  const client = clients.get(ws);
  log('Error', `player=${client?.playerId} code=${code} msg="${message}"`);
  send(ws, { type: 'session_error', code, message });
}

function findSessionById(id) {
  for (const [, s] of sessions) { if (s.id === id) return s; }
  return null;
}

function broadcastToSession(session, excludeWs, obj) {
  const data = JSON.stringify(obj);
  let count = 0;
  for (const [, p] of session.players) {
    if (p.ws !== excludeWs && p.ws.readyState === WebSocket.OPEN) {
      p.ws.send(data);
      count++;
    }
  }
  return count;
}

function canEdit(role) {
  return role === 'owner' || role === 'editor';
}

function sessionInfo(session) {
  return `${session.id} (players: ${session.players.size}, seq: ${session.sequenceNumber})`;
}

// --- Per-property LWW (Last-Writer-Wins) ---

const PROP_POSITION     = 1;
const PROP_ROTATION     = 2;
const PROP_SCALE        = 4;
const PROP_PLANE_OFFSET = 8;
const PROP_ALL          = 15;

function getEntityState(session, entityId) {
  if (!session.entityState) session.entityState = new Map();
  if (!session.entityState.has(entityId)) session.entityState.set(entityId, {});
  return session.entityState.get(entityId);
}

function lwwMerge(entity, prop, value, timestamp) {
  const current = entity[prop];
  if (!current || timestamp >= current.ts) {
    entity[prop] = { v: value, ts: timestamp };
    return true;
  }
  return false;
}

// --- Legacy: auto-create/join default session for clients that skip create_session ---

function getOrCreateLegacySession(ws, client) {
  let session = sessions.get(LEGACY_INVITE);
  if (!session) {
    session = {
      id: 'sess_legacy',
      inviteCode: LEGACY_INVITE,
      ownerId: client.playerId,
      ownerUserId: null,
      projectXml: '',
      linkPermission: 'edit',
      sequenceNumber: 0,
      players: new Map(),
      entityState: new Map()
    };
    sessions.set(LEGACY_INVITE, session);
    log('Legacy', 'Session created');
  }

  const player = {
    playerId: client.playerId, userId: null,
    userName: `Player ${client.playerId}`, role: 'owner',
    position: { x: 0, y: 0, z: 0 }, rotation: { x: 0, y: 0, z: 0 }, ws
  };
  session.players.set(client.playerId, player);
  client.sessionId = session.id;

  // Send welcome (old-style message for current client code)
  const existing = [];
  for (const [, p] of session.players) {
    if (p.playerId !== client.playerId)
      existing.push({ id: p.playerId, position: p.position, rotation: p.rotation });
  }
  send(ws, { type: 'welcome', playerId: client.playerId, players: existing });

  broadcastToSession(session, ws, {
    type: 'player_joined', playerId: client.playerId,
    position: player.position, rotation: player.rotation
  });

  log('Legacy', `Player ${client.playerId} auto-joined (total: ${session.players.size})`);
  return session;
}

// --- Session that client is in (with legacy fallback) ---

function getSession(ws, client, autoJoinLegacy) {
  if (client.sessionId) return findSessionById(client.sessionId);
  if (autoJoinLegacy) return getOrCreateLegacySession(ws, client);
  sendError(ws, 'INVALID_MESSAGE', 'Not in a session');
  return null;
}

// --- Session lifecycle ---

function handleCreateSession(ws, client, msg) {
  if (client.sessionId) { sendError(ws, 'ALREADY_IN_SESSION', 'Already in a session'); return; }

  // Dev mode: if another non-legacy session exists, auto-join it instead of creating a new one
  for (const [code, existing] of sessions) {
    if (code === LEGACY_INVITE) continue;
    if (existing.players.size > 0) {
      log('Session', `Dev auto-join: player ${client.playerId} → existing session ${existing.id} (invite: ${code})`);
      msg.inviteCode = code;
      return handleJoinSession(ws, client, msg);
    }
  }

  const inviteCode = generateCode();
  const sessionId = 'sess_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const linkPermission = msg.linkPermission || 'edit';

  // Create session first (needed for pickColor)
  const session = {
    id: sessionId, inviteCode, ownerId: client.playerId,
    ownerUserId: msg.userId || null, projectXml: msg.projectXml || '',
    linkPermission, sequenceNumber: 0,
    players: new Map(),
    entityState: new Map()
  };

  const player = {
    playerId: client.playerId, userId: msg.userId || null,
    userName: msg.userName || `Player ${client.playerId}`, role: 'owner',
    color: pickColor(session),
    position: { x: 0, y: 0, z: 0 }, rotation: { x: 0, y: 0, z: 0 }, ws
  };

  session.players.set(client.playerId, player);

  sessions.set(inviteCode, session);
  client.sessionId = sessionId;

  send(ws, {
    type: 'session_created', inviteCode, sessionId,
    sequenceNumber: 0, playerId: client.playerId,
    color: player.color
  });

  log('Session', `Created ${sessionId} (invite: ${inviteCode}, permission: ${linkPermission}) by player ${client.playerId}`);

  // Auto-spawn bots after owner has time to load and send position
  setTimeout(() => spawnSessionBots(inviteCode, session.projectXml), 5000);
}

function handleJoinSession(ws, client, msg) {
  if (client.sessionId) { sendError(ws, 'ALREADY_IN_SESSION', 'Already in a session'); return; }

  const session = sessions.get(msg.inviteCode);
  if (!session) { sendError(ws, 'NOT_FOUND', `Session not found: ${msg.inviteCode}`); return; }
  if (session.linkPermission === 'none') { sendError(ws, 'NO_ACCESS', 'Link disabled'); return; }
  if (session.players.size >= 25) { sendError(ws, 'SESSION_FULL', 'Max 25 players'); return; }

  let role = session.linkPermission === 'edit' ? 'editor' : 'viewer';
  if (msg.userId && msg.userId === session.ownerUserId) role = 'owner';

  const player = {
    playerId: client.playerId, userId: msg.userId || null,
    userName: msg.userName || `Player ${client.playerId}`, role,
    color: pickColor(session),
    position: { x: 0, y: 0, z: 0 }, rotation: { x: 0, y: 0, z: 0 }, ws
  };

  session.players.set(client.playerId, player);
  client.sessionId = session.id;

  const presence = [];
  for (const [, p] of session.players) {
    presence.push({
      playerId: p.playerId, userId: p.userId, userName: p.userName,
      role: p.role, color: p.color, position: p.position, rotation: p.rotation
    });
  }

  send(ws, {
    type: 'session_state', projectXml: session.projectXml,
    sequenceNumber: session.sequenceNumber, presence, role,
    playerId: client.playerId
  });

  broadcastToSession(session, ws, {
    type: 'player_joined', playerId: client.playerId,
    userId: player.userId, userName: player.userName, role,
    color: player.color,
    position: player.position, rotation: player.rotation
  });

  log('Session', `Player ${client.playerId} joined ${session.id} as ${role} (total: ${session.players.size})`);
}

function leaveSession(ws, client) {
  if (!client.sessionId) return;
  const session = findSessionById(client.sessionId);
  client.sessionId = null;
  if (!session) return;

  // Release all locks held by this player
  releasePlayerLocks(session, client.playerId, ws);

  session.players.delete(client.playerId);

  if (client.playerId === session.ownerId) {
    if (session.players.size === 0) {
      // No players left → delete session
      sessions.delete(session.inviteCode);
      log('Session', `${session.id} closed (owner left, no players remaining)`);
    } else {
      // Transfer ownership to next player
      const nextPlayer = session.players.values().next().value;
      session.ownerId = nextPlayer.playerId;
      nextPlayer.role = 'owner';

      // Notify all remaining players
      broadcastToSession(session, null, {
        type: 'owner_changed',
        newOwnerId: nextPlayer.playerId,
        reason: 'owner_left'
      });

      log('Session', `${session.id} ownership transferred to player ${nextPlayer.playerId} (total: ${session.players.size})`);
    }
  } else {
    broadcastToSession(session, ws, { type: 'player_left', playerId: client.playerId });
    log('Session', `Player ${client.playerId} left ${session.id} (total: ${session.players.size})`);
  }

  // Clean up empty legacy session
  if (session.inviteCode === LEGACY_INVITE && session.players.size === 0) {
    sessions.delete(LEGACY_INVITE);
    log('Legacy', 'Session removed (empty)');
  }
}

// --- Relay handlers ---

function handleMove(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;

  const player = session.players.get(client.playerId);
  if (player) { player.position = msg.position || player.position; player.rotation = msg.rotation || player.rotation; }

  broadcastToSession(session, ws, {
    type: 'player_moved', playerId: client.playerId,
    position: msg.position, rotation: msg.rotation
  });
}

function handlePointer(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;

  broadcastToSession(session, ws, {
    type: 'pointer', playerId: client.playerId,
    origin: msg.origin, target: msg.target
  });
}

function handleFurnitureUpdate(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} furniture_update (role: ${role})`);
    return;
  }

  const ts = Date.now();
  const changed = msg.changed || PROP_ALL;
  const entity = getEntityState(session, msg.furnitureId);

  // Per-property LWW merge
  let wins = 0;
  if (changed & PROP_POSITION)     { if (lwwMerge(entity, 'position', msg.position, ts)) wins |= PROP_POSITION; }
  if (changed & PROP_ROTATION)     { if (lwwMerge(entity, 'rotation', msg.rotation, ts)) wins |= PROP_ROTATION; }
  if (changed & PROP_SCALE)        { if (lwwMerge(entity, 'scale', msg.scale, ts)) wins |= PROP_SCALE; }
  if (changed & PROP_PLANE_OFFSET) { if (lwwMerge(entity, 'planeOffset', msg.planeOffset, ts)) wins |= PROP_PLANE_OFFSET; }

  const fid = msg.furnitureId?.slice(-6) || '?';
  const c = msg.committed ? 'C' : 'D'; // Committed / Drag
  log('⬇ FU', `p${client.playerId} ${fid} ${c} chg=${fmtChanged(changed)} pos=${fmtPos(msg.position)} wins=${fmtChanged(wins)}`);

  // If nothing won and not committed, skip broadcast
  if (wins === 0 && !msg.committed) {
    log('⬇ FU', `p${client.playerId} ${fid} SKIP (no wins)`);
    return;
  }

  if (msg.committed) session.sequenceNumber++;

  // Broadcast merged state (all properties from server-side entity state)
  const merged = {
    type: 'furniture_update', playerId: client.playerId,
    furnitureId: msg.furnitureId,
    position: entity.position?.v || msg.position,
    rotation: entity.rotation?.v || msg.rotation,
    scale: entity.scale?.v || msg.scale,
    planeOffset: entity.planeOffset?.v ?? msg.planeOffset,
    committed: msg.committed
  };
  const n = broadcastToSession(session, ws, merged);

  log('⬆ FU', `p${client.playerId} ${fid} ${c} → ${n} peers pos=${fmtPos(merged.position)}`);
}

function handleFurnitureAdd(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} furniture_add (role: ${role})`);
    return;
  }
  session.sequenceNumber++;

  const n = broadcastToSession(session, ws, {
    type: 'furniture_add', playerId: client.playerId,
    furnitureId: msg.furnitureId, variationPath: msg.variationPath,
    position: msg.position, rotation: msg.rotation, scale: msg.scale,
    planeOffset: msg.planeOffset, parentId: msg.parentId
  });

  log('Furniture', `add "${msg.furnitureId}" variation="${msg.variationPath}" by player=${client.playerId} → ${n} peers (seq: ${session.sequenceNumber})`);
}

function handleFurnitureRemove(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} furniture_remove (role: ${role})`);
    return;
  }
  session.sequenceNumber++;

  // Clean up LWW entity state
  if (session.entityState) {
    session.entityState.delete(msg.furnitureId);
    session.entityState.delete(`var:${msg.furnitureId}`);
  }

  const n = broadcastToSession(session, ws, {
    type: 'furniture_remove', playerId: client.playerId,
    furnitureId: msg.furnitureId
  });

  log('Furniture', `remove "${msg.furnitureId}" by player=${client.playerId} → ${n} peers (seq: ${session.sequenceNumber})`);
}

function handleFurnitureChangeVariation(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} furniture_change_variation (role: ${role})`);
    return;
  }
  session.sequenceNumber++;

  const n = broadcastToSession(session, ws, {
    type: 'furniture_change_variation', playerId: client.playerId,
    furnitureId: msg.furnitureId, variationPath: msg.variationPath
  });

  log('Furniture', `change_variation "${msg.furnitureId}" → "${msg.variationPath}" by player=${client.playerId} → ${n} peers`);
}

function handleDomainChange(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} domain_change (role: ${role})`);
    return;
  }

  if (!msg.changes || !msg.changes.length) return;

  const ts = Date.now();
  const winningChanges = [];

  for (const change of msg.changes) {
    const entityId = `dom:${msg.targetId}:${change.name}`;
    const entity = getEntityState(session, entityId);
    if (lwwMerge(entity, 'value', change.value, ts)) {
      winningChanges.push(change);
    }
  }

  if (winningChanges.length === 0) {
    log('Domain', `all changes rejected (stale) target="${msg.targetId}" by player=${client.playerId}`);
    return;
  }

  session.sequenceNumber++;

  const n = broadcastToSession(session, ws, {
    type: 'domain_change', playerId: client.playerId,
    targetId: msg.targetId,
    changes: winningChanges
  });

  const names = winningChanges.map(c => c.name).join(',');
  log('Domain', `change target="${msg.targetId}" props=[${names}] by player=${client.playerId} → ${n} peers`);
}

// --- Furniture locking ---

// Lock key format: "furnitureId:property" (per-property locking)
function lockKey(furnitureId, property) {
  return `${furnitureId}:${property || 'position'}`;
}

function parseLockKey(key) {
  const i = key.indexOf(':');
  return { furnitureId: key.slice(0, i), property: key.slice(i + 1) };
}

function handleFurnitureLock(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} furniture_lock (role: ${role})`);
    return;
  }

  if (!session.locks) session.locks = new Map();

  const key = lockKey(msg.furnitureId, msg.property);
  const existing = session.locks.get(key);
  if (existing && existing !== client.playerId) {
    send(ws, {
      type: 'furniture_lock_denied', furnitureId: msg.furnitureId,
      property: msg.property || 'position', lockedBy: existing
    });
    log('Lock', `denied "${msg.furnitureId}.${msg.property}" for player=${client.playerId} (held by ${existing})`);
    return;
  }

  session.locks.set(key, client.playerId);

  broadcastToSession(session, ws, {
    type: 'furniture_locked', furnitureId: msg.furnitureId,
    property: msg.property || 'position', playerId: client.playerId
  });

  log('Lock', `granted "${msg.furnitureId}.${msg.property}" to player=${client.playerId}`);
}

function handleFurnitureUnlock(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;

  if (!session.locks) return;
  const key = lockKey(msg.furnitureId, msg.property);
  if (session.locks.get(key) !== client.playerId) return;

  session.locks.delete(key);
  broadcastToSession(session, ws, {
    type: 'furniture_unlocked', furnitureId: msg.furnitureId,
    property: msg.property || 'position'
  });
  log('Lock', `released "${msg.furnitureId}.${msg.property}" by player=${client.playerId}`);
}

function releasePlayerLocks(session, playerId, excludeWs) {
  if (!session.locks) return;
  const toRelease = [];
  for (const [key, pid] of session.locks) {
    if (pid === playerId) toRelease.push(key);
  }
  for (const key of toRelease) {
    session.locks.delete(key);
    const { furnitureId, property } = parseLockKey(key);
    broadcastToSession(session, excludeWs, {
      type: 'furniture_unlocked', furnitureId, property
    });
    log('Lock', `released "${furnitureId}.${property}" (was player=${playerId})`);
  }
}

function handleUpdateState(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;
  if (client.playerId !== session.ownerId) {
    log('Denied', `player=${client.playerId} update_state (not owner)`);
    return;
  }
  const xmlLen = (msg.projectXml || '').length;
  session.projectXml = msg.projectXml || session.projectXml;
  log('Session', `${session.id} state updated by player=${client.playerId} (xml: ${xmlLen} chars, seq: ${session.sequenceNumber})`);
}

function handleLinkPermissionChange(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;
  if (client.playerId !== session.ownerId) {
    log('Denied', `player=${client.playerId} link_permission_change (not owner)`);
    return;
  }

  const permission = msg.linkPermission || 'edit';
  session.linkPermission = permission;

  const n = broadcastToSession(session, null, {
    type: 'link_permission_changed', linkPermission: permission
  });

  log('Session', `${session.id} link permission → ${permission} by player=${client.playerId} → ${n} peers`);
}

// --- Chunk assembly per connection ---
const chunkBuffers = new Map(); // ws → Map<messageId, { chunks: string[], received: number, total: number }>

function handleChunk(ws, msg) {
  if (!chunkBuffers.has(ws)) chunkBuffers.set(ws, new Map());
  const buf = chunkBuffers.get(ws);

  const { messageId, index, total, data } = msg;
  if (!messageId || total <= 0 || index < 0 || index >= total) return null;

  if (!buf.has(messageId)) {
    buf.set(messageId, { chunks: new Array(total).fill(null), received: 0, total });
  }
  const entry = buf.get(messageId);
  if (entry.chunks[index] === null) {
    entry.chunks[index] = data;
    entry.received++;
  }

  if (entry.received < entry.total) return null; // still waiting

  const full = entry.chunks.join('');
  buf.delete(messageId);
  const pid = clients.get(ws)?.playerId ?? '?';
  log('Chunk', `Reassembled ${total} chunks (${full.length} chars) from player=${pid} id=${messageId}`);
  return full;
}

function cleanupChunkBuffers(ws) {
  chunkBuffers.delete(ws);
}

// --- Chunk sending (server → client) ---
const MAX_CHUNK_SIZE = 48 * 1024;
let serverChunkId = 0;

function sendPossiblyChunked(ws, obj) {
  const json = JSON.stringify(obj);
  if (json.length <= MAX_CHUNK_SIZE) {
    ws.send(json);
    return;
  }
  const messageId = String(++serverChunkId);
  const total = Math.ceil(json.length / MAX_CHUNK_SIZE);
  for (let i = 0; i < total; i++) {
    const chunk = json.slice(i * MAX_CHUNK_SIZE, (i + 1) * MAX_CHUNK_SIZE);
    ws.send(JSON.stringify({ type: 'chunk', messageId, index: i, total, data: chunk }));
  }
  log('Chunk', `Sent ${total} chunks (${json.length} chars) id=${messageId}`);
}

// --- Connection ---

wss.on('connection', (ws) => {
  const playerId = nextPlayerId++;
  clients.set(ws, { playerId, sessionId: null, ws });
  log('Connect', `Player ${playerId} connected (total: ${clients.size})`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) {
      log('Error', `Player ${clients.get(ws)?.playerId} sent invalid JSON: ${String(raw).slice(0, 100)}`);
      sendError(ws, 'INVALID_MESSAGE', 'Bad JSON');
      return;
    }
    if (!msg.type) {
      log('Error', `Player ${clients.get(ws)?.playerId} sent message without type`);
      sendError(ws, 'INVALID_MESSAGE', 'Missing type');
      return;
    }

    // Handle chunk assembly
    if (msg.type === 'chunk') {
      const fullJson = handleChunk(ws, msg);
      if (!fullJson) return; // waiting for more chunks
      try { msg = JSON.parse(fullJson); } catch (e) {
        log('Error', `Player ${clients.get(ws)?.playerId} reassembled chunk is invalid JSON`);
        return;
      }
      if (!msg.type) return;
    }

    const client = clients.get(ws);

    // Compact incoming log (skip noisy move/pointer)
    if (msg.type !== 'move' && msg.type !== 'pointer') {
      const fid = (msg.furnitureId || msg.targetId || '').slice(-6);
      const extra = fid ? ` id=..${fid}` : '';
      log('⬇ IN', `p${client.playerId} ${msg.type}${extra}${msg.committed ? ' COMMIT' : ''}`);
    }

    switch (msg.type) {
      case 'create_session': handleCreateSession(ws, client, msg); break;
      case 'join_session':   handleJoinSession(ws, client, msg); break;
      case 'leave_session':  leaveSession(ws, client); break;
      case 'move':           handleMove(ws, client, msg); break;
      case 'pointer':        handlePointer(ws, client, msg); break;
      case 'furniture_update': handleFurnitureUpdate(ws, client, msg); break;
      case 'furniture_add':  handleFurnitureAdd(ws, client, msg); break;
      case 'furniture_remove': handleFurnitureRemove(ws, client, msg); break;
      case 'furniture_change_variation': handleFurnitureChangeVariation(ws, client, msg); break;
      case 'domain_change': handleDomainChange(ws, client, msg); break;
      case 'furniture_lock': handleFurnitureLock(ws, client, msg); break;
      case 'furniture_unlock': handleFurnitureUnlock(ws, client, msg); break;
      case 'update_state':   handleUpdateState(ws, client, msg); break;
      case 'link_permission_change': handleLinkPermissionChange(ws, client, msg); break;
      default:
        log('Warn', `Player ${client.playerId} sent unknown type: "${msg.type}"`);
        break;
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client?.sessionId) leaveSession(ws, client);
    clients.delete(ws);
    cleanupChunkBuffers(ws);
    log('Disconnect', `Player ${client?.playerId} disconnected (total: ${clients.size})`);
  });

  ws.on('error', (err) => {
    const client = clients.get(ws);
    log('Error', `Player ${client?.playerId} websocket error: ${err.message}`);
  });
});

// --- Inline bots ---

const BOT_NAMES = ['Luna', 'Ricardo', 'Emma', 'Mark', 'Daniel', 'Sophia', 'Alex', 'Mia', 'Leo', 'Zara'];
const BOT_MOVE_INTERVAL = 200;
const BOT_WALK_SPEED = 0.2;          // ~1 unit/sec at 200ms interval (matches WASD speed)
const BOT_DIR_CHANGE = 0.03;
const BOT_PAUSE_CHANCE = 0.01;
const BOT_PAUSE_TICKS = 15;
const BOT_LOOK_SPEED = 4;
const BOT_ROOM_CHANGE = 0.005;
const BOT_WALL_MARGIN = 0.4;

function parseRoomsFromXml(xml) {
  const rooms = [];
  const floorRe = /<Floor[^>]*>[\s\S]*?<Shape>([\s\S]*?)<\/Shape>/g;
  let m;
  while ((m = floorRe.exec(xml)) !== null) {
    const verts = [];
    const vRe = /<Vector2\s+x="([^"]+)"\s+y="([^"]+)"/g;
    let v;
    while ((v = vRe.exec(m[1])) !== null) {
      verts.push({ x: parseFloat(v[1]), z: parseFloat(v[2]) });
    }
    if (verts.length >= 3) rooms.push(verts);
  }
  return rooms;
}

function ptInPoly(px, pz, poly) {
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const xi = poly[i].x, zi = poly[i].z, xj = poly[j].x, zj = poly[j].z;
    if ((zi > pz) !== (zj > pz) && px < (xj - xi) * (pz - zi) / (zj - zi) + xi) inside = !inside;
  }
  return inside;
}

function polyArea(poly) {
  let area = 0;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    area += (poly[j].x + poly[i].x) * (poly[j].z - poly[i].z);
  }
  return Math.abs(area / 2);
}

function polyCenter(poly) {
  let cx = 0, cz = 0;
  for (const v of poly) { cx += v.x; cz += v.z; }
  return { x: cx / poly.length, z: cz / poly.length };
}

function randInPoly(poly) {
  let minX = Infinity, maxX = -Infinity, minZ = Infinity, maxZ = -Infinity;
  for (const v of poly) { minX = Math.min(minX, v.x); maxX = Math.max(maxX, v.x); minZ = Math.min(minZ, v.z); maxZ = Math.max(maxZ, v.z); }
  for (let i = 0; i < 100; i++) {
    const px = minX + Math.random() * (maxX - minX), pz = minZ + Math.random() * (maxZ - minZ);
    if (isInsideWithMargin(px, pz, poly, BOT_WALL_MARGIN)) return { x: px, z: pz };
  }
  return polyCenter(poly);
}

/** Check if point is at least `margin` away from all polygon edges. */
function isInsideWithMargin(px, pz, poly, margin) {
  if (!ptInPoly(px, pz, poly)) return false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const ax = poly[j].x, az = poly[j].z;
    const bx = poly[i].x, bz = poly[i].z;
    const dx = bx - ax, dz = bz - az;
    const len2 = dx * dx + dz * dz;
    if (len2 === 0) continue;
    let t = ((px - ax) * dx + (pz - az) * dz) / len2;
    t = Math.max(0, Math.min(1, t));
    const cx = ax + t * dx, cz = az + t * dz;
    const dist = Math.sqrt((px - cx) * (px - cx) + (pz - cz) * (pz - cz));
    if (dist < margin) return false;
  }
  return true;
}

function spawnSessionBots(inviteCode, projectXml) {
  if (BOT_COUNT <= 0) return;

  const rooms = parseRoomsFromXml(projectXml || '');
  log('Bots', `Parsed ${rooms.length} room(s), spawning ${BOT_COUNT} bots for invite: ${inviteCode}`);
  for (let r = 0; r < rooms.length; r++) {
    const c = polyCenter(rooms[r]);
    const verts = rooms[r].map(v => `(${v.x.toFixed(2)},${v.z.toFixed(2)})`).join(' ');
    log('Bots', `  Room ${r}: center=(${c.x.toFixed(2)},${c.z.toFixed(2)}) verts=[${verts}]`);
  }

  for (let i = 0; i < BOT_COUNT; i++) {
    setTimeout(() => {
      const botWs = new WebSocket(`ws://localhost:${PORT}`);
      const name = BOT_NAMES[i % BOT_NAMES.length];
      // Default to smallest room (skip outer boundary)
      let currentRoom = null;
      if (rooms.length > 0) {
        let minArea = Infinity;
        for (const r of rooms) {
          const a = polyArea(r);
          if (a < minArea) { minArea = a; currentRoom = r; }
        }
      }

      let x = 0, z = 0, y = 0;
      let dirX = (Math.random() - 0.5) * 2, dirZ = (Math.random() - 0.5) * 2;
      let len = Math.sqrt(dirX * dirX + dirZ * dirZ) || 1; dirX /= len; dirZ /= len;
      let rotY = Math.random() * 360;
      let paused = false, pauseTicks = 0, lookDir = 1;
      let timer = null;

      botWs.on('open', () => {
        botWs.send(JSON.stringify({ type: 'join_session', inviteCode, userName: name }));
      });

      botWs.on('message', (raw) => {
        let msg; try { msg = JSON.parse(raw); } catch { return; }
        if (msg.type === 'session_state') {
          // Spawn at owner's position
          const owner = (msg.presence || []).find(p => p.role === 'owner');
          if (owner && owner.position) {
            x = owner.position.x || 0;
            y = owner.position.y || 0;
            z = owner.position.z || 0;
          }
          // Find the smallest room containing the owner (skip outer boundary)
          if (rooms.length > 0) {
            let bestRoom = null;
            let bestArea = Infinity;
            for (const r of rooms) {
              if (ptInPoly(x, z, r)) {
                const area = polyArea(r);
                if (area < bestArea) { bestArea = area; bestRoom = r; }
              }
            }
            if (bestRoom) currentRoom = bestRoom;
          }
          // Make sure bot starts inside room
          if (currentRoom && !isInsideWithMargin(x, z, currentRoom, BOT_WALL_MARGIN)) {
            const p = randInPoly(currentRoom);
            x = p.x; z = p.z;
          }
          const inRoom = currentRoom ? ptInPoly(x, z, currentRoom) : 'no-room';
          log('Bots', `${name} joined as player ${msg.playerId} at (${x.toFixed(2)}, ${y.toFixed(2)}, ${z.toFixed(2)}) inRoom=${inRoom}`);
          timer = setInterval(() => {
            if (botWs.readyState !== WebSocket.OPEN) { clearInterval(timer); return; }

            // Pause / look around
            if (!paused && Math.random() < BOT_PAUSE_CHANCE) {
              paused = true;
              pauseTicks = BOT_PAUSE_TICKS + Math.floor(Math.random() * BOT_PAUSE_TICKS);
              lookDir = Math.random() < 0.5 ? 1 : -1;
            }

            if (paused) {
              rotY += BOT_LOOK_SPEED * lookDir;
              if (Math.random() < 0.08) lookDir *= -1;
              pauseTicks--;
              if (pauseTicks <= 0) paused = false;
            } else {
              // Switch rooms
              if (rooms.length > 1 && Math.random() < BOT_ROOM_CHANGE) {
                currentRoom = rooms[Math.floor(Math.random() * rooms.length)];
                const p = randInPoly(currentRoom); x = p.x; z = p.z;
              }
              // Direction change
              if (Math.random() < BOT_DIR_CHANGE) {
                dirX = (Math.random() - 0.5) * 2; dirZ = (Math.random() - 0.5) * 2;
                len = Math.sqrt(dirX * dirX + dirZ * dirZ) || 1; dirX /= len; dirZ /= len;
              }
              const nx = x + dirX * BOT_WALK_SPEED, nz = z + dirZ * BOT_WALK_SPEED;
              if (currentRoom && isInsideWithMargin(nx, nz, currentRoom, BOT_WALL_MARGIN)) {
                x = nx; z = nz;
              } else if (!currentRoom) {
                x = nx; z = nz;
                const d = Math.sqrt(x * x + z * z);
                if (d > 8) { dirX = -x / d; dirZ = -z / d; len = Math.sqrt(dirX * dirX + dirZ * dirZ) || 1; dirX /= len; dirZ /= len; }
              } else {
                // Hit wall — pick new random direction, do NOT move
                dirX = (Math.random() - 0.5) * 2; dirZ = (Math.random() - 0.5) * 2;
                len = Math.sqrt(dirX * dirX + dirZ * dirZ) || 1; dirX /= len; dirZ /= len;
              }
              rotY = Math.atan2(dirX, dirZ) * 180 / Math.PI;
            }

            botWs.send(JSON.stringify({ type: 'move', position: { x, y, z }, rotation: { x: 0, y: rotY, z: 0 } }));
          }, BOT_MOVE_INTERVAL);
        }
        if (msg.type === 'session_error') log('Bots', `${name} error: ${msg.code}`);
      });

      botWs.on('close', () => { if (timer) clearInterval(timer); });
      botWs.on('error', () => {});
    }, i * 1000);
  }
}

// --- UDP Discovery ---

const DISCOVERY_PORT = 3001;
const DISCOVERY_MSG = 'MULTIPLAYER_DISCOVER';
const udpDiscovery = dgram.createSocket('udp4');

udpDiscovery.on('message', (msg, rinfo) => {
  if (msg.toString() === DISCOVERY_MSG) {
    const response = JSON.stringify({ type: 'MULTIPLAYER_DISCOVER_RESPONSE', wsPort: PORT });
    udpDiscovery.send(response, rinfo.port, rinfo.address, (err) => {
      if (err) log('Discovery', `Error responding: ${err.message}`);
      else log('Discovery', `Responded to ${rinfo.address}:${rinfo.port} (wsPort: ${PORT})`);
    });
  }
});

udpDiscovery.on('error', (err) => {
  log('Discovery', `UDP error: ${err.message}`);
});

udpDiscovery.bind(DISCOVERY_PORT, () => {
  log('Discovery', `UDP discovery listening on port ${DISCOVERY_PORT}`);
});

// --- Start ---

server.listen(PORT, () => {
  log('Server', `Multiplayer server running on port ${PORT}`);
  log('Server', `Auto-bots: ${BOT_COUNT} per session`);
});
