const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const LEGACY_INVITE = '__legacy__';

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Multiplayer server OK. Sessions: ${sessions.size}, Clients: ${clients.size}`);
});

const wss = new WebSocket.Server({ server });

// --- State ---

let nextPlayerId = 1;
const sessions = new Map();  // inviteCode -> Session
const clients = new Map();   // WebSocket -> ClientState

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

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
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
  return role === 'owner' || role === 'co-author' || role === 'guest-edit';
}

function sessionInfo(session) {
  return `${session.id} (players: ${session.players.size}, seq: ${session.sequenceNumber})`;
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
      players: new Map()
    };
    sessions.set(LEGACY_INVITE, session);
    log('Legacy', 'Session created');
  }

  const player = {
    playerId: client.playerId, userId: null,
    userName: `Player ${client.playerId}`, role: 'owner',
    position: { x: 0, y: 0, z: 0 }, rotation: { y: 0 }, ws
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

  const player = {
    playerId: client.playerId, userId: msg.userId || null,
    userName: msg.userName || 'Owner', role: 'owner',
    position: { x: 0, y: 0, z: 0 }, rotation: { y: 0 }, ws
  };

  const session = {
    id: sessionId, inviteCode, ownerId: client.playerId,
    ownerUserId: msg.userId || null, projectXml: msg.projectXml || '',
    linkPermission, sequenceNumber: 0,
    players: new Map([[client.playerId, player]])
  };

  sessions.set(inviteCode, session);
  client.sessionId = sessionId;

  send(ws, {
    type: 'session_created', inviteCode, sessionId,
    sequenceNumber: 0, playerId: client.playerId
  });

  log('Session', `Created ${sessionId} (invite: ${inviteCode}, permission: ${linkPermission}) by player ${client.playerId}`);
}

function handleJoinSession(ws, client, msg) {
  if (client.sessionId) { sendError(ws, 'ALREADY_IN_SESSION', 'Already in a session'); return; }

  const session = sessions.get(msg.inviteCode);
  if (!session) { sendError(ws, 'NOT_FOUND', `Session not found: ${msg.inviteCode}`); return; }
  if (session.linkPermission === 'none') { sendError(ws, 'NO_ACCESS', 'Link disabled'); return; }
  if (session.players.size >= 25) { sendError(ws, 'SESSION_FULL', 'Max 25 players'); return; }

  let role = session.linkPermission === 'edit' ? 'guest-edit' : 'guest-view';
  if (msg.userId && msg.userId === session.ownerUserId) role = 'owner';

  const player = {
    playerId: client.playerId, userId: msg.userId || null,
    userName: msg.userName || 'Guest', role,
    position: { x: 0, y: 0, z: 0 }, rotation: { y: 0 }, ws
  };

  session.players.set(client.playerId, player);
  client.sessionId = session.id;

  const presence = [];
  for (const [, p] of session.players) {
    presence.push({
      playerId: p.playerId, userId: p.userId, userName: p.userName,
      role: p.role, position: p.position, rotation: p.rotation
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
    position: player.position, rotation: player.rotation
  });

  log('Session', `Player ${client.playerId} joined ${session.id} as ${role} (total: ${session.players.size})`);
}

function leaveSession(ws, client) {
  if (!client.sessionId) return;
  const session = findSessionById(client.sessionId);
  client.sessionId = null;
  if (!session) return;

  session.players.delete(client.playerId);

  if (client.playerId === session.ownerId) {
    // Owner left → close session
    broadcastToSession(session, null, { type: 'session_closed', reason: 'owner_left' });
    for (const [, p] of session.players) {
      const c = clients.get(p.ws);
      if (c) c.sessionId = null;
    }
    sessions.delete(session.inviteCode);
    log('Session', `${session.id} closed (owner left, kicked ${session.players.size} players)`);
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

function handleFurnitureMove(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} furniture_move (role: ${role})`);
    return;
  }
  if (msg.committed) session.sequenceNumber++;

  const n = broadcastToSession(session, ws, {
    type: 'furniture_move', playerId: client.playerId,
    furnitureId: msg.furnitureId, position: msg.position,
    rotation: msg.rotation, planeOffset: msg.planeOffset,
    committed: msg.committed
  });

  if (msg.committed)
    log('Furniture', `move committed "${msg.furnitureId}" by player=${client.playerId} → ${n} peers (seq: ${session.sequenceNumber})`);
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
    position: msg.position, rotation: msg.rotation,
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

function handleMaterialChange(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} material_change (role: ${role})`);
    return;
  }
  session.sequenceNumber++;

  const n = broadcastToSession(session, ws, {
    type: 'material_change', playerId: client.playerId,
    targetId: msg.targetId, targetType: msg.targetType,
    materialPath: msg.materialPath, categoryId: msg.categoryId || null
  });

  log('Material', `change target="${msg.targetId}" type="${msg.targetType}" by player=${client.playerId} → ${n} peers`);
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

    const client = clients.get(ws);

    switch (msg.type) {
      case 'create_session': handleCreateSession(ws, client, msg); break;
      case 'join_session':   handleJoinSession(ws, client, msg); break;
      case 'leave_session':  leaveSession(ws, client); break;
      case 'move':           handleMove(ws, client, msg); break;
      case 'pointer':        handlePointer(ws, client, msg); break;
      case 'furniture_move': handleFurnitureMove(ws, client, msg); break;
      case 'furniture_add':  handleFurnitureAdd(ws, client, msg); break;
      case 'furniture_remove': handleFurnitureRemove(ws, client, msg); break;
      case 'furniture_change_variation': handleFurnitureChangeVariation(ws, client, msg); break;
      case 'material_change': handleMaterialChange(ws, client, msg); break;
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
    log('Disconnect', `Player ${client?.playerId} disconnected (total: ${clients.size})`);
  });

  ws.on('error', (err) => {
    const client = clients.get(ws);
    log('Error', `Player ${client?.playerId} websocket error: ${err.message}`);
  });
});

server.listen(PORT, () => {
  log('Server', `Multiplayer server running on port ${PORT}`);
  log('Server', 'Legacy mode: clients auto-join default session on first message');
});
