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
  send(ws, { type: 'session_error', code, message });
}

function findSessionById(id) {
  for (const [, s] of sessions) { if (s.id === id) return s; }
  return null;
}

function broadcastToSession(session, excludeWs, obj) {
  const data = JSON.stringify(obj);
  for (const [, p] of session.players) {
    if (p.ws !== excludeWs && p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
  }
}

function canEdit(role) {
  return role === 'owner' || role === 'co-author' || role === 'guest-edit';
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
    console.log('[Legacy] Session created');
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

  console.log(`[Legacy] Player ${client.playerId} auto-joined (total: ${session.players.size})`);
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

  console.log(`[Session] Created ${sessionId} (invite: ${inviteCode}) by player ${client.playerId}`);
}

function handleJoinSession(ws, client, msg) {
  if (client.sessionId) { sendError(ws, 'ALREADY_IN_SESSION', 'Already in a session'); return; }

  const session = sessions.get(msg.inviteCode);
  if (!session) { sendError(ws, 'NOT_FOUND', 'Session not found'); return; }
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

  console.log(`[Session] Player ${client.playerId} joined ${session.id} as ${role} (total: ${session.players.size})`);
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
    console.log(`[Session] ${session.id} closed (owner left)`);
  } else {
    broadcastToSession(session, ws, { type: 'player_left', playerId: client.playerId });
    console.log(`[Session] Player ${client.playerId} left ${session.id} (total: ${session.players.size})`);
  }

  // Clean up empty legacy session
  if (session.inviteCode === LEGACY_INVITE && session.players.size === 0) {
    sessions.delete(LEGACY_INVITE);
    console.log('[Legacy] Session removed (empty)');
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
  if (!canEdit(role)) return;
  if (msg.committed) session.sequenceNumber++;

  broadcastToSession(session, ws, {
    type: 'furniture_move', playerId: client.playerId,
    furnitureId: msg.furnitureId, position: msg.position,
    rotation: msg.rotation, planeOffset: msg.planeOffset,
    committed: msg.committed
  });
}

function handleFurnitureAdd(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) return;
  session.sequenceNumber++;

  broadcastToSession(session, ws, {
    type: 'furniture_add', playerId: client.playerId,
    furnitureId: msg.furnitureId, variationPath: msg.variationPath,
    position: msg.position, rotation: msg.rotation,
    planeOffset: msg.planeOffset, parentId: msg.parentId
  });
}

function handleFurnitureRemove(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) return;
  session.sequenceNumber++;

  broadcastToSession(session, ws, {
    type: 'furniture_remove', playerId: client.playerId,
    furnitureId: msg.furnitureId
  });
}

function handleFurnitureChangeVariation(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) return;
  session.sequenceNumber++;

  broadcastToSession(session, ws, {
    type: 'furniture_change_variation', playerId: client.playerId,
    furnitureId: msg.furnitureId, variationPath: msg.variationPath
  });
}

function handleMaterialChange(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) return;
  session.sequenceNumber++;

  broadcastToSession(session, ws, {
    type: 'material_change', playerId: client.playerId,
    targetId: msg.targetId, targetType: msg.targetType,
    materialPath: msg.materialPath, categoryId: msg.categoryId || null
  });
}

function handleUpdateState(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;
  if (client.playerId !== session.ownerId) return;
  session.projectXml = msg.projectXml || session.projectXml;
  console.log(`[Session] ${session.id} state updated (seq: ${session.sequenceNumber})`);
}

// --- Connection ---

wss.on('connection', (ws) => {
  const playerId = nextPlayerId++;
  clients.set(ws, { playerId, sessionId: null, ws });
  console.log(`[+] Player ${playerId} connected (total: ${clients.size})`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { sendError(ws, 'INVALID_MESSAGE', 'Bad JSON'); return; }
    if (!msg.type) { sendError(ws, 'INVALID_MESSAGE', 'Missing type'); return; }

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
      default: break; // Unknown types silently ignored
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client?.sessionId) leaveSession(ws, client);
    clients.delete(ws);
    console.log(`[-] Player ${client?.playerId} disconnected (total: ${clients.size})`);
  });
});

server.listen(PORT, () => {
  console.log(`Multiplayer server running on port ${PORT}`);
  console.log('Legacy mode: clients auto-join default session on first message');
});
