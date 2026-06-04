const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto'); // DEBUG_STATE_VERIFIER
const projectsModule = require('./projects');

const PORT = process.env.PORT || 3000;
const LEGACY_INVITE = '__legacy__';
let BOT_COUNT = 0; // bots auto-spawned per session (0 to disable)
let BOT_VIEW_MODE = 'random'; // 'random', '2d', '3d', 'panorama' — forced view mode for bots
let BOT_REJOIN = true; // whether bots disconnect after session time and reconnect
let BOT_MOBILE_MODE = 'all'; // 'all' (every bot is mobile) or 'random' (50/50 mobile/desktop)

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

  // GET /bots?count=2 — spawn bots in all active sessions
  // GET /bots?count=0 — disable bots (new sessions won't get bots)
  // GET /bots?mode=2d|3d|panorama|random — force bot view mode
  // GET /bots?mobile=all|random — toggle bot mobile flag (all=every bot mobile, random=50/50)
  // GET /bots?rejoin=0|1 — toggle disconnect/reconnect cycle (0 keeps bots online forever)
  // GET /bots?sessionMin=N&sessionMax=N — bot session duration in seconds (online time)
  // GET /bots?offlineMin=N&offlineMax=N — bot offline duration in seconds (before reconnect)
  // GET /bots — show current settings
  if (url.pathname === '/bots') {
    const count = url.searchParams.get('count');
    const mode = url.searchParams.get('mode');
    const mobile = url.searchParams.get('mobile');
    const rejoin = url.searchParams.get('rejoin');
    const sessionMin = url.searchParams.get('sessionMin');
    const sessionMax = url.searchParams.get('sessionMax');
    const offlineMin = url.searchParams.get('offlineMin');
    const offlineMax = url.searchParams.get('offlineMax');
    const reset = url.searchParams.get('reset');

    if (count !== null) {
      BOT_COUNT = Math.max(0, Math.min(10, parseInt(count) || 0));
      if (BOT_COUNT > 0 && reset === null) {
        for (const [code, s] of sessions) {
          if (code === LEGACY_INVITE) continue;
          spawnSessionBots(code, s.projectXml);
        }
      }
      log('Bots', `Bot count set to ${BOT_COUNT}`);
    }
    if (mode !== null && ['2d', '3d', 'panorama', 'random'].includes(mode)) {
      BOT_VIEW_MODE = mode;
      log('Bots', `Bot view mode set to ${BOT_VIEW_MODE}`);
    }
    if (mobile !== null && ['all', 'random'].includes(mobile)) {
      BOT_MOBILE_MODE = mobile;
      log('Bots', `Bot mobile mode set to ${BOT_MOBILE_MODE}`);
    }
    if (rejoin !== null) {
      BOT_REJOIN = !(rejoin === '0' || rejoin === 'false');
      log('Bots', `Bot rejoin set to ${BOT_REJOIN}`);
    }
    if (sessionMin !== null) {
      const v = Math.max(1, parseInt(sessionMin) || BOT_MIN_ONLINE_SEC);
      BOT_MIN_ONLINE_SEC = v;
      if (BOT_MAX_ONLINE_SEC < v) BOT_MAX_ONLINE_SEC = v;
      log('Bots', `Bot sessionMin set to ${BOT_MIN_ONLINE_SEC}s`);
    }
    if (sessionMax !== null) {
      const v = Math.max(BOT_MIN_ONLINE_SEC, parseInt(sessionMax) || BOT_MAX_ONLINE_SEC);
      BOT_MAX_ONLINE_SEC = v;
      log('Bots', `Bot sessionMax set to ${BOT_MAX_ONLINE_SEC}s`);
    }
    if (offlineMin !== null) {
      const v = Math.max(0, parseInt(offlineMin) || BOT_MIN_OFFLINE_SEC);
      BOT_MIN_OFFLINE_SEC = v;
      if (BOT_MAX_OFFLINE_SEC < v) BOT_MAX_OFFLINE_SEC = v;
      log('Bots', `Bot offlineMin set to ${BOT_MIN_OFFLINE_SEC}s`);
    }
    if (offlineMax !== null) {
      const v = Math.max(BOT_MIN_OFFLINE_SEC, parseInt(offlineMax) || BOT_MAX_OFFLINE_SEC);
      BOT_MAX_OFFLINE_SEC = v;
      log('Bots', `Bot offlineMax set to ${BOT_MAX_OFFLINE_SEC}s`);
    }

    if (reset !== null && reset !== '0' && reset !== 'false') {
      const managerCodes = Array.from(sessionBotManagers.keys());
      for (const code of managerCodes) stopBotManager(code);

      let strayKicked = 0;
      for (const [ws, c] of clients) {
        if (c && c.isBot) {
          try { ws.close(); } catch (e) {}
          strayKicked++;
        }
      }
      log('Bots', `RESET: stopped ${managerCodes.length} manager(s), kicked ${strayKicked} stray bot ws`);

      if (BOT_COUNT > 0) {
        for (const [code, s] of sessions) {
          if (code === LEGACY_INVITE) continue;
          if (!hasHumanPlayers(s)) continue;
          spawnSessionBots(code, s.projectXml);
        }
        log('Bots', `RESET: respawned bots for active sessions (count=${BOT_COUNT})`);
      }
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      botCount: BOT_COUNT,
      viewMode: BOT_VIEW_MODE,
      mobileMode: BOT_MOBILE_MODE,
      rejoin: BOT_REJOIN,
      sessionMin: BOT_MIN_ONLINE_SEC,
      sessionMax: BOT_MAX_ONLINE_SEC,
      offlineMin: BOT_MIN_OFFLINE_SEC,
      offlineMax: BOT_MAX_OFFLINE_SEC
    }));
    return;
  }

  // GET /rooms — show parsed rooms for first active session
  if (url.pathname === '/rooms') {
    for (const [code, s] of sessions) {
      if (code === LEGACY_INVITE) continue;
      const allRooms = parseRoomsFromXml(s.projectXml || '');
      const indoor = filterIndoorRooms(allRooms);
      const data = allRooms.map((r, i) => {
        const c = polyCenter(r);
        const a = polyArea(r);
        return { index: i, center: { x: +c.x.toFixed(1), z: +c.z.toFixed(1) }, area: +a.toFixed(0), indoor: indoor.includes(r) };
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ rooms: data }, null, 2));
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ rooms: [], error: 'No active session' }));
    return;
  }

  // --- Test coordination (GET/POST /test) ---
  if (url.pathname === '/test') {
    if (req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(testCoordination));
      return;
    }
    if (req.method === 'POST') {
      let body = '';
      req.on('data', chunk => body += chunk);
      req.on('end', () => {
        try {
          testCoordination = JSON.parse(body);
          log('Test', `Coordination updated: phase=${testCoordination.phase}`);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true }));
        } catch (e) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
  }

  if (projectsModule.handleRequest(req, res, url, sessions, projectIndex)) return;

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Multiplayer server OK. Sessions: ${sessions.size}, Clients: ${clients.size}`);
});

const wss = new WebSocket.Server({ server });

// --- State ---

let nextPlayerId = 1;
const sessions = new Map();  // inviteCode -> Session
const projectIndex = new Map();  // projectId -> inviteCode (only for sessions with non-empty projectId)
const clients = new Map();   // WebSocket -> ClientState
const wsLastSeen = new Map(); // WebSocket -> timestamp of last received message
let testCoordination = { phase: 'idle' };  // Test coordination state (GET/POST /test)

// After 20s (2× ping interval) with no message, a connection is considered a zombie.
// Regular clients send Move ~100ms and Ping every 10s, so legitimate connections are always fresh.
const ZOMBIE_THRESHOLD_MS = 20_000;

// Kick all stale (zombie) players in a session except the given excludeWs.
// Called on new JoinSession to clean up internet-drop zombies before adding the rejoining player.
function kickStaleSessionPlayers(session, excludeWs) {
  const now = Date.now();
  const toKick = [];
  for (const [, p] of session.players) {
    if (p.ws === excludeWs) continue;
    const last = wsLastSeen.get(p.ws) ?? 0;
    if (now - last > ZOMBIE_THRESHOLD_MS)
      toKick.push({ ws: p.ws, playerId: p.playerId, idleSec: Math.round((now - last) / 1000) });
  }
  for (const { ws: deadWs, playerId, idleSec } of toKick) {
    const c = clients.get(deadWs);
    log('Session', `Kicking zombie player ${playerId} (no message for ${idleSec}s)`);
    if (c) leaveSession(deadWs, c);
    wsLastSeen.delete(deadWs);
    clients.delete(deadWs);
    try { deadWs.terminate(); } catch (e) {}
  }
}

// Periodic sweep — belt-and-suspenders cleanup when no new joins trigger kickStaleSessionPlayers.
setInterval(() => {
  const now = Date.now();
  for (const [ws, t] of [...wsLastSeen]) {
    if (now - t <= ZOMBIE_THRESHOLD_MS) continue;
    const client = clients.get(ws);
    if (!client) { wsLastSeen.delete(ws); continue; }
    log('Sweep', `Closing stale ws for player ${client.playerId} (${Math.round((now - t) / 1000)}s idle)`);
    wsLastSeen.delete(ws);
    if (client.sessionId) leaveSession(ws, client);
    clients.delete(ws);
    try { ws.terminate(); } catch (e) {}
  }
}, 5_000);

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
  send(ws, { type: 'SessionError', code, message });
}

function findSessionById(id) {
  for (const [, s] of sessions) { if (s.id === id) return s; }
  return null;
}

function broadcastToSession(session, excludeWs, obj) {
  const data = JSON.stringify(obj);
  let count = 0;
  for (const [, p] of session.players) {
    if (p.ws === excludeWs) continue;
    if (p.ws.readyState === WebSocket.OPEN) {
      p.ws.send(data);
      count++;
    }
  }
  return count;
}

function sendSessionStateTo(session, ws, role) {
  const client = clients.get(ws);
  if (!client || ws.readyState !== WebSocket.OPEN) return;

  const presence = [];
  for (const [, p] of session.players) {
    // Skip players who haven't sent their first real position yet — others will
    // see them appear via deferred PlayerJoined broadcast (sent on first non-zero Move).
    if (p.pendingJoinedBroadcast && p.ws !== ws) continue;
    presence.push({
      playerId: p.playerId, userId: p.userId, userName: p.userName,
      role: p.role, color: p.color, avatarUrl: p.avatarUrl || '',
      position: p.position, rotation: p.rotation,
      viewMode: p.viewMode || '3d',
      isMobile: !!p.isMobile
    });
  }

  ensureSelectionMaps(session);
  const selections = [];
  for (const [targetId, playerIds] of session.selections)
    for (const playerId of playerIds)
      selections.push({ playerId, targetId });

  send(ws, {
    type: 'SessionState', projectXml: session.projectXml,
    presence, role,
    playerId: client.playerId, inviteCode: session.inviteCode,
    selections
  });
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
      entityState: new Map(),
      selections: new Map(),
      playerSelections: new Map(),
      lastActivityByPlayer: new Map()
    };
    sessions.set(LEGACY_INVITE, session);
    log('Legacy', 'Session created');
  }

  const player = {
    playerId: client.playerId, userId: null,
    userName: `Designer ${client.playerId}`, role: 'owner',
    position: { x: 0, y: 0, z: 0 }, rotation: { x: 0, y: 0, z: 0 },
    viewMode: '3d', isMobile: false, ws,
    pendingJoinedBroadcast: true
  };
  session.players.set(client.playerId, player);
  client.sessionId = session.id;

  const existing = [];
  for (const [, p] of session.players) {
    if (p.playerId !== client.playerId && !p.pendingJoinedBroadcast)
      existing.push({
        id: p.playerId, userName: p.userName, color: p.color,
        position: p.position, rotation: p.rotation,
        viewMode: p.viewMode || '3d',
        isMobile: !!p.isMobile
      });
  }
  send(ws, { type: 'Welcome', playerId: client.playerId, role: player.role, players: existing });

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

// Resolves the wire role ('owner' | 'editor' | 'viewer') for a joining player from the project DB.
// Source of truth: projects.owner_user_id, project_users.role (per-user override), projects.global_role.
async function resolveRoleFromDb(projectId, userId) {
  const projRow = await projectsModule.getProjectRow(projectId);
  if (!projRow) return { error: 'NOT_FOUND', message: `Project ${projectId} not registered` };

  if (userId && userId === projRow.owner_user_id) return { role: 'owner' };

  let effective;
  if (userId) {
    const userRow = await projectsModule.getProjectUserRow(projectId, userId);
    effective = projectsModule.getEffectiveRole(projRow, userRow);
  } else {
    effective = projRow.global_role; // guest with no userId — inherits project-level role
  }

  if (effective === 'owner') return { role: 'owner' };
  if (effective === 'can_edit') return { role: 'editor' };
  return { role: 'viewer' };
}

async function handleCreateSession(ws, client, msg) {
  if (client.sessionId) { sendError(ws, 'ALREADY_IN_SESSION', 'Already in a session'); return; }

  const projectId = msg.projectId || null;

  // Reuse existing session if one is already linked to this projectId
  if (projectId && projectIndex.has(projectId)) {
    const inviteCode = projectIndex.get(projectId);
    const existing = sessions.get(inviteCode);
    if (!existing) {
      // Defensive: stale index entry — drop it and fall through to create
      projectIndex.delete(projectId);
    } else {
      const isOwnerRejoin = msg.userId && msg.userId === existing.ownerUserId;

      if (existing.players.size >= 25) {
        sendError(ws, 'SESSION_FULL', 'Max 25 players');
        return;
      }

      // Owner-rejoin ownership reset: refresh ownerId to the new playerId so the
      // session host (used for leave-time ownership transfer) tracks the returning owner.
      if (isOwnerRejoin) {
        existing.ownerId = client.playerId;
      }

      msg.inviteCode = inviteCode;
      log('Session', `Session reuse: project=${projectId} → invite=${inviteCode}, player ${client.playerId}`);
      return handleJoinSession(ws, client, msg);
    }
  }

  // Resolve link permission from the project DB row (single source of truth).
  // For sessions with no projectId (anonymous/test path) the legacy msg.linkPermission is used.
  let linkPermission;
  if (projectId) {
    const projRow = await projectsModule.getProjectRow(projectId);
    if (!projRow) {
      sendError(ws, 'NOT_FOUND', `Project ${projectId} not registered`);
      return;
    }
    linkPermission = projRow.global_role === 'can_edit' ? 'edit' : 'view';
  } else {
    linkPermission = msg.linkPermission || 'edit';
  }

  const inviteCode = generateCode();
  const sessionId = 'sess_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

  // Create session first (needed for pickColor)
  const session = {
    id: sessionId, inviteCode, ownerId: client.playerId,
    ownerUserId: msg.userId || null, projectXml: msg.projectXml || '',
    linkPermission, sequenceNumber: 0,
    projectId: projectId,
    players: new Map(),
    entityState: new Map(),
    selections: new Map(),         // targetId -> playerId
    playerSelections: new Map(),   // playerId -> targetId
    lastActivityByPlayer: new Map()// playerId -> Date.now()
  };

  const player = {
    playerId: client.playerId, userId: msg.userId || null,
    userName: msg.userName || `Designer ${client.playerId}`, role: 'owner',
    color: pickColor(session), avatarUrl: msg.avatarUrl || '',
    position: { x: 0, y: 0, z: 0 }, rotation: { x: 0, y: 0, z: 0 },
    viewMode: '3d', isMobile: !!msg.isMobile, ws
  };

  session.players.set(client.playerId, player);

  sessions.set(inviteCode, session);
  if (projectId) projectIndex.set(projectId, inviteCode);
  client.sessionId = sessionId;

  send(ws, {
    type: 'SessionCreated', inviteCode,
    playerId: client.playerId,
    color: player.color
  });

  log('Session', `Created ${sessionId} (invite: ${inviteCode}, permission: ${linkPermission}) by player ${client.playerId}`);

  // Auto-spawn bots after owner has time to load and send position
  setTimeout(() => spawnSessionBots(inviteCode, session.projectXml), 5000);
}

async function handleJoinSession(ws, client, msg) {
  if (client.sessionId) { sendError(ws, 'ALREADY_IN_SESSION', 'Already in a session'); return; }

  const session = sessions.get(msg.inviteCode);
  if (!session) { sendError(ws, 'NOT_FOUND', `Session not found: ${msg.inviteCode}`); return; }

  // Reconnect: kick zombie by userId (authenticated users) OR by stale wsLastSeen (anonymous users).
  if (msg.userId && !msg.isBot) {
    for (const [, existingPlayer] of session.players) {
      if (existingPlayer.userId !== msg.userId) continue;
      if (existingPlayer.ws === ws) continue;
      const oldWs = existingPlayer.ws;
      const oldClient = clients.get(oldWs);
      log('Session', `Reconnect: closing zombie ws for userId=${msg.userId} (old playerId=${existingPlayer.playerId})`);
      if (oldClient) leaveSession(oldWs, oldClient);
      wsLastSeen.delete(oldWs);
      clients.delete(oldWs);
      try { oldWs.terminate(); } catch (e) {}
      break;
    }
  }

  // For anonymous users (no userId) remove any stale zombie connections in this session.
  // Handles the internet-drop case where TCP didn't close cleanly and the old WS still appears OPEN.
  if (!msg.isBot) kickStaleSessionPlayers(session, ws);

  if (session.players.size >= 25) { sendError(ws, 'SESSION_FULL', 'Max 25 players'); return; }

  if (msg.isBot) client.isBot = true;
  if (msg.isMirror) client.isMirror = true;

  // Resolve role from the project DB (single source of truth). Bots and DB-less sessions
  // (anonymous/test invites) fall back to a derived rule.
  let role;
  if (msg.isBot) {
    role = 'editor';
  } else if (session.projectId) {
    const resolved = await resolveRoleFromDb(session.projectId, msg.userId);
    if (resolved.error) { sendError(ws, resolved.error, resolved.message); return; }
    role = resolved.role;
  } else {
    role = session.linkPermission === 'edit' ? 'editor' : 'viewer';
    if (msg.userId && msg.userId === session.ownerUserId) role = 'owner';
  }

  const player = {
    playerId: client.playerId, userId: msg.userId || null,
    userName: msg.userName || `Designer ${client.playerId}`, role,
    color: pickColor(session), avatarUrl: msg.avatarUrl || '',
    position: { x: 0, y: 0, z: 0 }, rotation: { x: 0, y: 0, z: 0 },
    viewMode: '3d', isMobile: !!msg.isMobile, ws,
    pendingJoinedBroadcast: true
  };

  session.players.set(client.playerId, player);
  client.sessionId = session.id;

  // Restore ownerId when the session owner reconnects after a zombie-kick ownership transfer
  if (role === 'owner') session.ownerId = client.playerId;

  log('Session', `Player ${client.playerId} joined ${session.id} as ${role} (total: ${session.players.size}) — PlayerJoined deferred until first Move`);

  sendSessionStateTo(session, ws, role);
  scheduleDeferredJoinFallback(session, player);
}

function hasHumanPlayers(session) {
  for (const [, p] of session.players) {
    const c = clients.get(p.ws);
    if (c && !c.isBot) return true;
  }
  return false;
}

function kickSessionBots(session) {
  // Stop the bot manager for this session
  stopBotManager(session.inviteCode);

  const botsToKick = [];
  for (const [, p] of session.players) {
    const c = clients.get(p.ws);
    if (c && c.isBot) botsToKick.push(p.ws);
  }
  for (const botWs of botsToKick) {
    botWs.close();
  }
  if (botsToKick.length > 0)
    log('Bots', `Kicked ${botsToKick.length} bots from session ${session.id} (no human players left)`);
}

function leaveSession(ws, client) {
  if (!client.sessionId) return;
  const session = findSessionById(client.sessionId);
  client.sessionId = null;
  if (!session) return;

  // Release any selection held by the leaving player (clients clear highlight)
  const releasedTarget = releasePlayerSelection(session, client.playerId);
  if (releasedTarget) {
    broadcastSelection(session, ws, client.playerId, '');
    log('Select', `auto-release "${releasedTarget.slice(-6)}" (p${client.playerId} leaving)`);
  }
  session.lastActivityByPlayer?.delete(client.playerId);

  const leavingPlayer = session.players.get(client.playerId);
  if (leavingPlayer?.pendingJoinedTimer) {
    clearTimeout(leavingPlayer.pendingJoinedTimer);
    leavingPlayer.pendingJoinedTimer = null;
  }

  session.players.delete(client.playerId);

  // If the last human left, kick all bots AND fully tear down the session.
  // Bots' subsequent ws-close → leaveSession() will be a no-op (session already gone).
  if (!client.isBot && !hasHumanPlayers(session)) {
    const botCount = session.players.size;
    kickSessionBots(session);
    sessions.delete(session.inviteCode);
    if (session.projectId) projectIndex.delete(session.projectId);
    log('Session', `${session.id} torn down (last human left, kicked ${botCount} bot(s), cache cleared)`);
    return;
  }

  if (client.playerId === session.ownerId) {
    if (session.players.size === 0) {
      // No players left → delete session
      sessions.delete(session.inviteCode);
      if (session.projectId) projectIndex.delete(session.projectId);
      log('Session', `${session.id} closed (owner left, no players remaining)`);
    } else {
      // Transfer ownership to next player
      const nextPlayer = session.players.values().next().value;
      session.ownerId = nextPlayer.playerId;
      nextPlayer.role = 'owner';

      // Notify all remaining players
      broadcastToSession(session, null, {
        type: 'OwnerChanged',
        newOwnerId: nextPlayer.playerId,
        reason: 'owner_left'
      });

      log('Session', `${session.id} ownership transferred to player ${nextPlayer.playerId} (total: ${session.players.size})`);
    }
  } else {
    broadcastToSession(session, ws, { type: 'PlayerLeft', playerId: client.playerId });
    log('Session', `Player ${client.playerId} left ${session.id} (total: ${session.players.size})`);
  }

  // Clean up empty legacy session
  if (session.inviteCode === LEGACY_INVITE && session.players.size === 0) {
    sessions.delete(LEGACY_INVITE);
    if (session.projectId) projectIndex.delete(session.projectId);
    log('Legacy', 'Session removed (empty)');
  }
}

// --- Relay handlers ---

function handleMove(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;

  const player = session.players.get(client.playerId);
  if (player) {
    player.position = msg.position || player.position;
    player.rotation = msg.rotation || player.rotation;
    if (msg.viewMode) player.viewMode = msg.viewMode;
  }

  // Defer PlayerJoined broadcast until the new player reports a real (non-zero) position.
  // Avoids spawning the avatar at (0,0,0) and snapping/lerping later.
  if (player && player.pendingJoinedBroadcast && isNonZeroPosition(msg.position)) {
    flushDeferredJoin(session, player, 'first real position received');
  }

  // Don't relay PlayerMoved while the player is still pending — others don't know about them yet.
  if (player && player.pendingJoinedBroadcast) return;

  broadcastToSession(session, ws, {
    type: 'PlayerMoved', playerId: client.playerId,
    position: msg.position, rotation: msg.rotation,
    viewMode: msg.viewMode || '3d'
  });
}

function isNonZeroPosition(pos) {
  if (!pos) return false;
  return Math.abs(pos.x) > 0.001 || Math.abs(pos.y) > 0.001 || Math.abs(pos.z) > 0.001;
}

const DEFERRED_JOIN_TIMEOUT_MS = 10_000;

function flushDeferredJoin(session, player, reason) {
  if (!player.pendingJoinedBroadcast) return;
  player.pendingJoinedBroadcast = false;
  if (player.pendingJoinedTimer) {
    clearTimeout(player.pendingJoinedTimer);
    player.pendingJoinedTimer = null;
  }
  broadcastToSession(session, player.ws, {
    type: 'PlayerJoined', playerId: player.playerId,
    userId: player.userId, userName: player.userName, role: player.role,
    color: player.color, avatarUrl: player.avatarUrl || '',
    position: player.position, rotation: player.rotation,
    viewMode: player.viewMode || '3d',
    isMobile: !!player.isMobile
  });
  log('Session', `Player ${player.playerId} PlayerJoined broadcast (${reason})`);
}

function scheduleDeferredJoinFallback(session, player) {
  if (player.pendingJoinedTimer) clearTimeout(player.pendingJoinedTimer);
  player.pendingJoinedTimer = setTimeout(() => {
    player.pendingJoinedTimer = null;
    if (!session.players.has(player.playerId)) return;
    if (!player.pendingJoinedBroadcast) return;
    flushDeferredJoin(session, player, `fallback ${DEFERRED_JOIN_TIMEOUT_MS}ms timeout`);
  }, DEFERRED_JOIN_TIMEOUT_MS);
}

function handleDomainTransform(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} domain_transform (role: ${role})`);
    return;
  }

  const ts = Date.now();
  const entity = getEntityState(session, msg.id);

  let wins = 0;
  if (lwwMerge(entity, 'position', msg.position, ts))       wins |= PROP_POSITION;
  if (lwwMerge(entity, 'rotation', msg.rotation, ts))       wins |= PROP_ROTATION;
  if (lwwMerge(entity, 'scale', msg.scale, ts))             wins |= PROP_SCALE;
  if (lwwMerge(entity, 'planeOffset', msg.planeOffset, ts)) wins |= PROP_PLANE_OFFSET;

  const idTail = msg.id?.slice(-6) || '?';
  const c = msg.committed ? 'C' : 'D';
  log('⬇ DT', `p${client.playerId} ${idTail} ${c} pos=${fmtPos(msg.position)} wins=${fmtChanged(wins)}`);

  if (wins === 0 && !msg.committed) {
    log('⬇ DT', `p${client.playerId} ${idTail} SKIP (no wins)`);
    return;
  }

  if (msg.committed) session.sequenceNumber++;

  const merged = {
    type: 'DomainTransform',
    id: msg.id,
    position: entity.position?.v || msg.position,
    rotation: entity.rotation?.v || msg.rotation,
    scale: entity.scale?.v || msg.scale,
    planeOffset: entity.planeOffset?.v ?? msg.planeOffset,
    committed: msg.committed
  };
  const n = broadcastToSession(session, ws, merged);

  log('⬆ DT', `p${client.playerId} ${idTail} ${c} → ${n} peers pos=${fmtPos(merged.position)}`);
}

// --- Domain lifecycle (add + remove) ---
// Универсальная схема для Wall/Furniture/Opening/Room. Поле op различает операции;
// для op=add тело объекта приходит в xml (результат Serialize()), сервер ретранслирует
// как есть. Никаких полей вроде furnitureId/wallId/roomId на wire-уровне нет —
// только uniqueId + (для add) kind/parentId/xml/variationPath.

function handleDomainLifecycle(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} domain_lifecycle op=${msg.op} (role: ${role})`);
    return;
  }
  session.sequenceNumber++;

  const idTail = (msg.uniqueId || '').slice(-6);

  if (msg.op === 'remove') {
    // Clean up LWW entity state (применяется и к мебели, и к пропертям).
    if (session.entityState && msg.uniqueId) {
      session.entityState.delete(msg.uniqueId);
      session.entityState.delete(`var:${msg.uniqueId}`);
      const prefix = `dom:${msg.uniqueId}:`;
      for (const key of Array.from(session.entityState.keys())) {
        if (key.startsWith(prefix)) session.entityState.delete(key);
      }
    }

    ensureSelectionMaps(session);
    if (session.selections.has(msg.uniqueId)) {
      const holders = session.selections.get(msg.uniqueId);
      session.selections.delete(msg.uniqueId);
      for (const holderId of holders) {
        if (session.playerSelections.get(holderId) === msg.uniqueId)
          session.playerSelections.delete(holderId);
        broadcastSelection(session, null, holderId, '');
      }
      log('Select', `auto-release "${(msg.uniqueId || '').slice(-6)}" (entity removed, ${holders.length} holder(s))`);
    }

    const n = broadcastToSession(session, ws, {
      type: 'DomainLifecycle', playerId: client.playerId,
      op: 'remove', uniqueId: msg.uniqueId
    });

    log('Domain', `remove "${idTail}" by p${client.playerId} → ${n} peers (seq: ${session.sequenceNumber})`);
    return;
  }

  // op === 'add' (default)
  const n = broadcastToSession(session, ws, {
    type: 'DomainLifecycle', playerId: client.playerId,
    op: 'add', uniqueId: msg.uniqueId, parentId: msg.parentId,
    kind: msg.kind, xml: msg.xml,
    variationPath: msg.variationPath || ''
  });

  log('Domain', `add ${msg.kind} "${idTail}" by p${client.playerId} → ${n} peers (seq: ${session.sequenceNumber})`);
}

function handleDomainChange(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} domain_change (role: ${role})`);
    return;
  }

  if (!msg.targets || !msg.targets.length) return;

  const ts = Date.now();
  const winningTargets = [];

  for (const target of msg.targets) {
    if (!target.changes || !target.changes.length) continue;

    const winningChanges = [];
    for (const change of target.changes) {
      const entityId = `dom:${target.targetId}:${change.name}`;
      const entity = getEntityState(session, entityId);
      if (lwwMerge(entity, 'value', change.value, ts)) {
        winningChanges.push(change);
      }
    }

    if (winningChanges.length > 0) {
      winningTargets.push({
        targetId: target.targetId,
        targetDebug: target.targetDebug || '',
        changes: winningChanges
      });
    }
  }

  if (winningTargets.length === 0) {
    log('Domain', `all changes rejected (stale) by player=${client.playerId}`);
    return;
  }

  session.sequenceNumber++;

  const n = broadcastToSession(session, ws, {
    type: 'DomainChange', playerId: client.playerId,
    targets: winningTargets
  });

  const summary = winningTargets.map(t => {
    const names = t.changes.map(c => c.name).join(',');
    const label = t.targetDebug || t.targetId;
    return `${label}[${names}]`;
  }).join(', ');
  log('Domain', `change batch: ${winningTargets.length} targets by player=${client.playerId} → ${n} peers (${summary})`);
}

// --- Domain selection (multi-player) ---

function ensureSelectionMaps(session) {
  if (!session.selections) session.selections = new Map();
  if (!session.playerSelections) session.playerSelections = new Map();
  if (!session.lastActivityByPlayer) session.lastActivityByPlayer = new Map();
}

function touchPlayerActivity(session, playerId) {
  ensureSelectionMaps(session);
  session.lastActivityByPlayer.set(playerId, Date.now());
}

const LOCK_TTL_MS = 60_000;
const LOCK_SWEEP_INTERVAL_MS = 15_000;

setInterval(() => {
  const now = Date.now();
  for (const [, session] of sessions) {
    if (!session.selections || session.selections.size === 0) continue;
    if (!session.lastActivityByPlayer) continue;

    const stale = [];
    for (const [targetId, playerIds] of session.selections) {
      for (const playerId of playerIds) {
        const last = session.lastActivityByPlayer.get(playerId) || 0;
        if (now - last > LOCK_TTL_MS) stale.push({ targetId, playerId, idle: now - last });
      }
    }
    for (const entry of stale) {
      removeSelectionEntry(session, entry.targetId, entry.playerId);
      if (session.playerSelections.get(entry.playerId) === entry.targetId)
        session.playerSelections.delete(entry.playerId);
      broadcastSelection(session, null, entry.playerId, '');
      log('Select', `auto-release "${entry.targetId.slice(-6)}" by p${entry.playerId} (idle ${entry.idle}ms)`);
    }
  }
}, LOCK_SWEEP_INTERVAL_MS);

function removeSelectionEntry(session, targetId, playerId) {
  const arr = session.selections.get(targetId);
  if (!arr) return false;
  const idx = arr.indexOf(playerId);
  if (idx < 0) return false;
  arr.splice(idx, 1);
  if (arr.length === 0) session.selections.delete(targetId);
  return true;
}

function releasePlayerSelection(session, playerId) {
  ensureSelectionMaps(session);
  const prev = session.playerSelections.get(playerId);
  if (!prev) return null;
  session.playerSelections.delete(playerId);
  removeSelectionEntry(session, prev, playerId);
  return prev;
}

function broadcastSelection(session, excludeWs, playerId, targetId) {
  return broadcastToSession(session, excludeWs, {
    type: 'DomainSelection',
    playerId,
    targetId: targetId || ''
  });
}

const BOT_RELEASE_MIN_MS = 4000;
const BOT_RELEASE_MAX_MS = 8000;
const BOT_RECLAIM_MIN_MS = 1000;
const BOT_RECLAIM_MAX_MS = 3000;

function getBotMirrors(session, targetId) {
  const result = [];
  for (const [pid, p] of session.players) {
    const c = clients.get(p.ws);
    if (c && c.isBot) result.push(pid);
  }
  result.sort((a, b) => a - b);
  if (!targetId || result.length === 0) return result;
  const count = 1 + Math.floor(Math.random() * result.length);
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result.slice(0, count);
}

function clearBotReleaseTimer(session, playerId) {
  if (!session.botReleaseTimers) return;
  const t = session.botReleaseTimers.get(playerId);
  if (t) { clearTimeout(t); session.botReleaseTimers.delete(playerId); }
}

function scheduleBotRelease(session, playerId, targetId) {
  if (!session.botReleaseTimers) session.botReleaseTimers = new Map();
  clearBotReleaseTimer(session, playerId);
  const delay = BOT_RELEASE_MIN_MS + Math.random() * (BOT_RELEASE_MAX_MS - BOT_RELEASE_MIN_MS);
  const timer = setTimeout(() => {
    session.botReleaseTimers?.delete(playerId);
    if (session.playerSelections.get(playerId) !== targetId) return;
    setBotSelection(session, playerId, '');
    log('Select', `bot p${playerId} auto-released "${targetId.slice(-6)}" after ${(delay / 1000).toFixed(1)}s`);
    scheduleBotReclaim(session, playerId);
  }, delay);
  session.botReleaseTimers.set(playerId, timer);
}

function findActiveUserTarget(session) {
  for (const [pid, p] of session.players) {
    const c = clients.get(p.ws);
    if (c && !c.isBot) {
      const t = session.playerSelections.get(pid);
      if (t) return t;
    }
  }
  return null;
}

function scheduleBotReclaim(session, playerId) {
  const userTarget = findActiveUserTarget(session);
  if (!userTarget) return;

  if (!session.botReleaseTimers) session.botReleaseTimers = new Map();
  clearBotReleaseTimer(session, playerId);
  const delay = BOT_RECLAIM_MIN_MS + Math.random() * (BOT_RECLAIM_MAX_MS - BOT_RECLAIM_MIN_MS);
  const timer = setTimeout(() => {
    session.botReleaseTimers?.delete(playerId);
    const stillActive = findActiveUserTarget(session);
    if (!stillActive) return;
    if (session.playerSelections.has(playerId)) return;
    setBotSelection(session, playerId, stillActive);
    log('Select', `bot p${playerId} re-claimed "${stillActive.slice(-6)}" after ${(delay / 1000).toFixed(1)}s`);
  }, delay);
  session.botReleaseTimers.set(playerId, timer);
}

function setBotSelection(session, playerId, targetId) {
  const prev = session.playerSelections.get(playerId);
  if ((prev || '') === (targetId || '')) return;

  if (prev) {
    removeSelectionEntry(session, prev, playerId);
    session.playerSelections.delete(playerId);
    broadcastToSession(session, null, { type: 'DomainSelection', playerId, targetId: '' });
  }

  if (targetId) {
    let arr = session.selections.get(targetId);
    if (!arr) { arr = []; session.selections.set(targetId, arr); }
    arr.push(playerId);
    session.playerSelections.set(playerId, targetId);
    broadcastToSession(session, null, { type: 'DomainSelection', playerId, targetId });
    scheduleBotRelease(session, playerId, targetId);
  } else {
    clearBotReleaseTimer(session, playerId);
  }
}

function getEntityWorldPos(session, targetId) {
  const entity = session.entityState?.get(targetId);
  if (entity?.position?.v) return { x: entity.position.v.x, z: entity.position.v.z };
  return null;
}

function findBotSlot(inviteCode, playerId) {
  const manager = sessionBotManagers.get(inviteCode);
  if (!manager) return null;
  return manager.slots.find(s => s.playerId === playerId) || null;
}

function mirrorSelectionToBots(session, sourceClient, targetId) {
  if (!sourceClient) { log('Mirror', 'skip: no sourceClient'); return; }
  if (sourceClient.isBot) { log('Mirror', `skip: source p${sourceClient.playerId} is bot`); return; }

  const ids = getBotMirrors(session, targetId);
  for (const pid of ids) {
    if (!targetId) {
      const slot = findBotSlot(session.inviteCode, pid);
      if (slot) slot.walkTarget = null;
      setBotSelection(session, pid, '');
      continue;
    }

    const playerViewMode = session.players.get(pid)?.viewMode;
    if (playerViewMode === '3d') {
      const targetPos = getEntityWorldPos(session, targetId);
      const slot = findBotSlot(session.inviteCode, pid);
      if (targetPos && slot) {
        slot.walkTarget = { x: targetPos.x, z: targetPos.z, onArrived: () => setBotSelection(session, pid, targetId) };
        log('Mirror', `3D bot p${pid} walking to "${targetId.slice(-6)}" at (${targetPos.x.toFixed(2)},${targetPos.z.toFixed(2)})`);
      } else {
        setBotSelection(session, pid, targetId);
      }
    } else {
      setBotSelection(session, pid, targetId);
    }
  }

  let total = 0, bots = 0, mirrorMarked = 0;
  for (const [, p] of session.players) {
    total++;
    const c = clients.get(p.ws);
    if (c && c.isBot) { bots++; if (c.isMirror) mirrorMarked++; }
  }
  log('Mirror', `target=${(targetId || '∅').slice(-6)} src=p${sourceClient.playerId} players=${total} bots=${bots} mirrorBots=${mirrorMarked} mirrored=[${ids.join(',')}]`);
}

function handleDomainSelection(ws, client, msg) {
  const session = getSession(ws, client, true);
  if (!session) return;
  const role = session.players.get(client.playerId)?.role;
  if (!canEdit(role)) {
    log('Denied', `player=${client.playerId} domain_selection (role: ${role})`);
    return;
  }
  ensureSelectionMaps(session);

  const targetId = (msg.targetId || '').trim();
  const idTail = targetId ? targetId.slice(-6) : '∅';

  if (!targetId) {
    const released = releasePlayerSelection(session, client.playerId);
    if (released) {
      const n = broadcastSelection(session, ws, client.playerId, '');
      log('Select', `release "${released.slice(-6)}" by p${client.playerId} → ${n} peers`);
    }
    mirrorSelectionToBots(session, client, '');
    return;
  }

  if (session.playerSelections.get(client.playerId) === targetId) return;

  const released = releasePlayerSelection(session, client.playerId);
  if (released)
    broadcastSelection(session, ws, client.playerId, '');

  let arr = session.selections.get(targetId);
  if (!arr) {
    arr = [];
    session.selections.set(targetId, arr);
  }
  arr.push(client.playerId);
  session.playerSelections.set(client.playerId, targetId);

  const n = broadcastSelection(session, ws, client.playerId, targetId);
  log('Select', `claim "${idTail}" by p${client.playerId} (slot ${arr.length}/${arr.length}) → ${n} peers`);

  mirrorSelectionToBots(session, client, targetId);
}

function handleUpdateState(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;
  const player = session.players.get(client.playerId);
  if (!player || !canEdit(player.role)) {
    log('Denied', `player=${client.playerId} update_state (role=${player?.role || 'none'})`);
    return;
  }
  const xmlLen = (msg.projectXml || '').length;
  session.projectXml = msg.projectXml || session.projectXml;
  projectsModule.onXmlUpdated(session.projectId, session.projectXml);
  log('Session', `${session.id} state updated by player=${client.playerId} role=${player.role} (xml: ${xmlLen} chars, seq: ${session.sequenceNumber})`);
  broadcastStateChecksum(session); // DEBUG_STATE_VERIFIER
}

// region: DEBUG_STATE_VERIFIER
function broadcastStateChecksum(session) {
  const xml = session.projectXml || '';
  const hash = crypto.createHash('md5').update(xml).digest('hex');
  const payload = { type: 'StateChecksum', hash, length: xml.length, seq: session.sequenceNumber };
  const n = broadcastToSession(session, null, payload);
  log('Verify', `checksum broadcast hash=${hash.slice(0, 8)} len=${xml.length} seq=${session.sequenceNumber} → ${n} clients`);
}

function handleRequestFullState(ws, client, msg) {
  const session = getSession(ws, client, false);
  if (!session) return;
  const xml = session.projectXml || '';
  send(ws, { type: 'FullStateResponse', projectXml: xml });
  log('Verify', `full state sent to player=${client.playerId} (${xml.length} chars)`);
}
// endregion: DEBUG_STATE_VERIFIER

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
    ws.send(JSON.stringify({ type: 'Chunk', messageId, index: i, total, data: chunk }));
  }
  log('Chunk', `Sent ${total} chunks (${json.length} chars) id=${messageId}`);
}

// --- Connection ---

wss.on('connection', (ws) => {
  const playerId = nextPlayerId++;
  clients.set(ws, { playerId, sessionId: null, ws });
  wsLastSeen.set(ws, Date.now());
  log('Connect', `Player ${playerId} connected (total: ${clients.size})`);

  ws.on('message', (raw) => {
    wsLastSeen.set(ws, Date.now());
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
    if (msg.type === 'Chunk') {
      const fullJson = handleChunk(ws, msg);
      if (!fullJson) return; // waiting for more chunks
      try { msg = JSON.parse(fullJson); } catch (e) {
        log('Error', `Player ${clients.get(ws)?.playerId} reassembled chunk is invalid JSON`);
        return;
      }
      if (!msg.type) return;
    }

    const client = clients.get(ws);

    if (msg.type !== 'Move' && msg.type !== 'Ping') {
      const idTail = (msg.id || msg.targetId || '').slice(-6);
      const extra = idTail ? ` id=..${idTail}` : '';
      log('⬇ IN', `p${client.playerId} ${msg.type}${extra}${msg.committed ? ' COMMIT' : ''}`);
    }

    if (client?.sessionId) {
      const session = findSessionById(client.sessionId);
      if (session) touchPlayerActivity(session, client.playerId);
    }

    switch (msg.type) {
      case 'CreateSession': handleCreateSession(ws, client, msg).catch(e => log('Session', `ERROR handleCreateSession: ${e.message}`)); break;
      case 'JoinSession':   handleJoinSession(ws, client, msg).catch(e => log('Session', `ERROR handleJoinSession: ${e.message}`)); break;
      case 'LeaveSession':  leaveSession(ws, client); break;
      case 'Move':           handleMove(ws, client, msg); break;
      case 'DomainTransform': handleDomainTransform(ws, client, msg); break;
      case 'DomainLifecycle': handleDomainLifecycle(ws, client, msg); break;
      case 'DomainChange':  handleDomainChange(ws, client, msg); break;
      case 'DomainSelection': handleDomainSelection(ws, client, msg); break;
      case 'UpdateState':   handleUpdateState(ws, client, msg); break;
      case 'RequestFullState': handleRequestFullState(ws, client, msg); break; // DEBUG_STATE_VERIFIER
      case 'Ping': send(ws, { type: 'Pong' }); break;
      default:
        log('Warn', `Player ${client.playerId} sent unknown type: "${msg.type}"`);
        break;
    }
  });

  ws.on('close', (code, reason) => {
    const client = clients.get(ws);
    const reasonStr = reason ? reason.toString() : 'no reason';
    if (client?.sessionId) leaveSession(ws, client);
    clients.delete(ws);
    wsLastSeen.delete(ws);
    cleanupChunkBuffers(ws);
    log('Disconnect', `Player ${client?.playerId} disconnected (code=${code || 'none'}, reason="${reasonStr}", total: ${clients.size})`);
  });

  ws.on('error', (err) => {
    const client = clients.get(ws);
    log('Error', `Player ${client?.playerId} websocket error: ${err.message}`);
  });
});

// --- Inline bots ---

const BOT_NAMES = ['Leonid', 'Danila', 'Oksana'];
const BOT_AVATARS = [
  'https://i.pravatar.cc/150?img=1',
  'https://i.pravatar.cc/150?img=3',
  'https://i.pravatar.cc/150?img=5',
  'https://i.pravatar.cc/150?img=7',
  'https://i.pravatar.cc/150?img=10',
  'https://i.pravatar.cc/150?img=12',
  'https://i.pravatar.cc/150?img=14',
  'https://i.pravatar.cc/150?img=16',
  'https://i.pravatar.cc/150?img=20',
  'https://i.pravatar.cc/150?img=22',
  'https://i.pravatar.cc/150?img=25',
  'https://i.pravatar.cc/150?img=27',
  'https://i.pravatar.cc/150?img=30',
  'https://i.pravatar.cc/150?img=32',
  'https://i.pravatar.cc/150?img=35',
  'https://i.pravatar.cc/150?img=38',
  'https://i.pravatar.cc/150?img=40',
  'https://i.pravatar.cc/150?img=44',
  'https://i.pravatar.cc/150?img=47',
  'https://i.pravatar.cc/150?img=50',
];
function shuffledBotAvatars() {
  const arr = [...BOT_AVATARS];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}
const BOT_MOVE_INTERVAL = 100;           // ms between position updates (matches client SendRate)
const BOT_WALK_SPEED = 0.1;             // ~1 unit/sec at 100ms interval (matches WASD speed)
const BOT_DIR_CHANGE = 0.015;           // halved — ticks are 2x faster now
const BOT_PAUSE_CHANCE = 0.005;         // halved
const BOT_PAUSE_TICKS = 30;             // doubled — same real-time duration
const BOT_LOOK_SPEED = 2;              // halved — same degrees/sec
const BOT_ROOM_CHANGE = 0.0025;        // halved
const BOT_WALL_MARGIN = 0.4;
const BOT_VIEW_SWITCH_CHANCE = 0.005;  // chance per tick to toggle 2D/3D (~once per 20s)
const BOT_ARRIVAL_DIST = 0.5;          // 3D-bot walk-to-target: arrival threshold (units)
const BOT_TAP_MIN_MS = 1500;            // mobile-bot tap interval lower bound (2D only)
const BOT_TAP_MAX_MS = 4000;            // mobile-bot tap interval upper bound (2D only)

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

/** Filter rooms to indoor only.
 *  1. Find the outdoor room = the one containing the most other room centers
 *  2. Keep only rooms whose center is inside the outdoor (excluding outdoor itself)
 *  This excludes both the outdoor and any stray polygons outside the building. */
function filterIndoorRooms(rooms) {
  if (rooms.length <= 1) return rooms;
  const centers = rooms.map(r => polyCenter(r));

  // Find outdoor: room containing the most other room centers
  let outdoorIdx = 0, maxContained = -1;
  for (let i = 0; i < rooms.length; i++) {
    let count = 0;
    for (let j = 0; j < rooms.length; j++) {
      if (j !== i && ptInPoly(centers[j].x, centers[j].z, rooms[i])) count++;
    }
    if (count > maxContained) { maxContained = count; outdoorIdx = i; }
  }

  // Keep rooms inside outdoor, excluding outdoor itself
  const indoor = rooms.filter((r, i) => {
    if (i === outdoorIdx) return false;
    return ptInPoly(centers[i].x, centers[i].z, rooms[outdoorIdx]);
  });

  const oc = centers[outdoorIdx];
  log('Bots', `filterIndoorRooms: outdoor=[${outdoorIdx}] center=(${oc.x.toFixed(1)},${oc.z.toFixed(1)}) contains ${maxContained} rooms, kept ${indoor.length}/${rooms.length}`);
  return indoor.length > 0 ? indoor : rooms;
}

// Per-session bot manager: randomly connects/disconnects bots to simulate real players
let BOT_MIN_ONLINE_SEC = 15;   // min time bot stays connected
let BOT_MAX_ONLINE_SEC = 60;   // max time bot stays connected
let BOT_MIN_OFFLINE_SEC = 5;   // min time before bot reconnects
let BOT_MAX_OFFLINE_SEC = 30;  // max time before bot reconnects
const sessionBotManagers = new Map(); // inviteCode -> BotManager

function randBetween(min, max) { return min + Math.random() * (max - min); }

function spawnSessionBots(inviteCode, projectXml) {
  if (BOT_COUNT <= 0) return;
  if (sessionBotManagers.has(inviteCode)) return; // already managing

  const allRooms = parseRoomsFromXml(projectXml || '');
  log('Bots', `ALL ${allRooms.length} rooms:`);
  for (let i = 0; i < allRooms.length; i++) { const c = polyCenter(allRooms[i]); log('Bots', `  [${i}] center=(${c.x.toFixed(1)}, ${c.z.toFixed(1)}), area=${polyArea(allRooms[i]).toFixed(0)}`); }
  const rooms = filterIndoorRooms(allRooms);
  log('Bots', `${rooms.length} indoor rooms after filter, managing ${BOT_COUNT} bot slots`);
  for (const r of rooms) { const c = polyCenter(r); log('Bots', `  Indoor: center=(${c.x.toFixed(1)}, ${c.z.toFixed(1)}), area=${polyArea(r).toFixed(0)}`); }

  const manager = { slots: [], managerTimer: null, stopped: false };
  sessionBotManagers.set(inviteCode, manager);

  const avatarPool = shuffledBotAvatars();
  for (let i = 0; i < BOT_COUNT; i++) {
    const slot = { index: i, botWs: null, moveTimer: null, reconnectTimer: null, avatarUrl: avatarPool[i % avatarPool.length], walkTarget: null, playerId: null };
    manager.slots.push(slot);
    // Stagger initial connections
    const initialDelay = i * 2000 + Math.random() * 3000;
    slot.reconnectTimer = setTimeout(() => connectBot(slot, inviteCode, rooms), initialDelay);
  }

  // Periodic check: stop manager if session gone or no humans
  manager.managerTimer = setInterval(() => {
    const s = sessions.get(inviteCode);
    if (!s || !hasHumanPlayers(s)) {
      stopBotManager(inviteCode);
    }
  }, 5000);
}

function connectBot(slot, inviteCode, rooms) {
  if (slot.botWs) return; // already connected

  const session = sessions.get(inviteCode);
  if (!session || !hasHumanPlayers(session)) return;

  const name = BOT_NAMES[slot.index % BOT_NAMES.length];
  const botWs = new WebSocket(`ws://localhost:${PORT}`);
  slot.botWs = botWs;

  // Rooms already filtered to indoor in spawnSessionBots
  let currentRoom = rooms.length > 0 ? rooms[slot.index % rooms.length] : null;
  const spawn = currentRoom ? randInPoly(currentRoom) : { x: 0, z: 0 };
  let x = spawn.x, y = 0, z = spawn.z;
  let dirX = (Math.random() - 0.5) * 2, dirZ = (Math.random() - 0.5) * 2;
  let len = Math.sqrt(dirX * dirX + dirZ * dirZ) || 1; dirX /= len; dirZ /= len;
  let rotY = Math.random() * 360;
  let paused = false, pauseTicks = 0, lookDir = 1;
  const isMobileBot = BOT_MOBILE_MODE === 'all' || (BOT_MOBILE_MODE === 'random' && Math.random() < 0.5);
  let viewMode = BOT_VIEW_MODE === 'random' ? (slot.index % 2 === 0 ? '2d' : '3d') : BOT_VIEW_MODE;

  let tapX = x, tapZ = z;
  let nextTapAt = 0;

  botWs.on('open', () => {
    botWs.send(JSON.stringify({
      type: 'JoinSession', inviteCode, userName: name,
      avatarUrl: slot.avatarUrl || '',
      isMobile: isMobileBot, isBot: true
    }));
  });

  const chunkBuf = new Map(); // messageId -> { chunks[], total }
  botWs.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    // Reassemble chunked messages
    if (msg.type === 'Chunk') {
      let entry = chunkBuf.get(msg.messageId);
      if (!entry) { entry = { chunks: [], total: msg.total }; chunkBuf.set(msg.messageId, entry); }
      entry.chunks[msg.index] = msg.data;
      const received = entry.chunks.filter(c => c !== undefined).length;
      if (received < entry.total) return;
      chunkBuf.delete(msg.messageId);
      const full = entry.chunks.join('');
      try { msg = JSON.parse(full); } catch { return; }
    }

    if (msg.type === 'SessionState') {
      slot.playerId = msg.playerId;
      const freshAllRooms = parseRoomsFromXml(msg.projectXml || '');
      const freshRooms = filterIndoorRooms(freshAllRooms);
      if (freshRooms.length > 0) {
        rooms = freshRooms;
        currentRoom = rooms[slot.index % rooms.length];
        const respawn = randInPoly(currentRoom);
        x = respawn.x; z = respawn.z;
      }
      log('Bots', `${name} connected at (${x.toFixed(2)}, ${y.toFixed(2)}, ${z.toFixed(2)})`);

      slot.moveTimer = setInterval(() => {
        if (botWs.readyState !== WebSocket.OPEN) { clearInterval(slot.moveTimer); return; }

        if (viewMode === '3d' && slot.walkTarget) {
          const dx = slot.walkTarget.x - x;
          const dz = slot.walkTarget.z - z;
          const dist = Math.sqrt(dx * dx + dz * dz);
          if (dist >= BOT_ARRIVAL_DIST) {
            x += (dx / dist) * BOT_WALK_SPEED;
            z += (dz / dist) * BOT_WALK_SPEED;
            rotY = Math.atan2(dx, dz) * 180 / Math.PI;
          } else {
            const cb = slot.walkTarget.onArrived;
            slot.walkTarget = null;
            if (cb) cb();
          }
          botWs.send(JSON.stringify({ type: 'Move', position: { x, y, z }, rotation: { x: 0, y: rotY, z: 0 }, viewMode }));
          return;
        }

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
          if (rooms.length > 1 && Math.random() < BOT_ROOM_CHANGE) {
            currentRoom = rooms[Math.floor(Math.random() * rooms.length)];
            const p = randInPoly(currentRoom); x = p.x; z = p.z;
          }
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
            dirX = (Math.random() - 0.5) * 2; dirZ = (Math.random() - 0.5) * 2;
            len = Math.sqrt(dirX * dirX + dirZ * dirZ) || 1; dirX /= len; dirZ /= len;
          }
          rotY = Math.atan2(dirX, dirZ) * 180 / Math.PI;
        }

        if (BOT_VIEW_MODE === 'random' && Math.random() < BOT_VIEW_SWITCH_CHANCE) {
          viewMode = viewMode === '3d' ? '2d' : '3d';
          log('Bots', `${name} switched to ${viewMode}`);
        } else if (BOT_VIEW_MODE !== 'random') {
          viewMode = BOT_VIEW_MODE;
        }

        if (viewMode === '2d' && isMobileBot) {
          const now = Date.now();
          if (now < nextTapAt) return;
          nextTapAt = now + BOT_TAP_MIN_MS + Math.random() * (BOT_TAP_MAX_MS - BOT_TAP_MIN_MS);
          if (currentRoom) {
            const tap = randInPoly(currentRoom);
            tapX = tap.x; tapZ = tap.z;
          } else {
            tapX = x; tapZ = z;
          }
        }

        const useTap = viewMode === '2d' && isMobileBot;
        const sendX = useTap ? tapX : x;
        const sendZ = useTap ? tapZ : z;
        const sendY = viewMode === '2d' ? 0 : y;
        const sendRotY = viewMode === '2d' ? 0 : rotY;
        botWs.send(JSON.stringify({ type: 'Move', position: { x: sendX, y: sendY, z: sendZ }, rotation: { x: 0, y: sendRotY, z: 0 }, viewMode }));
      }, BOT_MOVE_INTERVAL);

      if (BOT_REJOIN) {
        const onlineTime = randBetween(BOT_MIN_ONLINE_SEC, BOT_MAX_ONLINE_SEC) * 1000;
        setTimeout(() => {
          if (slot.botWs === botWs && botWs.readyState === WebSocket.OPEN) {
            log('Bots', `${name} disconnecting (was online ${(onlineTime / 1000).toFixed(0)}s)`);
            disconnectBot(slot);
            const offlineTime = randBetween(BOT_MIN_OFFLINE_SEC, BOT_MAX_OFFLINE_SEC) * 1000;
            slot.reconnectTimer = setTimeout(() => connectBot(slot, inviteCode, rooms), offlineTime);
          }
        }, onlineTime);
      }
    }
    if (msg.type === 'SessionError') log('Bots', `${name} error: ${msg.code}`);
  });

  botWs.on('close', () => {
    if (slot.moveTimer) { clearInterval(slot.moveTimer); slot.moveTimer = null; }
    if (slot.botWs === botWs) slot.botWs = null;
  });
  botWs.on('error', () => {});
}

function disconnectBot(slot) {
  if (slot.moveTimer) { clearInterval(slot.moveTimer); slot.moveTimer = null; }
  if (slot.botWs) {
    slot.botWs.close();
    slot.botWs = null;
  }
}

function stopBotManager(inviteCode) {
  const manager = sessionBotManagers.get(inviteCode);
  if (!manager) return;
  manager.stopped = true;
  if (manager.managerTimer) clearInterval(manager.managerTimer);
  for (const slot of manager.slots) {
    if (slot.reconnectTimer) clearTimeout(slot.reconnectTimer);
    disconnectBot(slot);
  }
  sessionBotManagers.delete(inviteCode);
  log('Bots', `Bot manager stopped for ${inviteCode}`);
}

// --- Start ---

server.listen(PORT, () => {
  log('Server', `Multiplayer server running on port ${PORT}`);
  log('Server', `Auto-bots: ${BOT_COUNT} per session`);
});
