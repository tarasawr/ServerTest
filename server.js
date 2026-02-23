const http = require('http');
const WebSocket = require('ws');
const { BotManager } = require('./bots');

const PORT = process.env.PORT || 3000;
const BOT_ID_START = 900; // Bot IDs start at 900 to not collide with real players

// HTTP server (required by Render for health checks + WebSocket upgrade)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Multiplayer server OK. Players online: ${players.size}, bots: ${botManager.bots.length}`);
});

const wss = new WebSocket.Server({ server });

// playerId counter
let nextId = 1;

// connectedPlayers: Map<WebSocket, { id, position, rotation }>
const players = new Map();

// --- Bots ---
const botManager = new BotManager();

function broadcastToAll(obj) {
  const data = JSON.stringify(obj);
  for (const [ws] of players) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

// Bots spawn lazily on first player move (so we know a valid position)

wss.on('connection', (ws) => {
  const playerId = nextId++;
  players.set(ws, { id: playerId, position: { x: 0, y: 0, z: 0 }, rotation: { y: 0 } });

  console.log(`[+] Player ${playerId} connected (total: ${players.size})`);

  // Send the player their assigned ID + all existing players + bots
  const existingPlayers = [];
  for (const [otherWs, data] of players) {
    if (otherWs !== ws) {
      existingPlayers.push(data);
    }
  }
  // Include bots as existing players
  existingPlayers.push(...botManager.getAllPlayerData());

  send(ws, {
    type: 'welcome',
    playerId: playerId,
    players: existingPlayers
  });

  // Notify others about the new player
  broadcast(ws, {
    type: 'player_joined',
    playerId: playerId,
    position: { x: 0, y: 0, z: 0 },
    rotation: { y: 0 }
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      console.log(`[!] Bad JSON from player ${playerId}`);
      return;
    }

    if (msg.type === 'move') {
      const player = players.get(ws);
      player.position = msg.position;
      player.rotation = msg.rotation;

      // Spawn bots on first move from any player (now we know a valid position)
      if (!botManager._started) {
        botManager.start(BOT_ID_START, broadcastToAll, msg.position);
        // Notify this player about the bots
        for (const bot of botManager.bots) {
          send(ws, { type: 'player_joined', playerId: bot.id, position: bot.position, rotation: bot.rotation });
        }
      }

      // Record player path for bots to follow
      botManager.recordPosition(msg.position, msg.rotation);

      broadcast(ws, {
        type: 'player_moved',
        playerId: playerId,
        position: msg.position,
        rotation: msg.rotation
      });
    }

    if (msg.type === 'pointer') {
      broadcast(ws, {
        type: 'pointer',
        playerId: playerId,
        origin: msg.origin,
        target: msg.target
      });
    }
  });

  ws.on('close', () => {
    players.delete(ws);
    console.log(`[-] Player ${playerId} disconnected (total: ${players.size})`);

    // Notify others
    broadcast(ws, {
      type: 'player_left',
      playerId: playerId
    });
  });
});

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(sender, obj) {
  const data = JSON.stringify(obj);
  for (const [ws] of players) {
    if (ws !== sender && ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

server.listen(PORT, () => {
  console.log(`Multiplayer relay server running on port ${PORT}`);
  console.log('Waiting for players...');
});
