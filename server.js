const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

// HTTP server (required by Render for health checks + WebSocket upgrade)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(`Multiplayer server OK. Players online: ${players.size}`);
});

const wss = new WebSocket.Server({ server });

// playerId counter
let nextId = 1;

// connectedPlayers: Map<WebSocket, { id, position, rotation }>
const players = new Map();

wss.on('connection', (ws) => {
  const playerId = nextId++;
  players.set(ws, { id: playerId, position: { x: 0, y: 0, z: 0 }, rotation: { y: 0 }, pointer: { x: 0, y: 0, z: 0 } });

  console.log(`[+] Player ${playerId} connected (total: ${players.size})`);

  // Send the player their assigned ID + all existing players
  const existingPlayers = [];
  for (const [otherWs, data] of players) {
    if (otherWs !== ws) {
      existingPlayers.push(data);
    }
  }

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
    rotation: { y: 0 },
    pointer: { x: 0, y: 0, z: 0 }
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
      // Update stored state
      const player = players.get(ws);
      player.position = msg.position;
      player.rotation = msg.rotation;
      player.pointer = msg.pointer || { x: 0, y: 0, z: 0 };

      // Relay to all others
      broadcast(ws, {
        type: 'player_moved',
        playerId: playerId,
        position: msg.position,
        rotation: msg.rotation,
        pointer: player.pointer
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
