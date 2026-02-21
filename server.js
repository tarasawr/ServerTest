const WebSocket = require('ws');

const PORT = 3000;
const wss = new WebSocket.Server({ port: PORT });

// playerId counter
let nextId = 1;

// connectedPlayers: Map<WebSocket, { id, position, rotation }>
const players = new Map();

wss.on('connection', (ws) => {
  const playerId = nextId++;
  players.set(ws, { id: playerId, position: { x: 0, y: 0, z: 0 }, rotation: { y: 0 } });

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
      // Update stored state
      const player = players.get(ws);
      player.position = msg.position;
      player.rotation = msg.rotation;

      // Relay to all others
      broadcast(ws, {
        type: 'player_moved',
        playerId: playerId,
        position: msg.position,
        rotation: msg.rotation
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

console.log(`Multiplayer relay server running on ws://localhost:${PORT}`);
console.log('Waiting for players...');
