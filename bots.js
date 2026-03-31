// Multiplayer bots — connect as real WebSocket clients
// Usage: node bots.js <inviteCode> [botCount=3]
// Bots join an existing session, walk inside room polygons parsed from projectXml.

const WebSocket = require('ws');
const http = require('http');

const SERVER_URL = 'ws://localhost:3000';
const HTTP_URL = 'http://localhost:3000';
const BOT_NAMES = ['Luna', 'Ricardo', 'Emma', 'Mark', 'Daniel', 'Sophia', 'Alex', 'Mia', 'Leo', 'Zara'];
const MOVE_INTERVAL = 100;       // ms between position updates (matches client SendRate)
const LEAVE_CHANCE = 0.001;      // chance per tick to leave (~once per 100s)
const REJOIN_DELAY = 5000;       // ms before rejoining after leave
const WALK_SPEED = 0.025;        // units per tick (halved — ticks 2x faster)
const DIRECTION_CHANGE = 0.015;  // chance per tick to pick new direction (halved)
const PAUSE_CHANCE = 0.005;      // chance per tick to stop and look around (halved)
const PAUSE_DURATION = 30;       // ticks to pause (~3s, doubled for 100ms ticks)
const LOOK_SPEED = 2;            // degrees per tick when looking around (halved)
const ROOM_CHANGE_CHANCE = 0.0025; // chance per tick to move to another room (halved)
const WALL_MARGIN = 0.15;        // stay this far from walls
const VIEW_SWITCH_CHANCE = 0.005; // chance per tick to toggle 2D/3D (~once per 20s)

const inviteCode = process.argv[2];
const botCount = parseInt(process.argv[3]) || 3;

if (!inviteCode) {
  console.log('Usage: node bots.js <inviteCode> [botCount]');
  console.log('  Get invite code from server: curl http://localhost:3000/sessions');
  process.exit(1);
}

function log(tag, msg) {
  const ts = new Date().toISOString().slice(11, 23);
  console.log(`${ts} [${tag}] ${msg}`);
}

// --- Room geometry parsing ---

function parseRoomsFromXml(xml) {
  const rooms = [];
  // Match each Floor > Shape block
  const floorRegex = /<Floor[^>]*>[\s\S]*?<Shape>([\s\S]*?)<\/Shape>/g;
  let floorMatch;
  while ((floorMatch = floorRegex.exec(xml)) !== null) {
    const shapeXml = floorMatch[1];
    const vertices = [];
    const vecRegex = /<Vector2\s+x="([^"]+)"\s+y="([^"]+)"/g;
    let vecMatch;
    while ((vecMatch = vecRegex.exec(shapeXml)) !== null) {
      // XML y = world z (top-down plan view)
      vertices.push({ x: parseFloat(vecMatch[1]), z: parseFloat(vecMatch[2]) });
    }
    if (vertices.length >= 3) {
      rooms.push(vertices);
    }
  }
  return rooms;
}

function pointInPolygon(px, pz, polygon) {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i].x, zi = polygon[i].z;
    const xj = polygon[j].x, zj = polygon[j].z;
    if ((zi > pz) !== (zj > pz) && px < (xj - xi) * (pz - zi) / (zj - zi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

function polygonCenter(polygon) {
  let cx = 0, cz = 0;
  for (const v of polygon) { cx += v.x; cz += v.z; }
  return { x: cx / polygon.length, z: cz / polygon.length };
}

function randomPointInPolygon(polygon) {
  // Bounding box sampling
  let minX = Infinity, maxX = -Infinity, minZ = Infinity, maxZ = -Infinity;
  for (const v of polygon) {
    if (v.x < minX) minX = v.x;
    if (v.x > maxX) maxX = v.x;
    if (v.z < minZ) minZ = v.z;
    if (v.z > maxZ) maxZ = v.z;
  }
  // Shrink by margin
  minX += WALL_MARGIN; maxX -= WALL_MARGIN;
  minZ += WALL_MARGIN; maxZ -= WALL_MARGIN;

  for (let attempt = 0; attempt < 50; attempt++) {
    const px = minX + Math.random() * (maxX - minX);
    const pz = minZ + Math.random() * (maxZ - minZ);
    if (pointInPolygon(px, pz, polygon)) return { x: px, z: pz };
  }
  // Fallback to center
  return polygonCenter(polygon);
}

function clampToPolygon(px, pz, polygon) {
  if (pointInPolygon(px, pz, polygon)) return { x: px, z: pz };

  // Find closest point on polygon edges
  let bestDist = Infinity, bestX = px, bestZ = pz;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const ax = polygon[j].x, az = polygon[j].z;
    const bx = polygon[i].x, bz = polygon[i].z;
    const dx = bx - ax, dz = bz - az;
    const len2 = dx * dx + dz * dz;
    let t = len2 > 0 ? ((px - ax) * dx + (pz - az) * dz) / len2 : 0;
    t = Math.max(0, Math.min(1, t));
    const cx = ax + t * dx + WALL_MARGIN * (-(bz - az)) / Math.sqrt(len2 || 1);
    const cz = az + t * dz + WALL_MARGIN * (bx - ax) / Math.sqrt(len2 || 1);
    const dist = (px - cx) * (px - cx) + (pz - cz) * (pz - cz);
    if (dist < bestDist) { bestDist = dist; bestX = cx; bestZ = cz; }
  }
  return { x: bestX, z: bestZ };
}

// --- Fetch project XML from server ---

function fetchProjectXml(invite) {
  return new Promise((resolve, reject) => {
    http.get(`${HTTP_URL}/sessions/latest/project`, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve(json.projectXml || '');
        } catch { resolve(''); }
      });
    }).on('error', () => resolve(''));
  });
}

/** Filter rooms to indoor only: outdoor is the largest room, exclude it. */
function filterIndoorRooms(rooms) {
  if (rooms.length <= 1) return rooms;
  let maxArea = -1, maxIdx = 0;
  const area = r => { let a = 0; for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j].x + r[i].x) * (r[j].z - r[i].z); return Math.abs(a / 2); };
  for (let i = 0; i < rooms.length; i++) {
    const a = area(rooms[i]);
    if (a > maxArea) { maxArea = a; maxIdx = i; }
  }
  const indoor = rooms.filter((_, i) => i !== maxIdx);
  return indoor.length > 0 ? indoor : rooms;
}

// --- Bot ---

class Bot {
  constructor(index, rooms) {
    this.index = index;
    this.name = BOT_NAMES[index % BOT_NAMES.length];
    this.ws = null;
    this.playerId = null;
    this.alive = true;
    this.moveTimer = null;
    // Use indoor rooms only — filter out outliers far from median
    this.rooms = filterIndoorRooms(rooms);
    this.currentRoom = this.rooms.length > 0 ? this.rooms[index % this.rooms.length] : null;

    // Spawn inside room or at origin
    const spawn = this.currentRoom ? randomPointInPolygon(this.currentRoom) : { x: 0, z: 0 };
    this.x = spawn.x;
    this.z = spawn.z;
    this.y = 0;

    this.dirX = (Math.random() - 0.5) * 2;
    this.dirZ = (Math.random() - 0.5) * 2;
    this._normalizeDir();
    this.rotY = Math.random() * 360;

    // Pause/look around state
    this.paused = false;
    this.pauseTicks = 0;
    this.lookDir = 1;
    this.viewMode = '3d';
  }

  connect() {
    this.ws = new WebSocket(SERVER_URL);

    this.ws.on('open', () => {
      log(this.name, 'Connected, joining session...');
      this.ws.send(JSON.stringify({
        type: 'join_session',
        inviteCode,
        userName: this.name
      }));
    });

    this.ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      if (msg.type === 'session_state') {
        this.playerId = msg.playerId;
        log(this.name, `Joined as player ${this.playerId} (role: ${msg.role})`);
        this._startWalking();
      }

      if (msg.type === 'session_error') {
        log(this.name, `Error: ${msg.code} — ${msg.message}`);
      }
    });

    this.ws.on('close', () => {
      this._stopWalking();
      if (this.alive) {
        log(this.name, `Disconnected, rejoining in ${REJOIN_DELAY / 1000}s...`);
        setTimeout(() => { if (this.alive) this.connect(); }, REJOIN_DELAY);
      }
    });

    this.ws.on('error', () => {});
  }

  disconnect() {
    this.alive = false;
    this._stopWalking();
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: 'leave_session' }));
      this.ws.close();
    }
  }

  _startWalking() {
    this._stopWalking();
    this.moveTimer = setInterval(() => this._tick(), MOVE_INTERVAL);
  }

  _stopWalking() {
    if (this.moveTimer) {
      clearInterval(this.moveTimer);
      this.moveTimer = null;
    }
  }

  _tick() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

    // Occasionally switch rooms
    if (this.rooms.length > 1 && Math.random() < ROOM_CHANGE_CHANCE) {
      const newRoom = this.rooms[Math.floor(Math.random() * this.rooms.length)];
      if (newRoom !== this.currentRoom) {
        this.currentRoom = newRoom;
        const target = randomPointInPolygon(newRoom);
        this.x = target.x;
        this.z = target.z;
        log(this.name, 'Moved to another room');
      }
    }

    // Start pausing (look around)
    if (!this.paused && Math.random() < PAUSE_CHANCE) {
      this.paused = true;
      this.pauseTicks = PAUSE_DURATION + Math.floor(Math.random() * PAUSE_DURATION);
      this.lookDir = Math.random() < 0.5 ? 1 : -1;
    }

    if (this.paused) {
      this.rotY += LOOK_SPEED * this.lookDir;
      if (Math.random() < 0.08) this.lookDir *= -1;
      this.pauseTicks--;
      if (this.pauseTicks <= 0) this.paused = false;
    } else {
      // Random direction change
      if (Math.random() < DIRECTION_CHANGE) {
        this.dirX = (Math.random() - 0.5) * 2;
        this.dirZ = (Math.random() - 0.5) * 2;
        this._normalizeDir();
      }

      // Move
      let newX = this.x + this.dirX * WALK_SPEED;
      let newZ = this.z + this.dirZ * WALK_SPEED;

      if (this.currentRoom) {
        // Clamp to room polygon
        if (pointInPolygon(newX, newZ, this.currentRoom)) {
          this.x = newX;
          this.z = newZ;
        } else {
          // Bounce: reverse direction and pick new random direction
          this.dirX = -this.dirX + (Math.random() - 0.5) * 0.5;
          this.dirZ = -this.dirZ + (Math.random() - 0.5) * 0.5;
          this._normalizeDir();
        }
      } else {
        // No rooms — fallback to radius
        this.x = newX;
        this.z = newZ;
        const dist = Math.sqrt(this.x * this.x + this.z * this.z);
        if (dist > 8) {
          this.dirX = -this.x / dist;
          this.dirZ = -this.z / dist;
          this._normalizeDir();
        }
      }

      this.rotY = Math.atan2(this.dirX, this.dirZ) * 180 / Math.PI;
    }

    // Randomly toggle 2D/3D
    if (Math.random() < VIEW_SWITCH_CHANCE) {
      this.viewMode = this.viewMode === '3d' ? '2d' : '3d';
      log(this.name, `Switched to ${this.viewMode}`);
    }

    // Send move — in 2D mode cursor is on floor (y=0, no rotation)
    const sendY = this.viewMode === '2d' ? 0 : this.y;
    const sendRotY = this.viewMode === '2d' ? 0 : this.rotY;
    this.ws.send(JSON.stringify({
      type: 'move',
      position: { x: this.x, y: sendY, z: this.z },
      rotation: { x: 0, y: sendRotY, z: 0 },
      viewMode: this.viewMode
    }));

    // Random leave
    if (Math.random() < LEAVE_CHANCE) {
      log(this.name, 'Leaving session...');
      this.ws.send(JSON.stringify({ type: 'leave_session' }));
      this.ws.close();
    }
  }

  _normalizeDir() {
    const len = Math.sqrt(this.dirX * this.dirX + this.dirZ * this.dirZ) || 1;
    this.dirX /= len;
    this.dirZ /= len;
  }
}

// --- Start ---

async function main() {
  log('Bots', `Fetching project XML...`);
  const xml = await fetchProjectXml(inviteCode);
  const rooms = parseRoomsFromXml(xml);

  if (rooms.length > 0) {
    log('Bots', `Parsed ${rooms.length} room(s) from project XML`);
    for (let i = 0; i < rooms.length; i++) {
      const c = polygonCenter(rooms[i]);
      log('Bots', `  Room ${i}: ${rooms[i].length} vertices, center=(${c.x.toFixed(1)}, ${c.z.toFixed(1)})`);
    }
  } else {
    log('Bots', 'No rooms found in project XML — bots will walk in radius');
  }

  log('Bots', `Starting ${botCount} bots for session invite: ${inviteCode}`);

  const bots = [];
  for (let i = 0; i < botCount; i++) {
    const bot = new Bot(i, rooms);
    bots.push(bot);
    setTimeout(() => bot.connect(), i * 500);
  }

  process.on('SIGINT', () => {
    log('Bots', 'Shutting down...');
    for (const bot of bots) bot.disconnect();
    setTimeout(() => process.exit(0), 1000);
  });
}

main();
