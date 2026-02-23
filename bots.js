// Bot system - virtual players that walk around randomly
// If a bot doesn't move 0.5m in 2 seconds, it changes direction (hit a wall)

const BOT_CONFIG = {
  count: 3,
  speed: 1.5,           // meters per second
  updateInterval: 100,   // ms between position broadcasts
  stuckCheckInterval: 2000, // ms - check if stuck
  stuckThreshold: 0.5,  // meters - min distance in stuckCheckInterval to not be "stuck"
  spawnRadius: 0.5,     // meters - max random offset from player spawn point
};

class Bot {
  constructor(id, broadcastAll, spawnPosition) {
    this.id = id;
    this.broadcastAll = broadcastAll;

    // Spawn near the player with random offset
    const r = BOT_CONFIG.spawnRadius;
    this.position = {
      x: spawnPosition.x + randomRange(-r, r),
      y: spawnPosition.y,
      z: spawnPosition.z + randomRange(-r, r),
    };

    // Pick random direction
    this.pickNewDirection();

    // For stuck detection
    this.lastCheckPosition = { ...this.position };

    this._moveTimer = null;
    this._stuckTimer = null;
  }

  pickNewDirection() {
    const angle = Math.random() * Math.PI * 2;
    this.dirX = Math.cos(angle);
    this.dirZ = Math.sin(angle);
    this.rotation = { y: angle * (180 / Math.PI) };
  }

  start() {
    // Movement tick
    this._moveTimer = setInterval(() => this.tick(), BOT_CONFIG.updateInterval);

    // Stuck detection
    this._stuckTimer = setInterval(() => this.checkStuck(), BOT_CONFIG.stuckCheckInterval);
  }

  stop() {
    if (this._moveTimer) clearInterval(this._moveTimer);
    if (this._stuckTimer) clearInterval(this._stuckTimer);
  }

  tick() {
    const dt = BOT_CONFIG.updateInterval / 1000;

    this.position.x += this.dirX * BOT_CONFIG.speed * dt;
    this.position.z += this.dirZ * BOT_CONFIG.speed * dt;

    // Broadcast movement
    this.broadcastAll({
      type: 'player_moved',
      playerId: this.id,
      position: this.position,
      rotation: this.rotation,
    });

    // Broadcast pointer - ray from bot's position forward along movement direction
    const pointerDist = 5;
    this.broadcastAll({
      type: 'pointer',
      playerId: this.id,
      origin: { x: this.position.x, y: this.position.y + 1, z: this.position.z },
      target: {
        x: this.position.x + this.dirX * pointerDist,
        y: this.position.y + 0.5,
        z: this.position.z + this.dirZ * pointerDist,
      },
    });
  }

  checkStuck() {
    const dx = this.position.x - this.lastCheckPosition.x;
    const dz = this.position.z - this.lastCheckPosition.z;
    const dist = Math.sqrt(dx * dx + dz * dz);

    if (dist < BOT_CONFIG.stuckThreshold) {
      console.log(`[Bot ${this.id}] Stuck (moved ${dist.toFixed(2)}m), changing direction`);
      this.pickNewDirection();
    }

    this.lastCheckPosition = { ...this.position };
  }

  updateRotation() {
    const angle = Math.atan2(this.dirZ, this.dirX);
    this.rotation = { y: angle * (180 / Math.PI) };
  }

  toPlayerData() {
    return {
      id: this.id,
      position: { ...this.position },
      rotation: { ...this.rotation },
    };
  }
}

// --- Helpers ---

function randomRange(min, max) {
  return min + Math.random() * (max - min);
}

// --- Bot Manager ---

class BotManager {
  constructor() {
    this.bots = [];
    this._started = false;
  }

  /**
   * Spawn bots near a player's position
   * @param {number} startId - first bot playerId
   * @param {function} broadcastAll - function(msgObj) sends to all real players
   * @param {{x,y,z}} spawnPosition - player position to spawn near
   */
  start(startId, broadcastAll, spawnPosition) {
    if (this._started) return;
    this._started = true;

    console.log(`[Bots] Spawning ${BOT_CONFIG.count} bots near (${spawnPosition.x.toFixed(1)}, ${spawnPosition.z.toFixed(1)})`);

    for (let i = 0; i < BOT_CONFIG.count; i++) {
      const bot = new Bot(startId + i, broadcastAll, spawnPosition);
      this.bots.push(bot);
      bot.start();
    }
  }

  stop() {
    for (const bot of this.bots) {
      bot.stop();
    }
    this.bots = [];
    console.log('[Bots] All bots stopped');
  }

  /** Get bot data array for welcome message */
  getAllPlayerData() {
    return this.bots.map(b => b.toPlayerData());
  }

  /** Get bot IDs set for filtering */
  getBotIds() {
    return new Set(this.bots.map(b => b.id));
  }
}

module.exports = { BotManager, BOT_CONFIG };
