// Bot system - bots follow the player's path with delay
// Each bot replays the player's recorded positions with a different time offset
// Occasionally a bot "backtracks" to a random earlier point in the path

const BOT_CONFIG = {
  count: 3,
  updateInterval: 100,     // ms between position broadcasts
  pathMaxLength: 600,       // max recorded path points (~60 sec at 10hz)
  delayPerBot: 20,          // path index offset between bots (20 = 2 sec delay at 100ms)
  backtrackChance: 0.003,   // chance per tick to backtrack (~once per 30 sec)
  backtrackSteps: 30,       // how many steps back to jump (30 = 3 sec back)
  spawnRadius: 0.5,
};

class Bot {
  constructor(id, broadcastAll, spawnPosition, pathDelay) {
    this.id = id;
    this.broadcastAll = broadcastAll;
    this.pathDelay = pathDelay; // how many steps behind the player

    // Start near the player
    this.position = {
      x: spawnPosition.x + randomRange(-BOT_CONFIG.spawnRadius, BOT_CONFIG.spawnRadius),
      y: spawnPosition.y,
      z: spawnPosition.z + randomRange(-BOT_CONFIG.spawnRadius, BOT_CONFIG.spawnRadius),
    };
    this.rotation = { y: 0 };

    // Current index in the shared path
    this.pathIndex = 0;
    this.backtracking = false;
    this.backtrackTarget = 0;

    this._timer = null;
  }

  start(pathRef) {
    this.path = pathRef; // shared reference to player path array
    this._timer = setInterval(() => this.tick(), BOT_CONFIG.updateInterval);
  }

  stop() {
    if (this._timer) clearInterval(this._timer);
  }

  tick() {
    if (this.path.length === 0) return;

    // Target index: follow player path with delay
    const headIndex = this.path.length - 1;
    let targetIndex = headIndex - this.pathDelay;

    // Random backtrack
    if (!this.backtracking && Math.random() < BOT_CONFIG.backtrackChance && targetIndex > BOT_CONFIG.backtrackSteps) {
      this.backtracking = true;
      this.backtrackTarget = targetIndex - BOT_CONFIG.backtrackSteps;
      this.pathIndex = targetIndex;
    }

    if (this.backtracking) {
      // Move backwards through the path
      this.pathIndex = Math.max(this.pathIndex - 1, this.backtrackTarget);
      if (this.pathIndex <= this.backtrackTarget) {
        this.backtracking = false; // done backtracking, resume following
      }
      targetIndex = this.pathIndex;
    } else {
      targetIndex = Math.max(0, targetIndex);
      this.pathIndex = targetIndex;
    }

    const point = this.path[targetIndex];
    if (!point) return;

    this.position = { ...point.position };
    this.rotation = { ...point.rotation };

    // Broadcast movement
    this.broadcastAll({
      type: 'player_moved',
      playerId: this.id,
      position: this.position,
      rotation: this.rotation,
    });

    // Broadcast pointer - look forward along rotation
    const yRad = (this.rotation.y || 0) * Math.PI / 180;
    const pointerDist = 5;
    this.broadcastAll({
      type: 'pointer',
      playerId: this.id,
      origin: { x: this.position.x, y: this.position.y + 1, z: this.position.z },
      target: {
        x: this.position.x + Math.sin(yRad) * pointerDist,
        y: this.position.y + 0.5,
        z: this.position.z + Math.cos(yRad) * pointerDist,
      },
    });
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
    this._path = []; // shared path recorded from player
  }

  /** Record a player's position into the shared path */
  recordPosition(position, rotation) {
    this._path.push({ position: { ...position }, rotation: { ...rotation } });
    // Trim to max length
    if (this._path.length > BOT_CONFIG.pathMaxLength) {
      this._path.shift();
      // Adjust bot indices
      for (const bot of this.bots) {
        bot.pathIndex = Math.max(0, bot.pathIndex - 1);
        if (bot.backtracking) {
          bot.backtrackTarget = Math.max(0, bot.backtrackTarget - 1);
        }
      }
    }
  }

  start(startId, broadcastAll, spawnPosition) {
    if (this._started) return;
    this._started = true;

    console.log(`[Bots] Spawning ${BOT_CONFIG.count} bots following player path`);

    for (let i = 0; i < BOT_CONFIG.count; i++) {
      const delay = BOT_CONFIG.delayPerBot * (i + 1); // bot 0 = 2s delay, bot 1 = 4s, bot 2 = 6s
      const bot = new Bot(startId + i, broadcastAll, spawnPosition, delay);
      this.bots.push(bot);
      bot.start(this._path);
    }
  }

  stop() {
    for (const bot of this.bots) bot.stop();
    this.bots = [];
    this._path = [];
  }

  getAllPlayerData() {
    return this.bots.map(b => b.toPlayerData());
  }

  getBotIds() {
    return new Set(this.bots.map(b => b.id));
  }
}

module.exports = { BotManager, BOT_CONFIG };
