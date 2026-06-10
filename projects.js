'use strict';

const fs = require('fs');
const path = require('path');
const db = require('./db');

// --- Projects database ---
// Persisted in Postgres (Neon). Schema in db.js:
//   projects(project_id, owner_user_id, owner_name, project_title, project_xml,
//            global_role, last_sync_date)
//   project_users(project_id, user_id, name, avatar_url, role, created_at)
//
// `role` is NULL when a user inherits the project-level global_role.
// 'owner' / 'can_edit' / 'can_view' are explicit overrides.
// 'no_access' is a soft-removed user: the row is kept (so re-join via the share
// link can be rejected) but the user is hidden from every listing endpoint.

// --- One-shot migration from legacy JSON storage ---

const LEGACY_STORAGE_FILE = path.join(__dirname, 'projects-data.json');

async function migrateFromJsonIfNeeded() {
  if (!fs.existsSync(LEGACY_STORAGE_FILE)) return;

  // Don't import if DB already has data — assume someone already migrated.
  const { rows } = await db.query('SELECT COUNT(*)::int AS n FROM projects');
  if (rows[0].n > 0) {
    log('Migration', `Skipping JSON import — DB has ${rows[0].n} project(s)`);
    return;
  }

  let arr;
  try {
    arr = JSON.parse(fs.readFileSync(LEGACY_STORAGE_FILE, 'utf8'));
  } catch (e) {
    log('Migration', `WARN: could not parse legacy JSON: ${e.message}`);
    return;
  }
  if (!Array.isArray(arr) || arr.length === 0) return;

  for (const p of arr) {
    if (!p.projectId || !p.ownerUserId) continue;
    try {
      await db.transaction(async client => {
        await client.query(
          `INSERT INTO projects (project_id, owner_user_id, owner_name, project_title,
                                 project_xml, global_role, last_sync_date)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (project_id) DO NOTHING`,
          [p.projectId, p.ownerUserId, p.ownerName || 'Unknown',
           p.projectTitle || '', p.projectXml || '',
           p.globalRole || 'can_view',
           p.lastSyncDate ? new Date(p.lastSyncDate) : new Date()]
        );
        for (const u of (p.users || [])) {
          if (!u.userId) continue;
          await client.query(
            `INSERT INTO project_users (project_id, user_id, name, avatar_url, role)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (project_id, user_id) DO NOTHING`,
            [p.projectId, u.userId, u.name || 'Unknown', u.avatarUrl || '',
             u.role === undefined ? null : u.role]
          );
        }
      });
    } catch (e) {
      log('Migration', `WARN: failed to import project ${p.projectId}: ${e.message}`);
    }
  }

  // Rename legacy file so we don't re-import on next restart.
  try {
    fs.renameSync(LEGACY_STORAGE_FILE, LEGACY_STORAGE_FILE + '.migrated-' + Date.now());
  } catch (_) { /* keep going — migration succeeded even if rename failed */ }

  log('Migration', `Imported ${arr.length} project(s) from legacy JSON`);
}

// Kick off migration after schema is ready. Don't block the module load —
// individual handlers wait on db.ready() via db.query().
db.ready().then(migrateFromJsonIfNeeded).catch(e => {
  log('Migration', `WARN: migration check failed: ${e.message}`);
});

// --- Helpers ---

function ts() {
  return new Date().toISOString().slice(11, 23);
}

function log(tag, msg) {
  console.log(`${ts()} [${tag}] ${msg}`);
}

function jsonOk(res, data) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function jsonErr(res, status, message) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: message }));
}

function readBody(req) {
  return new Promise(resolve => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try { resolve(JSON.parse(body)); }
      catch { resolve({}); }
    });
  });
}

function isoDate(d) {
  return d instanceof Date ? d.toISOString() : (d || '');
}

// --- DB helpers ---

async function getProjectRow(projectId, client) {
  const q = (client || db);
  const r = await q.query('SELECT * FROM projects WHERE project_id = $1', [projectId]);
  return r.rows[0] || null;
}

async function getProjectUserRow(projectId, userId, client) {
  const q = (client || db);
  const r = await q.query(
    'SELECT * FROM project_users WHERE project_id = $1 AND user_id = $2',
    [projectId, userId]
  );
  return r.rows[0] || null;
}

// Centralised ownership check used by every write endpoint.
// Returns true when the caller IS the project's owner. On mismatch the helper
// writes a 403 response itself and returns false — handlers MUST early-return
// after a false (writing twice to `res` crashes Node's HTTP layer).
//
// `res` may be null when the caller wants a pure predicate (e.g. tests).
// In that case no HTTP response is written.
function assertIsOwner(projRow, callerUserId, res) {
  if (!projRow) {
    if (res) jsonErr(res, 404, 'Project not found');
    return false;
  }
  if (!callerUserId || callerUserId !== projRow.owner_user_id) {
    if (res) jsonErr(res, 403, 'Only the owner can perform this action');
    return false;
  }
  return true;
}

// Extracts the caller identity from the request body. New functionality —
// callerUserId is required on every write endpoint; missing/empty returns null
// and the handler responds with 403 via assertIsOwner.
function resolveCallerUserId(body) {
  return body && typeof body.callerUserId === 'string' && body.callerUserId !== ''
    ? body.callerUserId : null;
}

// Effective role: 'owner' if user.role == 'owner', else user.role (if set), else project.global_role.
// `userRow` may be null (user not in project) → falls back to global_role.
function getEffectiveRole(projRow, userRow) {
  if (!userRow) return projRow.global_role;
  if (userRow.role === 'owner') return 'owner';
  return userRow.role !== null && userRow.role !== undefined ? userRow.role : projRow.global_role;
}

// Pushes a RoleChanged message to every player currently in any session linked to this project.
// Also synchronizes session.players[*].role so the server-side canEdit() gate uses the fresh role.
// `userId === ''` means a global-role change — applies to all guests whose effective role inherits global.
async function broadcastRoleChanged(sessions, projectId, userId, newRole) {
  if (!sessions) return;

  const wireRole = (newRole === 'can_edit') ? 'editor' : 'viewer';
  const msg = JSON.stringify({ type: 'RoleChanged', projectId, userId: userId || '', newRole });

  // For a global change we need the set of users whose stored role is NULL
  // (= inherits global). For a per-user change we only target that one userId.
  let inheritingIds = null;
  if (!userId) {
    try {
      const r = await db.query(
        'SELECT user_id FROM project_users WHERE project_id = $1 AND role IS NULL',
        [projectId]
      );
      inheritingIds = new Set(r.rows.map(row => row.user_id));
    } catch (e) {
      log('Projects', `WARN: broadcastRoleChanged user lookup failed: ${e.message}`);
      inheritingIds = new Set();
    }
  }

  for (const [, s] of sessions) {
    if (s.projectId !== projectId) continue;
    for (const [, p] of s.players) {
      if (p.role !== 'owner') {
        const isTarget = userId
          ? p.userId === userId
          : (p.userId && inheritingIds.has(p.userId));
        if (isTarget) p.role = wireRole;
      }
      if (p.ws && p.ws.readyState === 1 /* OPEN */) {
        try { p.ws.send(msg); } catch (_) { /* peer disconnected mid-broadcast */ }
      }
    }
  }
}

// --- Per-user avatar colors (single source of truth) ---
// Палитра живёт на сервере. Цвет назначается члену проекта без коллизий (до размера палитры),
// хранится в project_users.color и отдаётся всем клиентам: HTTP (/users, /active-users) и WS-сессии.
const PLAYER_COLORS = [
  '#F8ED15', '#FFC935', '#F79009', '#F34439', '#EF0AFF',
  '#742AED', '#4C5FF0', '#5AA9FF', '#7CD4FD', '#4BD3CE',
];

function pickFreeColor(usedColors) {
  const free = PLAYER_COLORS.filter(c => !usedColors.has(c));
  const pool = free.length > 0 ? free : PLAYER_COLORS;
  return pool[Math.floor(Math.random() * pool.length)];
}

// Назначает цвет каждому члену проекта, у которого его ещё нет (новые вступления + backfill старых строк).
// Без коллизий в пределах палитры; при переполнении (> размера палитры) цвета повторяются.
// Транзакция + блокировка строк, чтобы конкурентные вызовы не выдали один цвет двум членам.
async function ensureColors(projectId) {
  const probe = await db.query(
    `SELECT 1 FROM project_users WHERE project_id = $1 AND (color IS NULL OR color = '') LIMIT 1`,
    [projectId]
  );
  if (probe.rows.length === 0) return; // быстрый путь — у всех уже есть цвет

  await db.transaction(async client => {
    const { rows } = await client.query(
      `SELECT user_id, color FROM project_users WHERE project_id = $1 ORDER BY created_at FOR UPDATE`,
      [projectId]
    );
    const used = new Set(rows.filter(r => r.color).map(r => r.color));
    for (const r of rows) {
      if (r.color) continue;
      const color = pickFreeColor(used);
      used.add(color);
      await client.query(
        `UPDATE project_users SET color = $1 WHERE project_id = $2 AND user_id = $3`,
        [color, projectId, r.user_id]
      );
    }
  });
}

// Цвет члена проекта (с ленивым назначением, если его ещё нет). Null для не-членов.
async function getUserColor(projectId, userId) {
  if (!projectId || !userId) return null;
  await ensureColors(projectId);
  const { rows } = await db.query(
    `SELECT color FROM project_users WHERE project_id = $1 AND user_id = $2`,
    [projectId, userId]
  );
  if (rows.length === 0) return null;
  return rows[0].color || null;
}

async function serializeUsers(projectId, projRow) {
  await ensureColors(projectId);
  const r = await db.query(
    `SELECT user_id, name, avatar_url, role, is_invitation_pending, color
     FROM project_users WHERE project_id = $1 AND role IS DISTINCT FROM 'no_access'
     ORDER BY created_at`,
    [projectId]
  );
  return r.rows.map(u => ({
    userId: u.user_id,
    name: u.name,
    avatarUrl: u.avatar_url,
    role: getEffectiveRole(projRow, u),
    isInvitationPending: Boolean(u.is_invitation_pending),
    color: u.color || ''
  }));
}

// --- Route handlers ---

async function handlePostProjects(req, res) {
  const body = await readBody(req);
  const { projectId, ownerUserId, ownerName, ownerAvatarUrl, projectXml, projectTitle } = body;

  if (!projectId || !ownerUserId) {
    return jsonErr(res, 400, 'projectId and ownerUserId are required');
  }

  try {
    // Atomic UPSERT to eliminate SELECT+INSERT race when two concurrent
    // RegisterProject calls fire (e.g. owner opens SharingPopup twice in
    // quick succession on flaky network). `xmax = 0` returns true for the
    // inserted row and false for the updated row — gives us insert-vs-update
    // signal in a single round-trip. Supported on PostgreSQL >= 9.5 (Neon ok).
    //
    // On UPDATE: keep existing owner / global_role, refresh xml/title/syncDate
    // ONLY when caller supplied them (COALESCE keeps title if caller sent '').
    const upsert = await db.query(
      `INSERT INTO projects (project_id, owner_user_id, owner_name, project_title,
                             project_xml, global_role, last_sync_date)
       VALUES ($1, $2, $3, $4, $5, 'can_view', NOW())
       ON CONFLICT (project_id) DO UPDATE
         SET project_xml = EXCLUDED.project_xml,
             project_title = CASE WHEN EXCLUDED.project_title <> ''
                                  THEN EXCLUDED.project_title
                                  ELSE projects.project_title END,
             last_sync_date = NOW()
       RETURNING (xmax = 0) AS inserted, owner_user_id`,
      [projectId, ownerUserId, ownerName || 'Unknown',
       projectTitle || '', projectXml || '']
    );

    const inserted = upsert.rows[0].inserted;
    const actualOwnerId = upsert.rows[0].owner_user_id;

    if (inserted) {
      // Brand-new project — insert the owner row in project_users.
      // Separate INSERT (not a transaction) is acceptable because the UPSERT
      // above is itself atomic; if this INSERT fails the projects row stays
      // ownerless-in-users-table and the next RegisterProject call won't
      // re-insert (ownership is recorded on `projects.owner_user_id`).
      await db.query(
        `INSERT INTO project_users (project_id, user_id, name, avatar_url, role)
         VALUES ($1, $2, $3, $4, 'owner')
         ON CONFLICT (project_id, user_id) DO NOTHING`,
        [projectId, ownerUserId, ownerName || 'Unknown', ownerAvatarUrl || '']
      );
      log('Projects', `Registered project ${projectId} by owner ${ownerUserId}`);
    } else {
      // Re-registration: refresh owner's avatar if provided. Owner identity
      // (projects.owner_user_id) is NOT changed — only the original owner
      // can re-register, foreign actors would land here through other endpoints
      // protected by assertIsOwner.
      if (ownerAvatarUrl && actualOwnerId === ownerUserId) {
        await db.query(
          `UPDATE project_users SET avatar_url = $1
           WHERE project_id = $2 AND user_id = $3`,
          [ownerAvatarUrl, projectId, ownerUserId]
        );
      }
      log('Projects', `Re-registered project ${projectId} by owner ${ownerUserId} (existing owner: ${actualOwnerId})`);
    }

    jsonOk(res, { ok: true, projectId, shareUrl: `/projects/${projectId}` });
  } catch (e) {
    log('Projects', `ERROR handlePostProjects: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleJoinProject(req, res, projectId) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const body = await readBody(req);
    const { userId, name, avatarUrl } = body;

    if (!userId) return jsonErr(res, 400, 'userId is required');

    const userRow = await getProjectUserRow(projectId, userId);
    if (userRow && userRow.role === 'no_access') {
      // Owner revoked this user's access (handleDeleteUser). The share link is no
      // longer valid for them — reject with a distinct code the client maps to a
      // "link unavailable" message instead of re-granting inherited access.
      log('Projects', `Rejected join: user ${userId} has no_access in project ${projectId}`);
      return jsonErr(res, 403, 'no_access');
    }
    if (userRow) {
      // Joining accepts any pending invitation — clear the flag unconditionally
      // (the WHERE narrows to rows still flagged, so this is a no-op otherwise).
      await db.query(
        `UPDATE project_users
         SET is_invitation_pending = false
         WHERE project_id = $1 AND user_id = $2 AND is_invitation_pending = true`,
        [projectId, userId]
      );
      const effectiveRole = getEffectiveRole(projRow, userRow);
      log('Projects', `User ${userId} already in project ${projectId} (role: ${effectiveRole})`);
      return jsonOk(res, { ok: true, role: effectiveRole, alreadyMember: true });
    }

    await db.query(
      `INSERT INTO project_users (project_id, user_id, name, avatar_url, role, is_invitation_pending)
       VALUES ($1, $2, $3, $4, NULL, false)`,
      [projectId, userId, name || 'Unknown', avatarUrl || '']
    );

    log('Projects', `User ${userId} (${name || 'Unknown'}) joined project ${projectId} (inherits globalRole: ${projRow.global_role})`);
    jsonOk(res, { ok: true, role: projRow.global_role });
  } catch (e) {
    log('Projects', `ERROR handleJoinProject: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

// POST /projects/:projectId/users/:userId/invite
// Owner-only. Adds (project, userId) with is_invitation_pending = true only if the
// row does not already exist. If the user is already in the project (whether still
// pending or already joined), the invite is a no-op — re-inviting an already-joined
// user must NOT flip the pending flag back to true.
async function handleInviteUser(req, res, projectId, userId) {
  try {
    const body = await readBody(req);
    const name = body && typeof body.name === 'string' ? body.name : '';
    const avatarUrl = body && typeof body.avatarUrl === 'string' ? body.avatarUrl : '';
    const callerUserId = resolveCallerUserId(body);

    if (!projectId || !userId) {
      return jsonErr(res, 400, 'projectId and userId are required');
    }

    const projRow = await getProjectRow(projectId);
    if (!assertIsOwner(projRow, callerUserId, res)) return;

    if (userId === projRow.owner_user_id) {
      return jsonErr(res, 409, 'cannot invite self');
    }

    await db.transaction(async client => {
      await client.query(
        `INSERT INTO project_users (project_id, user_id, name, avatar_url, role, is_invitation_pending)
         VALUES ($1, $2, $3, $4, NULL, true)
         ON CONFLICT (project_id, user_id) DO NOTHING`,
        [projectId, userId, name, avatarUrl]
      );
    });

    log('Projects', `User ${userId} invited to project ${projectId} by owner ${callerUserId}`);
    jsonOk(res, { ok: true });
  } catch (e) {
    log('Projects', `ERROR handleInviteUser: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleGetProject(res, projectId) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const { rows } = await db.query(
      `SELECT COUNT(*)::int AS n FROM project_users
       WHERE project_id = $1 AND role IS DISTINCT FROM 'no_access'`,
      [projectId]
    );
    // ownerUserId намеренно НЕ отдаём — клиент держит ownership locally
    // через ProjectOwnershipController. assertIsOwner всё ещё авторитативно
    // проверяет владельца при write-операциях, читая projects.owner_user_id
    // из БД — но в read-API это поле клиенту не нужно.
    jsonOk(res, {
      projectId: projRow.project_id,
      ownerName: projRow.owner_name,
      lastSyncDate: isoDate(projRow.last_sync_date),
      globalRole: projRow.global_role || 'can_view',
      userCount: rows[0].n
    });
  } catch (e) {
    log('Projects', `ERROR handleGetProject: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleGetProjectUsers(res, projectId) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const users = await serializeUsers(projectId, projRow);
    jsonOk(res, { globalRole: projRow.global_role, users });
  } catch (e) {
    log('Projects', `ERROR handleGetProjectUsers: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleGetUserProjects(res, userId) {
  try {
    const { rows } = await db.query(
      `SELECT p.project_id, p.owner_name, p.project_title, p.global_role,
              p.last_sync_date, pu.role AS user_role
       FROM projects p
       JOIN project_users pu ON pu.project_id = p.project_id
       WHERE pu.user_id = $1 AND pu.role IS DISTINCT FROM 'no_access'
       ORDER BY p.last_sync_date DESC`,
      [userId]
    );

    const result = rows.map(p => {
      const role = p.user_role === 'owner'
        ? 'owner'
        : (p.user_role !== null && p.user_role !== undefined ? p.user_role : p.global_role);
      return {
        projectId: p.project_id,
        ownerName: p.owner_name,
        projectTitle: p.project_title || '',
        role,
        lastSyncDate: isoDate(p.last_sync_date)
      };
    });

    jsonOk(res, { projects: result });
  } catch (e) {
    log('Projects', `ERROR handleGetUserProjects: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handlePutUserRole(req, res, projectId, userId, sessions) {
  try {
    const body = await readBody(req);
    const { role } = body;
    const callerUserId = resolveCallerUserId(body);

    const projRow = await getProjectRow(projectId);
    // Closes critical gap: previously NO ownership check at all — any caller
    // could change anyone's role on any project.
    if (!assertIsOwner(projRow, callerUserId, res)) return;

    const userRow = await getProjectUserRow(projectId, userId);
    if (!userRow) return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);

    if (userId === projRow.owner_user_id) {
      return jsonErr(res, 400, "Cannot change the owner's role");
    }

    if (!['can_view', 'can_edit'].includes(role)) {
      return jsonErr(res, 400, 'role must be can_view or can_edit');
    }

    await db.query(
      'UPDATE project_users SET role = $1 WHERE project_id = $2 AND user_id = $3',
      [role, projectId, userId]
    );
    log('Projects', `Role of user ${userId} in project ${projectId} changed to ${role}`);

    await broadcastRoleChanged(sessions, projectId, userId, role);

    jsonOk(res, { ok: true, role });
  } catch (e) {
    log('Projects', `ERROR handlePutUserRole: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handlePutProjectGlobalRole(req, res, projectId, sessions) {
  try {
    const body = await readBody(req);
    const { globalRole } = body;
    const callerUserId = resolveCallerUserId(body);

    const projRow = await getProjectRow(projectId);
    if (!assertIsOwner(projRow, callerUserId, res)) return;

    if (!['can_view', 'can_edit'].includes(globalRole)) {
      return jsonErr(res, 400, 'globalRole must be can_view or can_edit');
    }

    await db.query(
      'UPDATE projects SET global_role = $1 WHERE project_id = $2',
      [globalRole, projectId]
    );
    log('Projects', `Global role of project ${projectId} changed to ${globalRole} by owner ${callerUserId}`);

    // userId === '' = global role change applies to everyone whose effective role is 'global'.
    // Per-user role overrides are NOT rewritten — they keep their explicit assignment.
    await broadcastRoleChanged(sessions, projectId, '', globalRole);

    jsonOk(res, { ok: true, globalRole });
  } catch (e) {
    log('Projects', `ERROR handlePutProjectGlobalRole: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handlePutSync(req, res, projectId) {
  try {
    const r = await db.query(
      'UPDATE projects SET last_sync_date = NOW() WHERE project_id = $1 RETURNING last_sync_date',
      [projectId]
    );
    if (r.rowCount === 0) return jsonErr(res, 404, `Project ${projectId} not found`);

    const lastSyncDate = isoDate(r.rows[0].last_sync_date);
    log('Projects', `Sync date updated for project ${projectId}`);
    jsonOk(res, { ok: true, lastSyncDate });
  } catch (e) {
    log('Projects', `ERROR handlePutSync: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleDeleteAllUsers(req, res, projectId) {
  try {
    const body = await readBody(req);
    const callerUserId = resolveCallerUserId(body);

    const projRow = await getProjectRow(projectId);
    if (!assertIsOwner(projRow, callerUserId, res)) return;

    const r = await db.query(
      'DELETE FROM project_users WHERE project_id = $1 AND user_id <> $2',
      [projectId, projRow.owner_user_id]
    );

    log('Projects', `All ${r.rowCount} non-owner users removed from project ${projectId} by owner`);
    jsonOk(res, { ok: true, removedCount: r.rowCount });
  } catch (e) {
    log('Projects', `ERROR handleDeleteAllUsers: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleDeleteProject(req, res, projectId, sessions, projectIndex) {
  try {
    const body = await readBody(req);
    const callerUserId = resolveCallerUserId(body);

    const projRow = await getProjectRow(projectId);
    if (!assertIsOwner(projRow, callerUserId, res)) return;

    // ON DELETE CASCADE on project_users.project_id removes user rows automatically.
    await db.query('DELETE FROM projects WHERE project_id = $1', [projectId]);
    log('Projects', `Project ${projectId} deleted by owner ${callerUserId}`);

    // Terminate all active multiplayer sessions linked to this project.
    if (sessions) {
      // Wire format is PascalCase to match Unity-side MessageType (case-sensitive Enum.TryParse).
      const closedMsg = JSON.stringify({ type: 'SessionClosed', reason: 'sharing_stopped' });
      for (const [inviteCode, s] of sessions) {
        if (s.projectId !== projectId) continue;
        for (const [, player] of s.players) {
          if (player.ws && player.ws.readyState === 1 /* OPEN */) {
            try { player.ws.send(closedMsg); } catch (_) { /* peer disconnected */ }
            player.ws.close();
          }
        }
        sessions.delete(inviteCode);
        if (projectIndex && s.projectId) projectIndex.delete(s.projectId);
        log('Projects', `Session ${inviteCode} terminated (project ${projectId} sharing stopped)`);
      }
    }

    jsonOk(res, { ok: true });
  } catch (e) {
    log('Projects', `ERROR handleDeleteProject: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleDeleteUser(req, res, projectId, userId, sessions) {
  try {
    const body = await readBody(req);
    const callerUserId = resolveCallerUserId(body);

    const projRow = await getProjectRow(projectId);
    if (!assertIsOwner(projRow, callerUserId, res)) return;

    if (userId === projRow.owner_user_id) {
      return jsonErr(res, 400, 'Cannot remove the owner');
    }

    // Soft-remove: keep the row but downgrade to 'no_access'. This lets the
    // server recognise a revoked user if they try to re-open the share link
    // (handleJoinProject rejects 'no_access') instead of silently re-granting
    // them inherited access. is_invitation_pending is cleared so the stale row
    // never resurfaces as a pending invite.
    const r = await db.query(
      `UPDATE project_users SET role = 'no_access', is_invitation_pending = false
       WHERE project_id = $1 AND user_id = $2`,
      [projectId, userId]
    );
    if (r.rowCount === 0) return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);

    log('Projects', `Access of user ${userId} revoked (no_access) in project ${projectId} by owner`);

    // Notify the kicked player (if currently in a session linked to this project) and force-close their WS.
    // server.js leaveSession() handles ownership transfer + PlayerLeft broadcast.
    if (sessions) {
      const kickMsg = JSON.stringify({ type: 'UserRemoved', projectId, userId });
      for (const [, s] of sessions) {
        if (s.projectId !== projectId) continue;
        for (const [, p] of s.players) {
          if (p.userId !== userId) continue;
          if (p.ws && p.ws.readyState === 1 /* OPEN */) {
            try { p.ws.send(kickMsg); } catch (_) { /* peer disconnected */ }
            try { p.ws.close(); } catch (_) { /* socket already closing */ }
          }
        }
      }
    }

    jsonOk(res, { ok: true });
  } catch (e) {
    log('Projects', `ERROR handleDeleteUser: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleLeaveProject(req, res, projectId) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const body = await readBody(req);
    const { userId } = body;

    if (!userId) return jsonErr(res, 400, 'userId is required');
    if (userId === projRow.owner_user_id) {
      return jsonErr(res, 400, 'Owner cannot leave the project');
    }

    const r = await db.query(
      'DELETE FROM project_users WHERE project_id = $1 AND user_id = $2',
      [projectId, userId]
    );
    if (r.rowCount === 0) return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);

    log('Projects', `User ${userId} left project ${projectId}`);
    jsonOk(res, { ok: true });
  } catch (e) {
    log('Projects', `ERROR handleLeaveProject: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleGetProjectXml(res, projectId) {
  try {
    const r = await db.query(
      'SELECT project_xml FROM projects WHERE project_id = $1',
      [projectId]
    );
    if (r.rowCount === 0) return jsonErr(res, 404, `Project ${projectId} not found`);

    const xml = r.rows[0].project_xml || '';
    res.writeHead(200, { 'Content-Type': 'text/xml; charset=utf-8' });
    res.end(xml);
  } catch (e) {
    log('Projects', `ERROR handleGetProjectXml: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleGetProjectSession(res, projectId, sessions) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    for (const [inviteCode, s] of sessions) {
      if (s.projectId === projectId && s.players.size > 0) {
        return jsonOk(res, { inviteCode });
      }
    }
    jsonErr(res, 404, 'No active session for this project');
  } catch (e) {
    log('Projects', `ERROR handleGetProjectSession: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

// Returns players currently online inside the multiplayer session linked to this project.
// Distinct from /users (= everyone with access). Empty list if no active session is linked.
async function handleGetProjectActiveUsers(res, projectId, sessions) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const list = [];
    for (const [, s] of sessions) {
      if (s.projectId !== projectId) continue;
      for (const [, p] of s.players) {
        list.push({
          userId: p.userId || '',
          name: p.userName || '',
          avatarUrl: p.avatarUrl || '',
          color: p.color || ''
        });
      }
    }
    jsonOk(res, { users: list });
  } catch (e) {
    log('Projects', `ERROR handleGetProjectActiveUsers: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

async function handleLinkSession(req, res, inviteCode, sessions, projectIndex) {
  try {
    const body = await readBody(req);
    const { projectId } = body;

    if (!projectId) return jsonErr(res, 400, 'projectId is required');

    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    if (inviteCode === '__legacy__') return jsonErr(res, 400, 'Cannot link legacy session');

    const session = sessions.get(inviteCode);
    if (!session) return jsonErr(res, 404, `Session ${inviteCode} not found`);

    if (session.projectId === projectId) {
      return jsonOk(res, { ok: true, alreadyLinked: true });
    }
    if (session.projectId && session.projectId !== projectId) {
      return jsonErr(res, 409, 'Session already linked to another project');
    }

    // session.projectId == null → adopt the new projectId.
    // Guard: refuse if another session is already indexed for this projectId.
    if (projectIndex && projectIndex.has(projectId) && projectIndex.get(projectId) !== inviteCode) {
      return jsonErr(res, 409, 'Another session is already linked to this project');
    }

    session.projectId = projectId;
    if (projectIndex) projectIndex.set(projectId, inviteCode);

    log('Projects', `Session ${inviteCode} linked to project ${projectId}`);
    jsonOk(res, { ok: true });
  } catch (e) {
    log('Projects', `ERROR handleLinkSession: ${e.message}`);
    jsonErr(res, 500, 'Internal error');
  }
}

// --- Main export ---

function handleRequest(req, res, url, sessions, projectIndex) {
  const p = url.pathname;

  // POST /projects
  if (p === '/projects' && req.method === 'POST') {
    handlePostProjects(req, res);
    return true;
  }

  // GET /projects/user/:uid  (check before /:id to avoid conflict)
  const userProjectsM = p.match(/^\/projects\/user\/([^\/]+)$/);
  if (userProjectsM && req.method === 'GET') {
    handleGetUserProjects(res, decodeURIComponent(userProjectsM[1]));
    return true;
  }

  // POST /projects/:id/join
  const joinM = p.match(/^\/projects\/([^\/]+)\/join$/);
  if (joinM && req.method === 'POST') {
    handleJoinProject(req, res, joinM[1]);
    return true;
  }

  // POST /projects/:id/leave
  const leaveM = p.match(/^\/projects\/([^\/]+)\/leave$/);
  if (leaveM && req.method === 'POST') {
    handleLeaveProject(req, res, leaveM[1]);
    return true;
  }

  // PUT /projects/:id/users/:uid/role
  const userRoleM = p.match(/^\/projects\/([^\/]+)\/users\/([^\/]+)\/role$/);
  if (userRoleM && req.method === 'PUT') {
    handlePutUserRole(req, res, userRoleM[1], decodeURIComponent(userRoleM[2]), sessions);
    return true;
  }

  // POST /projects/:id/users/:uid/invite
  const inviteM = p.match(/^\/projects\/([^\/]+)\/users\/([^\/]+)\/invite$/);
  if (inviteM && req.method === 'POST') {
    handleInviteUser(req, res, inviteM[1], decodeURIComponent(inviteM[2]));
    return true;
  }

  // DELETE /projects/:id/users/:uid
  const userM = p.match(/^\/projects\/([^\/]+)\/users\/([^\/]+)$/);
  if (userM && req.method === 'DELETE') {
    handleDeleteUser(req, res, userM[1], decodeURIComponent(userM[2]), sessions);
    return true;
  }

  // GET|DELETE /projects/:id/users
  const usersM = p.match(/^\/projects\/([^\/]+)\/users$/);
  if (usersM) {
    if (req.method === 'GET') { handleGetProjectUsers(res, usersM[1]); return true; }
    if (req.method === 'DELETE') { handleDeleteAllUsers(req, res, usersM[1]); return true; }
  }

  // PUT /projects/:id/globalRole
  const projGlobalRoleM = p.match(/^\/projects\/([^\/]+)\/globalRole$/);
  if (projGlobalRoleM && req.method === 'PUT') {
    handlePutProjectGlobalRole(req, res, projGlobalRoleM[1], sessions);
    return true;
  }

  // PUT /projects/:id/sync
  const syncM = p.match(/^\/projects\/([^\/]+)\/sync$/);
  if (syncM && req.method === 'PUT') {
    handlePutSync(req, res, syncM[1]);
    return true;
  }

  // GET /projects/:id/xml — fetch stored project XML
  const projXmlM = p.match(/^\/projects\/([^\/]+)\/xml$/);
  if (projXmlM && req.method === 'GET') {
    handleGetProjectXml(res, projXmlM[1]);
    return true;
  }

  // GET /projects/:id/session — find active session linked to this project
  const projSessionM = p.match(/^\/projects\/([^\/]+)\/session$/);
  if (projSessionM && req.method === 'GET') {
    handleGetProjectSession(res, projSessionM[1], sessions);
    return true;
  }

  // GET /projects/:id/active-users — players currently in the session linked to this project
  const projActiveUsersM = p.match(/^\/projects\/([^\/]+)\/active-users$/);
  if (projActiveUsersM && req.method === 'GET') {
    handleGetProjectActiveUsers(res, projActiveUsersM[1], sessions);
    return true;
  }

  // GET|DELETE /projects/:id
  const projectM = p.match(/^\/projects\/([^\/]+)$/);
  if (projectM) {
    if (req.method === 'GET') { handleGetProject(res, projectM[1]); return true; }
    if (req.method === 'DELETE') { handleDeleteProject(req, res, projectM[1], sessions, projectIndex); return true; }
  }

  // PUT /sessions/:inviteCode/project
  const sessLinkM = p.match(/^\/sessions\/([^\/]+)\/project$/);
  if (sessLinkM && req.method === 'PUT') {
    handleLinkSession(req, res, sessLinkM[1], sessions, projectIndex);
    return true;
  }

  return false;
}

// Fire-and-forget XML update from session.UpdateState. The previous in-memory
// implementation was synchronous; with Neon we accept eventual consistency —
// errors are logged but never bubble up to the WS handler.
function onXmlUpdated(projectId, xml) {
  if (!projectId) return;
  db.query(
    'UPDATE projects SET project_xml = $1, last_sync_date = NOW() WHERE project_id = $2',
    [xml || '', projectId]
  ).catch(e => log('Projects', `WARN: onXmlUpdated failed for ${projectId}: ${e.message}`));
}

module.exports = {
  handleRequest, onXmlUpdated,
  getProjectRow, getProjectUserRow, getEffectiveRole,
  assertIsOwner, resolveCallerUserId,
  getUserColor, PLAYER_COLORS
};
