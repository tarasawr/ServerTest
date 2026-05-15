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

async function serializeUsers(projectId, projRow) {
  const r = await db.query(
    `SELECT user_id, name, avatar_url, role, is_invitation_pending
     FROM project_users WHERE project_id = $1
     ORDER BY created_at`,
    [projectId]
  );
  return r.rows.map(u => ({
    userId: u.user_id,
    name: u.name,
    avatarUrl: u.avatar_url,
    role: getEffectiveRole(projRow, u),
    isInvitationPending: Boolean(u.is_invitation_pending)
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
    const existing = await getProjectRow(projectId);

    if (existing) {
      // Re-registration: idempotent update of xml/title/syncDate, owner stays as-is.
      const updates = [];
      const params = [];
      let i = 1;
      if (typeof projectXml === 'string') {
        updates.push(`project_xml = $${i++}`);
        params.push(projectXml);
      }
      if (typeof projectTitle === 'string' && projectTitle !== '') {
        updates.push(`project_title = $${i++}`);
        params.push(projectTitle);
      }
      updates.push(`last_sync_date = NOW()`);
      params.push(projectId);
      await db.query(
        `UPDATE projects SET ${updates.join(', ')} WHERE project_id = $${i}`,
        params
      );

      if (ownerAvatarUrl) {
        await db.query(
          `UPDATE project_users SET avatar_url = $1
           WHERE project_id = $2 AND user_id = $3`,
          [ownerAvatarUrl, projectId, ownerUserId]
        );
      }

      log('Projects', `Re-registered project ${projectId} by owner ${ownerUserId}`);
      return jsonOk(res, { ok: true, projectId, shareUrl: `/projects/${projectId}` });
    }

    // New project: insert project row + owner user atomically.
    await db.transaction(async client => {
      await client.query(
        `INSERT INTO projects (project_id, owner_user_id, owner_name, project_title,
                               project_xml, global_role)
         VALUES ($1, $2, $3, $4, $5, 'can_view')`,
        [projectId, ownerUserId, ownerName || 'Unknown',
         projectTitle || '', projectXml || '']
      );
      await client.query(
        `INSERT INTO project_users (project_id, user_id, name, avatar_url, role)
         VALUES ($1, $2, $3, $4, 'owner')`,
        [projectId, ownerUserId, ownerName || 'Unknown', ownerAvatarUrl || '']
      );
    });

    log('Projects', `Registered project ${projectId} by owner ${ownerUserId}`);
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
    const ownerUserId = body && body.ownerUserId ? body.ownerUserId : '';
    const name = body && typeof body.name === 'string' ? body.name : '';
    const avatarUrl = body && typeof body.avatarUrl === 'string' ? body.avatarUrl : '';

    if (!projectId || !userId || !ownerUserId) {
      return jsonErr(res, 400, 'projectId, userId and ownerUserId are required');
    }

    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    if (projRow.owner_user_id !== ownerUserId) {
      return jsonErr(res, 403, 'Only the owner can invite users');
    }

    if (userId === ownerUserId) {
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

    log('Projects', `User ${userId} invited to project ${projectId} by owner ${ownerUserId}`);
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
      'SELECT COUNT(*)::int AS n FROM project_users WHERE project_id = $1',
      [projectId]
    );
    jsonOk(res, {
      projectId: projRow.project_id,
      ownerUserId: projRow.owner_user_id,
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

async function handleGetUserRole(res, projectId, userId) {
  try {
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const userRow = await getProjectUserRow(projectId, userId);
    if (!userRow) return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);

    jsonOk(res, { userId, role: getEffectiveRole(projRow, userRow) });
  } catch (e) {
    log('Projects', `ERROR handleGetUserRole: ${e.message}`);
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
       WHERE pu.user_id = $1
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
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const userRow = await getProjectUserRow(projectId, userId);
    if (!userRow) return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);

    if (userId === projRow.owner_user_id) {
      return jsonErr(res, 400, "Cannot change the owner's role");
    }

    const body = await readBody(req);
    const { role } = body;
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
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const body = await readBody(req);
    const { globalRole, ownerUserId } = body;

    if (ownerUserId !== projRow.owner_user_id) {
      return jsonErr(res, 403, 'Only the owner can change the project global role');
    }
    if (!['can_view', 'can_edit'].includes(globalRole)) {
      return jsonErr(res, 400, 'globalRole must be can_view or can_edit');
    }

    await db.query(
      'UPDATE projects SET global_role = $1 WHERE project_id = $2',
      [globalRole, projectId]
    );
    log('Projects', `Global role of project ${projectId} changed to ${globalRole} by owner ${ownerUserId}`);

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
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const body = await readBody(req);
    const { ownerUserId } = body;

    if (ownerUserId !== projRow.owner_user_id) {
      return jsonErr(res, 403, 'Only the owner can remove all users');
    }

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
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const body = await readBody(req);
    const { ownerUserId } = body;

    if (ownerUserId !== projRow.owner_user_id) {
      return jsonErr(res, 403, 'Only the owner can delete the project');
    }

    // ON DELETE CASCADE on project_users.project_id removes user rows automatically.
    await db.query('DELETE FROM projects WHERE project_id = $1', [projectId]);
    log('Projects', `Project ${projectId} deleted by owner ${ownerUserId}`);

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
    const projRow = await getProjectRow(projectId);
    if (!projRow) return jsonErr(res, 404, `Project ${projectId} not found`);

    const body = await readBody(req);
    const { ownerUserId } = body;

    if (ownerUserId !== projRow.owner_user_id) {
      return jsonErr(res, 403, 'Only the owner can remove users');
    }
    if (userId === projRow.owner_user_id) {
      return jsonErr(res, 400, 'Cannot remove the owner');
    }

    const r = await db.query(
      'DELETE FROM project_users WHERE project_id = $1 AND user_id = $2',
      [projectId, userId]
    );
    if (r.rowCount === 0) return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);

    log('Projects', `User ${userId} removed from project ${projectId} by owner`);

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
          avatarUrl: p.avatarUrl || ''
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

  // GET/PUT /projects/:id/users/:uid/role
  const userRoleM = p.match(/^\/projects\/([^\/]+)\/users\/([^\/]+)\/role$/);
  if (userRoleM) {
    if (req.method === 'GET') { handleGetUserRole(res, userRoleM[1], decodeURIComponent(userRoleM[2])); return true; }
    if (req.method === 'PUT') { handlePutUserRole(req, res, userRoleM[1], decodeURIComponent(userRoleM[2]), sessions); return true; }
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
  getProjectRow, getProjectUserRow, getEffectiveRole
};
