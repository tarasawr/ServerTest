'use strict';

// --- Projects database ---
// projectId → {
//   projectId, ownerUserId, ownerName,
//   projectXml, lastSyncDate,
//   globalRole: 'can_view' | 'can_edit',    ← project-level default for all users
//   users: Map<userId, { userId, name, avatarUrl, role, isGuest }>
//   role: 'owner' | 'can_edit' | 'can_view' | null
//   null role means "inherit from project globalRole"
// }

const projects = new Map();

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

// Returns the effective role for a user: individual role if set, otherwise project globalRole.
// Owner always returns 'owner'.
function getEffectiveRole(proj, userId) {
  const user = proj.users.get(userId);
  if (!user) return proj.globalRole;
  if (user.role === 'owner') return 'owner';
  return user.role !== null ? user.role : proj.globalRole;
}

function serializeUsers(usersMap, proj) {
  const list = [];
  for (const [, u] of usersMap) {
    list.push({
      userId: u.userId,
      name: u.name,
      avatarUrl: u.avatarUrl,
      role: getEffectiveRole(proj, u.userId),  // always non-null effective role
      isGuest: u.isGuest
    });
  }
  return list;
}

// --- Route handlers ---

async function handlePostProjects(req, res) {
  const body = await readBody(req);
  const { projectId, ownerUserId, ownerName, projectXml } = body;

  if (!projectId || !ownerUserId) {
    return jsonErr(res, 400, 'projectId and ownerUserId are required');
  }

  if (projects.has(projectId)) {
    // Re-registration: update owner info and XML (idempotent)
    const p = projects.get(projectId);
    if (projectXml) p.projectXml = projectXml;
    p.lastSyncDate = new Date().toISOString();
    log('Projects', `Re-registered project ${projectId} by owner ${ownerUserId}`);
    return jsonOk(res, { ok: true, projectId, shareUrl: `/projects/${projectId}` });
  }

  const owner = { userId: ownerUserId, name: ownerName || 'Unknown', avatarUrl: '', role: 'owner', isGuest: false };
  const users = new Map([[ownerUserId, owner]]);
  projects.set(projectId, {
    projectId,
    ownerUserId,
    ownerName: ownerName || 'Unknown',
    projectXml: projectXml || '',
    lastSyncDate: new Date().toISOString(),
    globalRole: 'can_view',
    users
  });

  log('Projects', `Registered project ${projectId} by owner ${ownerUserId}`);
  jsonOk(res, { ok: true, projectId, shareUrl: `/projects/${projectId}` });
}

async function handleJoinProject(req, res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }

  const body = await readBody(req);
  const { userId, name, avatarUrl, isGuest } = body;

  if (!userId) {
    return jsonErr(res, 400, 'userId is required');
  }

  const proj = projects.get(projectId);

  // Already in project → return effective role
  if (proj.users.has(userId)) {
    const effectiveRole = getEffectiveRole(proj, userId);
    log('Projects', `User ${userId} already in project ${projectId} (role: ${effectiveRole})`);
    return jsonOk(res, { ok: true, role: effectiveRole, alreadyMember: true });
  }

  const user = {
    userId,
    name: name || (isGuest ? 'Guest' : 'Unknown'),
    avatarUrl: avatarUrl || '',
    role: null,   // inherits project globalRole
    isGuest: !!isGuest
  };
  proj.users.set(userId, user);

  log('Projects', `User ${userId} (${user.name}) joined project ${projectId} (inherits globalRole: ${proj.globalRole})`);
  jsonOk(res, { ok: true, role: proj.globalRole });
}

function handleGetProject(res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const p = projects.get(projectId);
  jsonOk(res, {
    projectId: p.projectId,
    ownerUserId: p.ownerUserId,
    ownerName: p.ownerName,
    lastSyncDate: p.lastSyncDate,
    globalRole: p.globalRole,
    userCount: p.users.size
  });
}

function handleGetProjectUsers(res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);
  jsonOk(res, {
    globalRole: proj.globalRole,
    users: serializeUsers(proj.users, proj)
  });
}

function handleGetUserRole(res, projectId, userId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);
  if (!proj.users.has(userId)) {
    return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);
  }
  jsonOk(res, { userId, role: getEffectiveRole(proj, userId) });
}

function handleGetUserProjects(res, userId) {
  const result = [];
  for (const [, p] of projects) {
    if (p.users.has(userId)) {
      result.push({
        projectId: p.projectId,
        ownerName: p.ownerName,
        role: getEffectiveRole(p, userId),
        lastSyncDate: p.lastSyncDate
      });
    }
  }
  jsonOk(res, { projects: result });
}

async function handlePutUserRole(req, res, projectId, userId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);
  if (!proj.users.has(userId)) {
    return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);
  }
  if (userId === proj.ownerUserId) {
    return jsonErr(res, 400, 'Cannot change the owner\'s role');
  }

  const body = await readBody(req);
  const { role } = body;
  if (!['can_view', 'can_edit'].includes(role)) {
    return jsonErr(res, 400, 'role must be can_view or can_edit');
  }

  proj.users.get(userId).role = role;
  log('Projects', `Role of user ${userId} in project ${projectId} changed to ${role}`);
  jsonOk(res, { ok: true, role });
}

async function handlePutProjectGlobalRole(req, res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);

  const body = await readBody(req);
  const { globalRole, ownerUserId } = body;

  if (ownerUserId !== proj.ownerUserId) {
    return jsonErr(res, 403, 'Only the owner can change the project global role');
  }
  if (!['can_view', 'can_edit'].includes(globalRole)) {
    return jsonErr(res, 400, 'globalRole must be can_view or can_edit');
  }

  proj.globalRole = globalRole;
  log('Projects', `Global role of project ${projectId} changed to ${globalRole} by owner ${ownerUserId}`);
  jsonOk(res, { ok: true, globalRole });
}

async function handlePutSync(req, res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const lastSyncDate = new Date().toISOString();
  projects.get(projectId).lastSyncDate = lastSyncDate;
  log('Projects', `Sync date updated for project ${projectId}`);
  jsonOk(res, { ok: true, lastSyncDate });
}

async function handleDeleteAllUsers(req, res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);

  const body = await readBody(req);
  const { ownerUserId } = body;

  if (ownerUserId !== proj.ownerUserId) {
    return jsonErr(res, 403, 'Only the owner can remove all users');
  }

  let removedCount = 0;
  for (const [userId] of proj.users) {
    if (userId !== proj.ownerUserId) {
      proj.users.delete(userId);
      removedCount++;
    }
  }

  log('Projects', `All ${removedCount} non-owner users removed from project ${projectId} by owner`);
  jsonOk(res, { ok: true, removedCount });
}

async function handleDeleteUser(req, res, projectId, userId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);

  const body = await readBody(req);
  const { ownerUserId } = body;

  if (ownerUserId !== proj.ownerUserId) {
    return jsonErr(res, 403, 'Only the owner can remove users');
  }
  if (userId === proj.ownerUserId) {
    return jsonErr(res, 400, 'Cannot remove the owner');
  }
  if (!proj.users.has(userId)) {
    return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);
  }

  proj.users.delete(userId);
  log('Projects', `User ${userId} removed from project ${projectId} by owner`);
  jsonOk(res, { ok: true });
}

async function handleLinkSession(req, res, inviteCode, sessions) {
  const body = await readBody(req);
  const { projectId } = body;

  if (!projectId) {
    return jsonErr(res, 400, 'projectId is required');
  }
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }

  const session = sessions.get(inviteCode);
  if (!session) {
    return jsonErr(res, 404, `Session ${inviteCode} not found`);
  }

  session.projectId = projectId;
  log('Projects', `Session ${inviteCode} linked to project ${projectId}`);
  jsonOk(res, { ok: true });
}

// --- Main export ---

function handleRequest(req, res, url, sessions) {
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

  // GET/PUT /projects/:id/users/:uid/role
  const userRoleM = p.match(/^\/projects\/([^\/]+)\/users\/([^\/]+)\/role$/);
  if (userRoleM) {
    if (req.method === 'GET') { handleGetUserRole(res, userRoleM[1], decodeURIComponent(userRoleM[2])); return true; }
    if (req.method === 'PUT') { handlePutUserRole(req, res, userRoleM[1], decodeURIComponent(userRoleM[2])); return true; }
  }

  // DELETE /projects/:id/users/:uid
  const userM = p.match(/^\/projects\/([^\/]+)\/users\/([^\/]+)$/);
  if (userM && req.method === 'DELETE') {
    handleDeleteUser(req, res, userM[1], decodeURIComponent(userM[2]));
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
    handlePutProjectGlobalRole(req, res, projGlobalRoleM[1]);
    return true;
  }

  // PUT /projects/:id/sync
  const syncM = p.match(/^\/projects\/([^\/]+)\/sync$/);
  if (syncM && req.method === 'PUT') {
    handlePutSync(req, res, syncM[1]);
    return true;
  }

  // GET /projects/:id
  const projectM = p.match(/^\/projects\/([^\/]+)$/);
  if (projectM && req.method === 'GET') {
    handleGetProject(res, projectM[1]);
    return true;
  }

  // PUT /sessions/:inviteCode/project
  const sessLinkM = p.match(/^\/sessions\/([^\/]+)\/project$/);
  if (sessLinkM && req.method === 'PUT') {
    handleLinkSession(req, res, sessLinkM[1], sessions);
    return true;
  }

  return false;
}

function onXmlUpdated(projectId, xml) {
  if (!projectId || !projects.has(projectId)) return;
  const p = projects.get(projectId);
  p.projectXml = xml;
  p.lastSyncDate = new Date().toISOString();
}

module.exports = { handleRequest, onXmlUpdated, projects };
