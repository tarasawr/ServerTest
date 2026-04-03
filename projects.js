'use strict';

// --- Projects database ---
// projectId → {
//   projectId, ownerUserId, ownerName,
//   projectXml, lastSyncDate,
//   users: Map<userId, { userId, name, avatarUrl, role, isGuest }>
// }
// role: 'owner' | 'can_edit' | 'can_view'

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

function serializeUsers(usersMap) {
  const list = [];
  for (const [, u] of usersMap) {
    list.push({ userId: u.userId, name: u.name, avatarUrl: u.avatarUrl, role: u.role, isGuest: u.isGuest });
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

  // Already in project → return existing role
  if (proj.users.has(userId)) {
    const existing = proj.users.get(userId);
    log('Projects', `User ${userId} already in project ${projectId} (role: ${existing.role})`);
    return jsonOk(res, { ok: true, role: existing.role, alreadyMember: true });
  }

  const user = {
    userId,
    name: name || (isGuest ? 'Guest' : 'Unknown'),
    avatarUrl: avatarUrl || '',
    role: 'can_view',
    isGuest: !!isGuest
  };
  proj.users.set(userId, user);

  log('Projects', `User ${userId} (${user.name}) joined project ${projectId} as can_view`);
  jsonOk(res, { ok: true, role: 'can_view' });
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
    userCount: p.users.size
  });
}

function handleGetProjectUsers(res, projectId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  jsonOk(res, { users: serializeUsers(projects.get(projectId).users) });
}

function handleGetUserRole(res, projectId, userId) {
  if (!projects.has(projectId)) {
    return jsonErr(res, 404, `Project ${projectId} not found`);
  }
  const proj = projects.get(projectId);
  if (!proj.users.has(userId)) {
    return jsonErr(res, 404, `User ${userId} not in project ${projectId}`);
  }
  jsonOk(res, { userId, role: proj.users.get(userId).role });
}

function handleGetUserProjects(res, userId) {
  const result = [];
  for (const [, p] of projects) {
    if (p.users.has(userId)) {
      result.push({
        projectId: p.projectId,
        ownerName: p.ownerName,
        role: p.users.get(userId).role,
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

  const body = await readBody(req);
  const { role } = body;
  if (!['can_view', 'can_edit'].includes(role)) {
    return jsonErr(res, 400, 'role must be can_view or can_edit');
  }

  proj.users.get(userId).role = role;
  log('Projects', `Role of user ${userId} in project ${projectId} changed to ${role}`);
  jsonOk(res, { ok: true, role });
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

  // GET /projects/:id/users
  const usersM = p.match(/^\/projects\/([^\/]+)\/users$/);
  if (usersM && req.method === 'GET') {
    handleGetProjectUsers(res, usersM[1]);
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
