// ─── Room Service ─────────────────────────────────────────────────────────────

const socketMeta = require('../store');
const { log }    = require('../utils/logger');

/**
 * Retourne le socket.id du premier admin connecté dans une salle,
 * ou null s'il n'y en a aucun.
 */
function getAdminOfRoom(room) {
  for (const [id, meta] of socketMeta.entries()) {
    if (meta.isAdmin && meta.room === room) return id;
  }
  return null;
}

/**
 * Diffuse userList({ admin }) à toute la salle.
 * Appelé quand un admin rejoint ou quitte une salle.
 */
function broadcastUserList(io, room) {
  const adminId = getAdminOfRoom(room);
  io.to(room).emit('userList', { admin: adminId });
  log(`  [userList] : room=${room} admin=${adminId || 'none'}`);
}

/**
 * Retourne des statistiques sur les salles actives.
 */
function getRoomStats() {
  const stats = {};
  for (const [, meta] of socketMeta.entries()) {
    const r = meta.room || 'none';
    if (!stats[r]) stats[r] = { count: 0, admins: [] };
    stats[r].count++;
    if (meta.isAdmin) stats[r].admins.push(meta.pseudo);
  }
  return stats;
}

module.exports = { getAdminOfRoom, broadcastUserList, getRoomStats };
