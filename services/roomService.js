// ─── Room Service ─────────────────────────────────────────────────────────────
const store = require("../store");
const { log } = require("../utils/logger");

/**
 * Retourne le socket.id du premier admin connecté dans une salle,
 * ou null s'il n'y en a aucun.
 */
async function getAdminOfRoom(room) {
  const entries = await store.entries();
  for (const [id, meta] of entries) {
    if (meta.isAdmin && meta.room === room) return id;
  }
  return null;
}

/**
 * Diffuse userList({ admin }) à toute la salle.
 * Appelé quand un admin rejoint ou quitte une salle.
 */
async function broadcastUserList(io, room) {
  const adminId = await getAdminOfRoom(room);
  io.to(room).emit("userList", { admin: adminId });
  log(`  [userList] : room=${room} admin=${adminId || "none"}`);
}

/**
 * Retourne des statistiques sur les salles actives.
 */
async function getRoomStats() {
  const entries = await store.entries();
  const stats = {};
  for (const [, meta] of entries) {
    const r = meta.room || "none";
    if (!stats[r]) stats[r] = { count: 0, admins: [] };
    stats[r].count++;
    if (meta.isAdmin) stats[r].admins.push(meta.pseudo);
  }
  return stats;
}

module.exports = { getAdminOfRoom, broadcastUserList, getRoomStats };
