// ─── Room Service ─────────────────────────────────────────────────────────────
const store = require("../store");
const { log } = require("../utils/logger");

/**
 * Retourne le socket.id du premier admin connecté dans une salle,
 * ou null s'il n'y en a aucun.
 *
 * FIX : un admin peut désormais appartenir à plusieurs salles en parallèle
 * (sa room de vente "auctavXX" + les salles auxiliaires "auctav_screen" et
 * "auctav_follow", rejointes via l'event 'joinExtra'). meta.room reste la
 * room "principale" (vente), meta.rooms est un tableau des rooms
 * additionnelles. On vérifie les deux pour retrouver l'admin, quelle que
 * soit la salle interrogée.
 */
async function getAdminOfRoom(room) {
  const entries = await store.entries();
  for (const [id, meta] of entries) {
    if (!meta.isAdmin) continue;
    if (meta.room === room) return id;
    if (Array.isArray(meta.rooms) && meta.rooms.includes(room)) return id;
  }
  return null;
}

/**
 * Diffuse userList({ admin }) à toute la salle.
 * Appelé quand un admin rejoint ou quitte une salle (room principale ou
 * salle auxiliaire via joinExtra).
 */
async function broadcastUserList(io, room) {
  if (!room) return;
  const adminId = await getAdminOfRoom(room);
  io.to(room).emit("userList", { admin: adminId });
  log(`  [userList] : room=${room} admin=${adminId || "none"}`);
}

/**
 * Retourne des statistiques sur les salles actives.
 * Prend en compte les rooms additionnelles (meta.rooms) en plus de la
 * room principale (meta.room), pour que les stats reflètent bien
 * auctav_screen / auctav_follow.
 */
async function getRoomStats() {
  const entries = await store.entries();
  const stats = {};

  const bump = (r, meta) => {
    if (!stats[r]) stats[r] = { count: 0, admins: [] };
    stats[r].count++;
    if (meta.isAdmin) stats[r].admins.push(meta.pseudo);
  };

  for (const [, meta] of entries) {
    const r = meta.room || "none";
    bump(r, meta);

    if (Array.isArray(meta.rooms)) {
      for (const extra of meta.rooms) {
        bump(extra, meta);
      }
    }
  }

  return stats;
}

module.exports = { getAdminOfRoom, broadcastUserList, getRoomStats };