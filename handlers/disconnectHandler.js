// ─── Disconnect Handler ───────────────────────────────────────────────────────
//
// Correctif multi-room admin (FIX auctav_screen / auctav_follow) :
//   Un admin peut désormais être enregistré sur plusieurs salles : sa room
//   de vente (meta.room) + les salles auxiliaires rejointes via 'joinExtra'
//   (meta.rooms, ex: "auctav_screen", "auctav_follow" — voir roomHandler.js
//   et roomService.js). Avant ce correctif, seule meta.room était notifiée
//   à la déconnexion : screen.php / follow.php gardaient un idAdmin mort
//   en cache jusqu'à expiration naturelle du socket. On notifie désormais
//   toutes les salles connues de cet admin.

const store = require("../store");
const { log } = require("../utils/logger");
const { broadcastUserList } = require("../services/roomService");

function registerDisconnectHandler(io, socket) {
  socket.on("disconnect", async (reason) => {
    const meta = await store.get(socket.id);

    log(`- Déconnexion: ${socket.id} (${reason})`);

    // Supprimer du store EN PREMIER pour que broadcastUserList
    // ne retrouve plus cet admin dans getAdminOfRoom
    await store.delete(socket.id);

    if (meta?.room) {
      // Notifie la salle principale qu'un utilisateur est parti
      io.to(meta.room).emit("sendMsg", {
        type: "exit",
        msg: { room: meta.room, email: meta.email || "" },
        name: meta.pseudo || "unknown",
        from: socket.id,
      });
    }

    // Si c'était l'admin → toutes les salles où il était identifié comme
    // admin (room principale + rooms additionnelles auctav_screen /
    // auctav_follow) doivent être notifiées, sinon les bidders/screens/
    // followers gardent un idAdmin mort et restent bloqués.
    if (meta?.isAdmin) {
      const roomsToNotify = new Set(
          [meta.room, ...(Array.isArray(meta.rooms) ? meta.rooms : [])].filter(
              Boolean,
          ),
      );

      for (const room of roomsToNotify) {
        await broadcastUserList(io, room);
      }
    }
  });
}

module.exports = { registerDisconnectHandler };