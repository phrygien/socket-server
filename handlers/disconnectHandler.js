// ─── Disconnect Handler ───────────────────────────────────────────────────────
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
      // Notifie la salle qu'un utilisateur est parti
      io.to(meta.room).emit("sendMsg", {
        type: "exit",
        msg: { room: meta.room, email: meta.email || "" },
        name: meta.pseudo || "unknown",
        from: socket.id,
      });

      // Si c'était l'admin → les bidders doivent cacher le formulaire d'enchère
      if (meta.isAdmin) {
        await broadcastUserList(io, meta.room);
      }
    }
  });
}

module.exports = { registerDisconnectHandler };
