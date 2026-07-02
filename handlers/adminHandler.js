// ─── Admin Handler ────────────────────────────────────────────────────────────
// Événements émis par switcher.php
//
// Correctif ajouté :
//   Si l'event 'admin' arrive alors que le socket est DÉJÀ dans une room
//   (ex: reconnexion qui rejoue 'joinroom' puis 'admin' dans un ordre
//   différent, ou ré-émission de 'admin' sans nouveau 'joinroom'), on
//   vérifie ici aussi qu'aucun autre socket n'est encore enregistré comme
//   admin de cette room. Si c'est le cas, l'ancien est expulsé immédiatement
//   au lieu d'attendre le pingTimeout. C'est une sécurité complémentaire à
//   celle déjà présente dans roomHandler.js (event 'joinroom'), qui couvre
//   le cas le plus fréquent (admin → joinroom).
//
const socketMeta = require("../store");
const { log } = require("../utils/logger");
const {
  broadcastUserList,
  getAdminOfRoom,
} = require("../services/roomService");

function registerAdminHandler(io, socket) {
  /**
   * Identification de l'admin — émis avant joinroom.
   * socket.emit('admin', pseudo)
   */
  socket.on("admin", (pseudo) => {
    const meta = socketMeta.get(socket.id);

    if (meta) {
      meta.pseudo = pseudo || "Admin";
      meta.isAdmin = true;

      // Si ce socket est déjà dans une room (reconnexion rapide, ou 'admin'
      // émis après 'joinroom'), s'assurer qu'aucun autre socket n'est
      // encore marqué admin sur cette même room.
      if (meta.room) {
        const existingAdminId = getAdminOfRoom(meta.room);

        if (existingAdminId && existingAdminId !== socket.id) {
          const oldSocket = io.sockets.sockets.get(existingAdminId);

          if (oldSocket) {
            log(
              `  [ADMIN REPLACE] (via 'admin' event) ancien admin=${existingAdminId} expulsé de room="${meta.room}" (remplacé par ${socket.id})`,
            );
            oldSocket.disconnect(true); // ferme proprement → déclenche disconnectHandler
          } else {
            log(
              `  [ADMIN REPLACE] (via 'admin' event) ancien admin=${existingAdminId} déjà fantôme, nettoyage direct du store`,
            );
            socketMeta.delete(existingAdminId);
          }
        }
      }
    }

    log(`  [admin]    : ${socket.id} → "${pseudo}"`);

    // Si l'admin était déjà dans une salle (reconnexion rapide), notifier
    if (meta?.room) broadcastUserList(io, meta.room);
  });
}

module.exports = { registerAdminHandler };
