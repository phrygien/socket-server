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
// Adaptation clustering (Redis store) :
//   store.get() retourne un objet désérialisé à chaque appel — le muter
//   localement (meta.pseudo = ..., meta.isAdmin = ...) n'a aucun effet sur
//   Redis. La mutation est désormais persistée explicitement via
//   store.set(socketId, meta) une fois toutes les modifications faites.
//
//   NOTE clustering (identique à evictStaleAdmin dans roomHandler.js) :
//   io.sockets.sockets.get(existingAdminId) ne trouve que les sockets
//   connectées à CETTE instance. Si l'ancien admin est sur une autre
//   instance (app2 alors qu'on traite l'event sur app1), on tombera dans la
//   branche "fantôme" et on supprimera juste son entrée store, sans fermer
//   sa vraie connexion. Cas rare avec sticky sessions, mais possible après
//   un redéploiement/restart d'instance — voir la note équivalente dans
//   roomHandler.js pour l'amélioration possible (Pub/Sub ciblé).

const store = require("../store");
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
  socket.on("admin", async (pseudo) => {
    const meta = await store.get(socket.id);

    if (meta) {
      meta.pseudo = pseudo || "Admin";
      meta.isAdmin = true;

      // Si ce socket est déjà dans une room (reconnexion rapide, ou 'admin'
      // émis après 'joinroom'), s'assurer qu'aucun autre socket n'est
      // encore marqué admin sur cette même room.
      if (meta.room) {
        const existingAdminId = await getAdminOfRoom(meta.room);
        if (existingAdminId && existingAdminId !== socket.id) {
          const oldSocket = io.sockets.sockets.get(existingAdminId);
          if (oldSocket) {
            log(
              `  [ADMIN REPLACE] (via 'admin' event) ancien admin=${existingAdminId} expulsé de room="${meta.room}" (remplacé par ${socket.id})`,
            );
            oldSocket.disconnect(true); // ferme proprement → déclenche disconnectHandler
          } else {
            log(
              `  [ADMIN REPLACE] (via 'admin' event) ancien admin=${existingAdminId} déjà fantôme (ou sur une autre instance), nettoyage direct du store`,
            );
            await store.delete(existingAdminId);
          }
        }
      }

      await store.set(socket.id, meta);
    }

    log(`  [admin]    : ${socket.id} → "${pseudo}"`);

    // Si l'admin était déjà dans une salle (reconnexion rapide), notifier
    if (meta?.room) await broadcastUserList(io, meta.room);
  });
}

module.exports = { registerAdminHandler };
