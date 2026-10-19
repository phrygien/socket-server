// ─── Admin Handler ────────────────────────────────────────────────────────────
// Événements émis par switcher.php
//
// SÉCURITÉ :
//   L'event 'admin' exige la clé ADMIN_KEY en 2ᵉ argument :
//       socket.emit("admin", pseudo, ADMIN_KEY, (res) => { /* res.ok */ })
//   Le 3ᵉ argument (callback) est optionnel : il reçoit { ok: true|false }.
//   Le client peut ainsi n'émettre 'joinroom' qu'APRÈS la validation de
//   l'admin, ce qui évite que 'admin' et 'joinroom' (deux lectures/écritures
//   concurrentes du même meta Redis) s'écrasent mutuellement.
//   La clé est écrite par switcher.php (page réservée aux admins connectés)
//   et vérifiée ici en temps constant. Un client qui se déclare admin depuis
//   la console sans connaître la clé est refusé, et déconnecté après
//   MAX_ADMIN_FAILS tentatives. Si ADMIN_KEY n'est pas configurée, aucun
//   admin n'est accepté (fail closed).
//
// Correctif existant :
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
const { ADMIN_KEY } = require("../config");
const safeEqual = require("../utils/safeEqual");
const { log } = require("../utils/logger");
const {
  broadcastUserList,
  getAdminOfRoom,
} = require("../services/roomService");

const MAX_ADMIN_FAILS = 3;

function registerAdminHandler(io, socket) {
  /**
   * Identification de l'admin — émis avant joinroom.
   * socket.emit('admin', pseudo)
   */
  socket.on("admin", async (pseudo, key, ack) => {
    const done = (ok) => {
      if (typeof ack === "function") ack({ ok });
    };

    // ── Contrôle d'autorisation : clé admin obligatoire ──
    if (!ADMIN_KEY) {
      log(`  [admin]    : REFUSÉ – ADMIN_KEY non configurée (${socket.id})`);
      done(false);
      return;
    }

    if (typeof key !== "string" || !safeEqual(key, ADMIN_KEY)) {
      socket.data.adminFails = (socket.data.adminFails || 0) + 1;
      log(
          `  [admin]    : REFUSÉ – clé invalide (${socket.data.adminFails}/${MAX_ADMIN_FAILS}) socket=${socket.id}`,
      );
      done(false);
      if (socket.data.adminFails >= MAX_ADMIN_FAILS) socket.disconnect(true);
      return;
    }

    const cleanPseudo =
        typeof pseudo === "string" && pseudo.trim()
            ? pseudo.trim().slice(0, 50)
            : "Admin";

    const meta = await store.get(socket.id);

    if (meta) {
      meta.pseudo = cleanPseudo;
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

    log(`  [admin]    : ${socket.id} → "${cleanPseudo}"`);

    // Si l'admin était déjà dans une salle (reconnexion rapide), notifier
    if (meta?.room) await broadcastUserList(io, meta.room);

    done(true);
  });
}

module.exports = { registerAdminHandler };