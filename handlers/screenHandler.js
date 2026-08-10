// ─── Screen Handler ───────────────────────────────────────────────────────────
// Gère la salle "auctav_screen" utilisée par screen.php
//
// Flux :
//   screen.php se connecte en tant que "Screen_<timestamp>" (non-admin)
//   → joinroom('auctav_screen')
//   → username(pseudo)
//   ← on('userList', { admin })
//       → si admin présent  : affiche #all + envoie getScreen à l'admin
//       → si admin absent   : cache #all
//
//   L'admin répond en broadcast sur la salle :
//   ← on('sendMsg', { type: 'numLot',      msg: { numLot, nom, pere, mere,
//                                                  presentateur, infos_suppl,
//                                                  tva, from, img, prices[] } })
//   ← on('sendMsg', { type: 'previousLot', msg: { numLot, prices[] } })
//
// Côté serveur :
//   - 'getScreen'   : le Screen demande à l'admin les infos du lot en cours
//                     → relayé à toute la salle (l'admin écoute et répond)
//   - 'numLot'      : déjà géré par getMsgRoom dans roomHandler
//   - 'previousLot' : déjà géré par getMsgRoom dans roomHandler
//
// Adaptation clustering (Redis store) :
//   store.get()/entries() sont désormais asynchrones (Redis au lieu d'un
//   Map local) — tous les handlers et fonctions consommant le store
//   deviennent async. Aucune mutation de meta n'était faite ici (lecture
//   seule), donc pas de risque de perte d'état comme dans roomHandler.

const store = require("../store");
const { log } = require("../utils/logger");

const SCREEN_ROOM = "auctav_screen";

function registerScreenHandler(io, socket) {
  /**
   * Le Screen demande les données du lot courant à l'admin.
   * Émis juste après réception de userList({ admin }).
   * socket.emit('getMsgPrivate', { toid: idAdmin, type: 'getScreen', name })
   * → déjà relayé par messageHandler.
   *
   * Ici on gère le cas où 'getScreen' est émis en broadcast à la salle.
   */
  socket.on("getScreen", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [getScreen]: ${socket.id} "${meta?.pseudo}" → ${room}`);

    io.to(room).emit("sendMsg", {
      type: "getScreen",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Mise à jour du lot affiché à l'écran.
   * Émis par l'admin via getMsgRoom({ type: 'numLot', … })
   * → déjà géré par roomHandler.
   *
   * Ce handler permet à un client non-admin (ex: régie) d'émettre
   * directement un 'numLot' sans passer par getMsgRoom.
   */
  socket.on("numLot", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [numLot]   : ${socket.id} lot=${data?.numLot} → ${room}`);

    io.to(room).emit("sendMsg", {
      type: "numLot",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Affichage du lot précédent (prix adjugé).
   * Idem : complément au flux getMsgRoom de roomHandler.
   */
  socket.on("previousLot", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [prevLot]  : ${socket.id} lot=${data?.numLot} → ${room}`);

    io.to(room).emit("sendMsg", {
      type: "previousLot",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });
}

/**
 * Retourne les écrans connectés dans la salle screen
 * (toutes instances confondues, grâce au store Redis partagé).
 * Utilisé par l'endpoint REST GET /screen/:room
 */
async function getScreensInRoom(room) {
  const entries = await store.entries();
  const screens = [];

  for (const [id, meta] of entries) {
    if (meta.room === room && !meta.isAdmin) {
      screens.push({
        id,
        pseudo: meta.pseudo,
        isScreen: meta.pseudo?.startsWith("Screen_"),
      });
    }
  }

  return screens;
}

module.exports = { registerScreenHandler, getScreensInRoom, SCREEN_ROOM };
