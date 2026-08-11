// ─── Screen Handler ───────────────────────────────────────────────────────────
// Gère l'affichage écran (screen.php) au sein de la MÊME room que vente_list.php
// et admin.php : "auctav{sale_id}". Il n'existe pas de room dédiée "auctav_screen" —
// screen.php rejoint la room de la vente comme n'importe quel autre client
// (viewer, admin, switcher), ce qui permet à l'admin/switcher de diffuser
// 'numLot'/'previousLot' à tout le monde en un seul emit (getMsgRoom, déjà
// géré par roomHandler).
//
// Flux :
//   screen.php se connecte en tant que "Screen_<timestamp>" (non-admin)
//   → joinroom('auctav{sale_id}')   // ← même room que vente_list.php
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
//                     → relayé à toute la room de la vente (l'admin écoute et répond)
//   - 'numLot'      : déjà géré par getMsgRoom dans roomHandler
//   - 'previousLot' : déjà géré par getMsgRoom dans roomHandler
//
// Adaptation clustering (Redis store) :
//   store.get()/entries() sont désormais asynchrones (Redis au lieu d'un
//   Map local) — tous les handlers et fonctions consommant le store
//   deviennent async. Aucune mutation de meta n'était faite ici (lecture
//   seule), donc pas de risque de perte d'état comme dans roomHandler.
//
// Polling serveur (relance périodique) :
//   screen.php ne connaît jamais l'id socket du switcher (screenHandler.js
//   côté régie), seulement idAdmin. Donc si le switcher est la seule source
//   de vérité pour un lot (cas "numLot direct" documenté dans screen.php),
//   un event raté par le screen ne peut jamais être rattrapé par un simple
//   ciblage privé depuis le client.
//
//   Pour combler ce trou sans modifier screen.php, le serveur diffuse
//   périodiquement un 'getScreen' dans chaque room de vente ("auctav{id}")
//   qui contient au moins un écran connecté ("Screen_*"). L'admin ET le
//   switcher, qui écoutent déjà (ou doivent écouter) sendMsg.type ===
//   'getScreen', répondent alors avec l'état courant du lot — exactement
//   le même mécanisme que 'reconnection' entre vente_list.php et
//   admin.php, mais poussé par le serveur plutôt que par le client.
//
//   ⚠️ Puisque la room "auctav{id}" est partagée avec tous les viewers de
//   vente_list.php, ce broadcast 'getScreen' leur parvient aussi. C'est
//   sans effet côté vente_list.php (aucun handler n'écoute ce type), mais
//   à garder en tête si on ajoute un jour un listener générique sur
//   sendMsg côté viewer.

const store = require("../store");
const { log } = require("../utils/logger");

const POLL_INTERVAL_MS = 3000;

function registerScreenHandler(io, socket) {
  /**
   * Le Screen demande les données du lot courant à l'admin.
   * Émis juste après réception de userList({ admin }).
   * socket.emit('getMsgPrivate', { toid: idAdmin, type: 'getScreen', name })
   * → déjà relayé par messageHandler.
   *
   * Ici on gère le cas où 'getScreen' est émis en broadcast à la room
   * de la vente (auctav{sale_id}), et non plus à une room dédiée.
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
   * Ce handler permet à un client non-admin (ex: régie/switcher) d'émettre
   * directement un 'numLot' sans passer par getMsgRoom, toujours dans la
   * room de vente courante (auctav{sale_id}).
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
 * Retourne les écrans connectés dans une room de vente donnée
 * ("auctav{sale_id}"), toutes instances confondues, grâce au store
 * Redis partagé.
 * Utilisé par l'endpoint REST GET /screen/:room
 *
 * @param {string} room - ex: "auctav123", la même room que vente_list.php
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

/**
 * Relance périodique server-side : pour chaque room de vente
 * ("auctav{sale_id}") contenant au moins un écran ("Screen_*") connecté,
 * diffuse un 'getScreen' à toute la room (admin + switcher + viewers
 * inclus), toutes les POLL_INTERVAL_MS.
 *
 * Reproduit pour le screen ce que fait 'reconnection' entre
 * vente_list.php et admin.php, mais côté serveur, sans dépendre de
 * screen.php qui ne connaît jamais l'id du switcher.
 *
 * Le switcher (régie) doit écouter sendMsg.type === 'getScreen' et
 * répondre par un numLot/getMsgRoom classique — comme admin.php le
 * fait déjà via son handler `if(data.type=="getScreen"){ maj_screen(); }`.
 *
 * À démarrer une seule fois au boot du serveur (pas par connexion
 * socket), pour éviter la multiplication d'intervals.
 */
function startScreenPolling(io) {
  setInterval(async () => {
    try {
      const entries = await store.entries();
      const roomsWithScreen = new Set();

      for (const [, meta] of entries) {
        if (meta?.room && meta?.pseudo?.startsWith("Screen_")) {
          roomsWithScreen.add(meta.room);
        }
      }

      for (const room of roomsWithScreen) {
        log(`  [pollScreen]: broadcast getScreen → ${room}`);
        io.to(room).emit("sendMsg", {
          type: "getScreen",
          msg: {},
          name: "System",
          from: null,
        });
      }
    } catch (err) {
      log(`  [pollScreen] error: ${err.message}`);
    }
  }, POLL_INTERVAL_MS);
}

module.exports = {
  registerScreenHandler,
  getScreensInRoom,
  startScreenPolling,
};