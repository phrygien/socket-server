// ─── Screen Handler ───────────────────────────────────────────────────────────
// Gère la ou les salles "auctav_screen_<sale_id>" utilisées par screen.php
// (une room dédiée par vente depuis le passage au multi-écrans — voir
// screen.php pour le détail du scoping par sale_id).
//
// Flux :
//   screen.php se connecte en tant que "Screen_<timestamp>" (non-admin)
//   → joinroom('auctav_screen_<sale_id>')
//   → username(pseudo)
//   ← on('userList', { admin })
//       → si admin présent  : affiche #all + envoie getScreen à l'admin
//       → si admin absent   : cache #all
//
//   L'admin (switcher.php / switcher_list.php) répond en broadcast sur la
//   même salle via getMsgRoom :
//   ← on('sendMsg', { type: 'numLot',      msg: { numLot, nom, pere, mere,
//                                                  presentateur, infos_suppl,
//                                                  tva, from, img, prices[] } })
//   ← on('sendMsg', { type: 'previousLot', msg: { numLot, prices[] } })
//
// Côté serveur :
//   - 'getScreen'      : le Screen demande à l'admin les infos du lot en
//                         cours → relayé à toute la salle (l'admin écoute
//                         et répond)
//   - 'numLot'         : déjà géré par getMsgRoom dans roomHandler
//   - 'previousLot'    : déjà géré par getMsgRoom dans roomHandler
//   - 'joinExtraRoom'  : voir bloc dédié ci-dessous
//
// Adaptation clustering (Redis store) :
//   store.get()/entries() sont désormais asynchrones (Redis au lieu d'un
//   Map local) — tous les handlers et fonctions consommant le store
//   deviennent async. Aucune mutation de meta n'était faite ici (lecture
//   seule), donc pas de risque de perte d'état comme dans roomHandler.
//
// ─────────────────────────────────────────────────────────────────────────
// AJOUT : 'joinExtraRoom' — rejoindre la room screen SANS quitter la room
// de vente principale
//
// Contexte : getMsgRoom (roomHandler.js) exige que l'émetteur appartienne
// nativement (socket.rooms) à la room ciblée avant de pouvoir y diffuser
// (socket.rooms.has(data.room)). Or l'admin (switcher.php / switcher_list.php)
// ne rejoint que la room principale de la vente ("auctav<sale_id>") via
// 'joinroom' — jamais la room dédiée à l'écran ("auctav_screen_<sale_id>").
// Résultat : tous ses appels getMsgRoom vers cette room étaient refusés
// silencieusement côté serveur (log "REFUSÉ – ... n'appartient pas à ...").
//
// 'joinroom' (dans roomHandler.js) ne peut pas être réutilisé tel quel pour
// ça : il ne gère qu'UNE seule room métier par socket (il fait
// socket.leave(meta.room) avant de join la nouvelle) — l'utiliser ferait
// quitter l'admin de la room de vente principale, cassant tout le reste
// (enchères, messages...).
//
// 'joinExtraRoom' répond à ce besoin précis : socket.join() natif
// Socket.IO, qui ajoute une room à socket.rooms SANS retirer les rooms déjà
// rejointes, et SANS toucher meta.room dans le store (cette room "extra"
// n'a aucun rapport avec la logique métier de meta.room, uniquement avec
// l'autorisation de diffusion Socket.IO). Placé ici plutôt que dans
// roomHandler.js car c'est un besoin propre au flux "écran" — ça garde
// roomHandler.js focalisé sur la room de vente principale.
// ─────────────────────────────────────────────────────────────────────────

const store = require("../store");
const { log } = require("../utils/logger");

const SCREEN_ROOM = "auctav_screen"; // fallback historique si aucun sale_id n'est fourni dans l'URL du screen

function registerScreenHandler(io, socket) {
  /**
   * Rejoint une room SECONDAIRE (native Socket.IO), en plus de la room
   * métier principale déjà rejointe via 'joinroom'.
   *
   * Utilisé par l'admin pour pouvoir diffuser (via getMsgRoom) vers la
   * room dédiée de l'écran géant d'une vente, sans quitter la room de
   * vente principale.
   *
   * socket.emit('joinExtraRoom', 'auctav_screen_52')
   */
  socket.on("joinExtraRoom", (room) => {
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinExtraRoom] REFUSÉ room invalide – socket=${socket.id}`);
      return;
    }

    const trimmed = room.trim();
    socket.join(trimmed);
    log(
        `  [joinExtraRoom] socket=${socket.id} → room="${trimmed}" (en plus de sa room principale)`,
    );
  });

  /**
   * Le Screen demande les données du lot courant à l'admin.
   * Émis juste après réception de userList({ admin }).
   * socket.emit('getMsgPrivate', { toid: idAdmin, type: 'getScreen', name })
   * → déjà relayé par bidderHandler (getMsgPrivate).
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
 * Retourne les écrans connectés dans une salle screen donnée
 * (toutes instances confondues, grâce au store Redis partagé).
 * Utilisé par l'endpoint REST GET /screen/:room
 *
 * NOTE : room doit maintenant être passée sous la forme
 * "auctav_screen_<sale_id>" (ou "auctav_screen" pour l'ancien flux global
 * sans sale_id) — voir screen.php.
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