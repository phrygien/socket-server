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
// État en cache (nouveau) :
//   Avant ce changement, un screen qui se connectait affichait "-" partout
//   (valeurs par défaut du HTML) jusqu'à ce que l'admin réponde à son
//   'getScreen' — délai visible, voire écran vide si l'admin est lent/absent.
//
//   Contrainte : impossible de modifier roomHandler.js. Or c'est LUI qui
//   traite le 'getMsgRoom' émis par admin.php (maj_screen()) et broadcast
//   le 'sendMsg'/'numLot' résultant — ce broadcast est une émission serveur
//   → client, donc invisible à un simple socket.on() ailleurs... SAUF que
//   Socket.IO autorise plusieurs listeners indépendants sur le MÊME event
//   ('getMsgRoom') d'un même socket. roomHandler.js et ce fichier sont tous
//   deux appelés à la connexion (registerRoomHandler + registerScreenHandler),
//   donc les deux listeners 'getMsgRoom' coexistent et s'exécutent chacun de
//   leur côté à chaque emit du client, sans interférence.
//
//   On ajoute donc ICI un second listener 'getMsgRoom', purement passif :
//   il observe (sans jamais bloquer ni rien réémettre côté room) les
//   messages de type 'numLot' et les met en cache Redis (`lotstate:{room}`).
//   Une validation minimale (appartenance à la room) est dupliquée ici pour
//   éviter de cacher un message qui serait de toute façon rejeté par
//   roomHandler (socket hors room) — mais sans dupliquer le contrôle
//   ADMIN_ONLY_TYPES : si un jour un non-admin réussit à émettre un faux
//   'numLot' via getMsgRoom, roomHandler.js le bloque déjà avant broadcast,
//   ce cache-ci resterait alors légèrement désynchronisé du "vrai" flux
//   affiché aux autres clients — impact mineur (un screen affiche un état
//   qui n'a jamais été diffusé aux autres), à surveiller si ça arrive.
//
//   Dès qu'un 'getScreen' est reçu (connexion du screen, ou polling
//   périodique serveur), on répond IMMÉDIATEMENT en privé à l'expéditeur
//   avec cet état caché, en plus du broadcast classique qui sollicite
//   l'admin pour une éventuelle mise à jour plus fraîche.
//
//   screen.php n'a besoin d'AUCUNE modification : il reçoit ce payload
//   caché via le même canal 'sendMsg'/type:'numLot' que d'habitude.
//
// Adaptation clustering (Redis store) :
//   store.get()/entries()/set() sont asynchrones (Redis). Tous les
//   handlers concernés sont async.

const store = require("../store");
const { log } = require("../utils/logger");

const POLL_INTERVAL_MS = 3000;
const LOTSTATE_PREFIX = "lotstate:";
const LOTSTATE_TTL_SECONDS = 6 * 60 * 60; // 6h, large marge pour couvrir une vente

function lotStateKey(room) {
  return `${LOTSTATE_PREFIX}${room}`;
}

/**
 * Enregistre en cache le dernier état de lot connu pour une room.
 */
async function cacheLotState(room, msg) {
  if (!room || !msg) return;
  try {
    await store.set(lotStateKey(room), JSON.stringify(msg), LOTSTATE_TTL_SECONDS);
  } catch (err) {
    log(`  [cacheLotState] error: ${err.message}`);
  }
}

/**
 * Récupère l'état de lot en cache pour une room, ou null si absent/expiré.
 */
async function getCachedLotState(room) {
  try {
    const raw = await store.get(lotStateKey(room));
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    return null;
  }
}

function registerScreenHandler(io, socket) {
  /**
   * Le Screen demande les données du lot courant.
   * Émis juste après réception de userList({ admin }), ou par le
   * polling serveur périodique.
   *
   * Comportement :
   *   1. Si un état est en cache pour cette room, on le renvoie
   *      IMMÉDIATEMENT et en privé au socket demandeur (le screen).
   *   2. On broadcast quand même 'getScreen' à toute la room, pour
   *      que l'admin puisse répondre avec un état plus à jour.
   */
  socket.on("getScreen", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [getScreen]: ${socket.id} "${meta?.pseudo}" → ${room}`);

    const cached = await getCachedLotState(room);
    if (cached) {
      log(`  [getScreen]: réponse immédiate depuis le cache → ${socket.id}`);
      socket.emit("sendMsg", {
        type: "numLot",
        msg: cached,
        name: "System",
        from: null,
      });
    }

    io.to(room).emit("sendMsg", {
      type: "getScreen",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Mise à jour du lot affiché à l'écran.
   * Émis directement par un client non-admin (ex: régie/switcher) sans
   * passer par getMsgRoom. On met à jour le cache avant de diffuser.
   */
  socket.on("numLot", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [numLot]   : ${socket.id} lot=${data?.numLot} → ${room}`);

    await cacheLotState(room, data);

    io.to(room).emit("sendMsg", {
      type: "numLot",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Affichage du lot précédent (prix adjugé).
   * Ne remplace pas le cache 'numLot' (le lot précédent n'est pas le
   * lot courant), simple complément au flux getMsgRoom de roomHandler.
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

  /**
   * Listener PASSIF sur 'getMsgRoom' — coexiste avec celui de
   * roomHandler.js sans le modifier ni le remplacer (Socket.IO exécute
   * tous les listeners enregistrés pour un même event). But unique :
   * capter les 'numLot' que l'admin envoie via getMsgRoom (le chemin
   * "normal" documenté dans admin.php/maj_screen()) pour les mettre en
   * cache, exactement comme le fait le listener 'numLot' direct ci-dessus
   * pour le switcher.
   *
   * Ne réémet RIEN, ne bloque RIEN : lecture seule, effet de bord = cache
   * uniquement. roomHandler.js garde l'entière responsabilité de la
   * validation et du broadcast réel du message.
   */
  socket.on("getMsgRoom", async (data) => {
    if (!data || data.type !== "numLot") return;
    if (typeof data.room !== "string" || !data.room.trim()) return;

    // Validation minimale dupliquée (appartenance à la room) pour éviter
    // de cacher un message qui sera de toute façon rejeté par roomHandler.
    if (!socket.rooms.has(data.room)) return;

    log(`  [getMsgRoom→cache] numLot lot=${data.msg?.numLot} room=${data.room}`);
    await cacheLotState(data.room, data.msg || {});
  });
}

/**
 * Retourne les écrans connectés dans une room de vente donnée
 * ("auctav{sale_id}"), toutes instances confondues, grâce au store
 * Redis partagé.
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

/**
 * Relance périodique server-side : pour chaque room de vente
 * ("auctav{sale_id}") contenant au moins un écran ("Screen_*") connecté,
 * diffuse un 'getScreen' à toute la room, toutes les POLL_INTERVAL_MS.
 *
 * Combiné au cache lotstate, ça garantit que même un screen qui vient
 * de rejoindre la room affichera l'état courant sans délai perceptible,
 * dès son premier 'getScreen' (déclenché juste après userList côté
 * client screen.php).
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
  cacheLotState,
  getCachedLotState,
};