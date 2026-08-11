// ─── Screen Handler ───────────────────────────────────────────────────────────
// Gère l'affichage écran (screen.php) au sein de la MÊME room que vente_list.php
// et admin.php : "auctav{sale_id}". Il n'existe pas de room dédiée "auctav_screen" —
// screen.php rejoint la room de la vente comme n'importe quel autre client
// (viewer, admin, switcher), ce qui permet à l'admin/switcher de diffuser
// 'numLot'/'previousLot' à tout le monde en un seul emit (getMsgRoom, déjà
// géré par roomHandler).
//
// Flux réel de screen.php (inchangé) :
//   connect → joinroom(room) → username(pseudo)
//   ← userList({admin}) → si admin présent, screen.php appelle
//     requestScreenInfos() qui émet :
//       socket.emit("getMsgPrivate", {toid: idAdmin, type: 'getScreen', name})
//   Ce 'getMsgPrivate' est relayé PRIVÉMENT à l'admin par messageHandler.js
//   (non modifié ici). L'admin répond alors via maj_screen() → getMsgRoom
//   'numLot' (broadcast room, géré par roomHandler.js, non modifié).
//   screen.php répète requestScreenInfos() toutes les 3s (polling client déjà
//   en place dans screen.php).
//
// Problème réglé ici : sur un refresh de screen.php, l'écran reste vide
// (valeurs par défaut "-") tant que l'admin n'a pas répondu à la demande
// privée — délai visible, voire écran vide si l'admin est lent/absent.
//
// Solution — sans toucher à screen.php, messageHandler.js ni roomHandler.js :
//   Socket.IO exécute TOUS les listeners enregistrés pour un même event.
//   On ajoute donc ici un second listener 'getMsgPrivate', purement passif
//   pour tout type autre que 'getScreen' (il ne fait rien et laisse
//   messageHandler.js gérer normalement), et pour 'getScreen' il répond
//   IMMÉDIATEMENT et en privé au socket demandeur avec le dernier état
//   connu en cache (Redis, clé `lotstate:{room}`), sans bloquer ni modifier
//   le relais habituel vers l'admin fait par messageHandler.js — qui
//   continue en parallèle et peut renvoyer un état plus frais une fois
//   l'admin réveillé/synchro.
//
//   Le cache est alimenté par deux sources, toutes deux déjà en place :
//     - 'numLot' direct (switcher) → écouté ici directement
//     - 'numLot' via getMsgRoom (admin/maj_screen) → écouté ici en
//       parallèle du listener de roomHandler.js, en lecture seule
//
//   screen.php n'a besoin d'AUCUNE modification : il reçoit ce payload
//   caché via le même canal 'sendMsg'/type:'numLot' que d'habitude, donc
//   son handler afficheLot() existant l'affiche normalement, dès la
//   connexion/refresh, sans attendre l'admin.
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
   * Listener PASSIF sur 'getMsgPrivate' — coexiste avec celui de
   * messageHandler.js (non modifié), sans jamais toucher au relais
   * privé vers l'admin qu'il effectue déjà.
   *
   * Ne s'active que pour type === 'getScreen' (la demande que screen.php
   * émet à chaque connexion/refresh et toutes les 3s). Dès réception,
   * répond IMMÉDIATEMENT en privé (socket.emit, jamais de broadcast room)
   * à l'expéditeur avec le dernier état connu en cache, s'il existe.
   *
   * C'est ce qui règle le refresh : screen.php n'a rien à changer, il
   * reçoit ce payload via son handler `sendMsg`/`type:"numLot"` existant,
   * avant même que l'admin n'ait eu le temps de répondre à la demande
   * privée relayée en parallèle par messageHandler.js.
   */
  socket.on("getMsgPrivate", async (data) => {
    if (!data || data.type !== "getScreen") return;

    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    const cached = await getCachedLotState(room);
    if (!cached) return;

    log(`  [getMsgPrivate→cache hit] getScreen ${socket.id} → réponse immédiate (room=${room})`);

    socket.emit("sendMsg", {
      type: "numLot",
      msg: cached,
      name: "System",
      from: null,
    });
  });

  /**
   * Le Screen (ou tout autre client) peut aussi demander l'état via un
   * 'getScreen' broadcast direct (utilisé par le polling serveur
   * ci-dessous). Même logique : réponse immédiate depuis le cache si
   * disponible, puis broadcast classique pour solliciter une réponse
   * plus fraîche de l'admin/switcher.
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
   * roomHandler.js sans le modifier ni le remplacer. But unique :
   * capter les 'numLot' que l'admin envoie via getMsgRoom (le chemin
   * "normal" documenté dans admin.php/maj_screen()) pour les mettre en
   * cache, exactement comme le fait le listener 'numLot' direct pour
   * le switcher.
   *
   * Ne réémet RIEN, ne bloque RIEN : lecture seule, effet de bord =
   * cache uniquement. roomHandler.js garde l'entière responsabilité de
   * la validation et du broadcast réel du message.
   */
  socket.on("getMsgRoom", async (data) => {
    if (!data || data.type !== "numLot") return;
    if (typeof data.room !== "string" || !data.room.trim()) return;

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
 * Complémentaire au flux normal de screen.php (qui interroge en privé
 * via getMsgPrivate toutes les 3s) — filet de sécurité supplémentaire.
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