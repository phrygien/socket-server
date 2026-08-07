// ─── Room Handler ─────────────────────────────────────────────────────────────
//
//  Optimisations latence :
//  1. Écriture historique asynchrone + buffer (ne bloque plus la event loop)
//  2. socketMeta accès optimisé (une seule récupération par event)
//  3. Payload nettoyé en amont
//
//  Sécurité conservée :
//  1. Validation des entrées (room, type)
//  2. Le socket doit appartenir à data.room (anti-room-forgery)
//  3. Liste blanche des types autorisés (ALLOWED_TYPES)
//  4. Types sensibles réservés à l'admin (ADMIN_ONLY_TYPES)
//
//  Correctif ajouté :
//  5. Anti double-admin : quand un socket admin rejoint une room qui a
//     déjà un admin actif (ancien socket pas encore timeout après une
//     déconnexion/reconnexion), l'ancien socket est expulsé immédiatement
//     au lieu d'attendre le pingTimeout (20s). Évite que deux sockets
//     admin émettent/reçoivent en parallèle sur la même room.
//
//  Correctif clustering (Redis store) :
//  6. store.get() retourne un objet désérialisé à chaque appel — le muter
//     localement (meta.room = ...) n'a aucun effet sur Redis. Chaque
//     changement d'état est désormais persisté explicitement via
//     store.set(socketId, meta) après modification.
// ─────────────────────────────────────────────────────────────────────────────

const store = require("../store");
const { log } = require("../utils/logger");
const {
  getAdminOfRoom,
  broadcastUserList,
} = require("../services/roomService");
const fs = require("fs");
const path = require("path");

// ── Chemin vers l'historique ──────────────────────────────────────────────────
const HISTORIQUE_PATH = path.resolve(__dirname, "../historique.json");

// ── Listes de contrôle ────────────────────────────────────────────────────────

const ALLOWED_TYPES = new Set([
  "listLot",
  "numLot",
  "previousLot",
  "message",
  "users",
  "closeEnchere",
  "updateLot",
]);

const ADMIN_ONLY_TYPES = new Set([
  "listLot",
  "numLot",
  "previousLot",
  "closeEnchere",
  "updateLot",
]);

// ─────────────────────────────────────────────────────────────────────────────
// HISTORIQUE — ÉCRITURE ASYNCHRONE BUFFERISÉE
//
// Problème précédent : fs.readFileSync + fs.writeFileSync dans chaque event
// bloquaient la boucle événementielle Node.js → latence visible pendant la vente.
//
// Solution : buffer en mémoire (tableau) + flush async toutes les 5 secondes.
// Zéro impact sur le hot path Socket.IO.
//
// NOTE clustering : ce buffer reste LOCAL à chaque instance. Chaque instance
// écrit dans son propre historique.json — si tu veux un historique unifié
// entre app1..app4, il faudra soit centraliser sur un volume partagé, soit
// écrire dans Redis/une base au lieu du disque local. Pour l'instant chaque
// instance produit son propre fichier, ce qui est correct si HISTORIQUE_PATH
// pointe vers un volume monté séparément par instance, mais À VÉRIFIER si tu
// veux un historique global consolidé.
// ─────────────────────────────────────────────────────────────────────────────

let historiqueBuffer = [];
let flushScheduled = false;

/**
 * Ajoute une entrée dans le buffer — ne touche pas le disque.
 * @param {object} entry
 */
function appendHistorique(entry) {
  historiqueBuffer.push(entry);
  scheduleFlush();
}

/**
 * Planifie un flush différé si pas déjà prévu.
 */
function scheduleFlush() {
  if (flushScheduled) return;
  flushScheduled = true;
  setTimeout(flushHistorique, 5000);
}

/**
 * Écrit le buffer sur disque de façon asynchrone (fs.appendFile).
 * Utilise appendFile ligne par ligne (NDJSON) — pas de lecture/parse du fichier entier.
 */
function flushHistorique() {
  flushScheduled = false;

  if (historiqueBuffer.length === 0) return;

  const toWrite = historiqueBuffer.splice(0); // vide le buffer atomiquement
  const lines = toWrite.map((e) => JSON.stringify(e)).join("\n") + "\n";

  fs.appendFile(HISTORIQUE_PATH, lines, "utf8", (err) => {
    if (err) log(`[historique] ERREUR flush : ${err.message}`);
  });
}

/**
 * Flush final au shutdown propre — évite de perdre les dernières entrées.
 */
function flushHistoriqueSync() {
  if (historiqueBuffer.length === 0) return;
  const toWrite = historiqueBuffer.splice(0);
  const lines = toWrite.map((e) => JSON.stringify(e)).join("\n") + "\n";
  try {
    fs.appendFileSync(HISTORIQUE_PATH, lines, "utf8");
  } catch (err) {
    log(`[historique] ERREUR flush sync : ${err.message}`);
  }
}

// Branché sur SIGTERM/SIGINT dans server.js — appeler flushHistoriqueSync() avant process.exit
process.on("SIGTERM", flushHistoriqueSync);
process.on("SIGINT", flushHistoriqueSync);

/**
 * Met à jour le pseudo dans le buffer en mémoire (pas de I/O).
 * @param {string} socketId
 * @param {string} pseudo
 */
function updateBufferPseudo(socketId, pseudo) {
  for (let i = historiqueBuffer.length - 1; i >= 0; i--) {
    if (
        historiqueBuffer[i].socketId === socketId &&
        historiqueBuffer[i].event === "joinroom"
    ) {
      historiqueBuffer[i].pseudo = pseudo;
      break;
    }
  }
}

// ── Utilitaire IP ─────────────────────────────────────────────────────────────

function getClientIp(socket) {
  const forwarded = socket.handshake.headers["x-forwarded-for"];
  if (forwarded) return forwarded.split(",")[0].trim();
  return socket.handshake.address || "inconnue";
}

// ─────────────────────────────────────────────────────────────────────────────
// ANTI DOUBLE-ADMIN
//
// Quand un socket admin rejoint une room, vérifie si un autre socket est
// déjà enregistré comme admin de cette room. Si oui, on le considère comme
// obsolète (ancienne connexion suite à reconnexion/changement réseau) et on
// le déconnecte immédiatement, ce qui déclenche son propre 'disconnect'
// (→ disconnectHandler fait le ménage : store.delete + broadcastUserList).
//
// Si le socket n'existe plus côté transport (fantôme déjà fermé mais pas
// encore nettoyé du store), on supprime directement son entrée dans le store.
//
// NOTE clustering : io.sockets.sockets.get(existingAdminId) ne trouve QUE les
// sockets connectées à CETTE instance. Si l'ancien admin est connecté sur une
// autre instance (app2 alors qu'on traite l'event sur app1), oldSocket sera
// undefined ici même s'il est bien vivant ailleurs — on tombera alors dans la
// branche "fantôme" et on supprimera son entrée store à tort, sans fermer sa
// vraie connexion. Pour un nettoyage cross-instance fiable, il faudrait
// émettre un event Redis Pub/Sub ciblé (ex: via l'adapter Socket.IO) demandant
// à l'instance propriétaire de déconnecter ce socket. À considérer si les
// admins changent fréquemment d'instance (peu probable avec sticky sessions,
// mais possible après un redéploiement/restart d'instance).
// ─────────────────────────────────────────────────────────────────────────────

async function evictStaleAdmin(io, room, incomingSocketId) {
  const existingAdminId = await getAdminOfRoom(room);
  if (!existingAdminId || existingAdminId === incomingSocketId) return;

  const oldSocket = io.sockets.sockets.get(existingAdminId);

  if (oldSocket) {
    log(
        `  [ADMIN REPLACE] ancien admin=${existingAdminId} expulsé de room="${room}" (remplacé par ${incomingSocketId})`,
    );
    oldSocket.disconnect(true); // ferme proprement → déclenche disconnectHandler
  } else {
    log(
        `  [ADMIN REPLACE] ancien admin=${existingAdminId} déjà fantôme (ou sur une autre instance), nettoyage direct du store`,
    );
    await store.delete(existingAdminId);
  }
}

// ─────────────────────────────────────────────────────────────────────────────

function registerRoomHandler(io, socket) {
  /**
   * Rejoindre une salle.
   */
  socket.on("joinroom", async (room) => {
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinroom] REFUSÉ room invalide – socket=${socket.id}`);
      return;
    }

    const meta = await store.get(socket.id);
    const clientIp = getClientIp(socket);

    // Quitter l'ancienne salle
    if (meta?.room) {
      socket.leave(meta.room);
      if (meta.isAdmin) await broadcastUserList(io, meta.room);
    }

    // ── Anti double-admin : expulser tout admin précédent sur cette room ──
    if (meta?.isAdmin) {
      await evictStaleAdmin(io, room, socket.id);
    }

    socket.join(room);

    // Persister le changement de room dans le store partagé
    if (meta) {
      meta.room = room;
      await store.set(socket.id, meta);
    }

    log(
        `  [joinroom] socket=${socket.id} → room="${room}" ip=${clientIp} admin=${meta?.isAdmin}`,
    );

    // Historique — asynchrone, ne bloque pas
    appendHistorique({
      event: "joinroom",
      socketId: socket.id,
      ip: clientIp,
      room,
      pseudo: meta?.pseudo || "inconnu",
      isAdmin: meta?.isAdmin ?? false,
      timestamp: new Date().toISOString(),
    });

    if (meta?.isAdmin) {
      await broadcastUserList(io, room);
    } else {
      const adminId = await getAdminOfRoom(room);
      socket.emit("userList", { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || "none"}`);
    }
  });

  // ───────────────────────────────────────────────────────────────────────────

  socket.on("username", async (pseudo) => {
    if (typeof pseudo !== "string" || !pseudo.trim()) return;

    const trimmed = pseudo.trim();
    const meta = await store.get(socket.id);

    if (meta) {
      meta.pseudo = trimmed;
      await store.set(socket.id, meta);
    }

    log(`  [username] socket=${socket.id} pseudo="${trimmed}"`);

    // Mise à jour en mémoire uniquement — zéro I/O
    updateBufferPseudo(socket.id, trimmed);
  });

  // ───────────────────────────────────────────────────────────────────────────

  socket.on("getMsgRoom", async (data) => {
    // Contrôle 1 : champs obligatoires
    if (!data || typeof data.room !== "string" || !data.room.trim()) {
      log(
          `  [getMsgRoom] REFUSÉ champ room absent/invalide – socket=${socket.id}`,
      );
      return;
    }
    if (typeof data.type !== "string" || !data.type.trim()) {
      log(
          `  [getMsgRoom] REFUSÉ champ type absent/invalide – socket=${socket.id}`,
      );
      return;
    }

    // Contrôle 2 : appartenance à la salle
    if (!socket.rooms.has(data.room)) {
      log(
          `  [getMsgRoom] REFUSÉ – ${socket.id} n'appartient pas à "${data.room}"`,
      );
      return;
    }

    // Contrôle 3 : type dans la liste blanche
    if (!ALLOWED_TYPES.has(data.type)) {
      log(
          `  [getMsgRoom] REFUSÉ – type non autorisé "${data.type}" depuis ${socket.id}`,
      );
      return;
    }

    // Contrôle 4 : types admin uniquement
    const meta = await store.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(
          `  [getMsgRoom] REFUSÉ – type admin "${data.type}" émis par non-admin ${socket.id}`,
      );
      return;
    }

    const payload = {
      type: data.type,
      msg: data.msg || {},
      name: data.name || meta?.pseudo || "unknown",
      from: socket.id,
    };

    log(
        `  [room→${data.room}] type="${data.type}" from=${socket.id} (admin=${meta?.isAdmin})`,
    );

    // Diffuse à toute la salle — hot path, aucun I/O
    // (io.to().emit() passe par l'adapter Redis, propagation cross-instance OK)
    io.to(data.room).emit("sendMsg", payload);
  });
}

module.exports = { registerRoomHandler, flushHistoriqueSync };
