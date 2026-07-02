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
// ─────────────────────────────────────────────────────────────────────────────

const socketMeta = require("../store");
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

// ─────────────────────────────────────────────────────────────────────────────
// NOTE : updateHistoriquePseudo supprimé
//
// L'ancienne version faisait readFileSync → parse → find → writeFileSync
// pour mettre à jour le pseudo — opération coûteuse bloquante.
//
// Désormais le pseudo est inclus directement dans l'entrée joinroom
// via socketMeta (mis à jour dès 'username'), ou mis à jour dans le buffer
// en mémoire avant le flush.
// ─────────────────────────────────────────────────────────────────────────────

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
// (→ disconnectHandler fait le ménage : socketMeta.delete + broadcastUserList).
//
// Si le socket n'existe plus côté transport (fantôme déjà fermé mais pas
// encore nettoyé du store), on supprime directement son entrée socketMeta.
// ─────────────────────────────────────────────────────────────────────────────

function evictStaleAdmin(io, room, incomingSocketId) {
  const existingAdminId = getAdminOfRoom(room);
  if (!existingAdminId || existingAdminId === incomingSocketId) return;

  const oldSocket = io.sockets.sockets.get(existingAdminId);

  if (oldSocket) {
    log(
      `  [ADMIN REPLACE] ancien admin=${existingAdminId} expulsé de room="${room}" (remplacé par ${incomingSocketId})`,
    );
    oldSocket.disconnect(true); // ferme proprement → déclenche disconnectHandler
  } else {
    log(
      `  [ADMIN REPLACE] ancien admin=${existingAdminId} déjà fantôme, nettoyage direct du store`,
    );
    socketMeta.delete(existingAdminId);
  }
}

// ─────────────────────────────────────────────────────────────────────────────

function registerRoomHandler(io, socket) {
  /**
   * Rejoindre une salle.
   */
  socket.on("joinroom", (room) => {
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinroom] REFUSÉ room invalide – socket=${socket.id}`);
      return;
    }

    const meta = socketMeta.get(socket.id);
    const clientIp = getClientIp(socket);

    // Quitter l'ancienne salle
    if (meta?.room) {
      socket.leave(meta.room);
      if (meta.isAdmin) broadcastUserList(io, meta.room);
    }

    // ── Anti double-admin : expulser tout admin précédent sur cette room ──
    if (meta?.isAdmin) {
      evictStaleAdmin(io, room, socket.id);
    }

    socket.join(room);
    if (meta) meta.room = room;

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
      broadcastUserList(io, room);
    } else {
      const adminId = getAdminOfRoom(room);
      socket.emit("userList", { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || "none"}`);
    }
  });

  // ───────────────────────────────────────────────────────────────────────────

  socket.on("username", (pseudo) => {
    if (typeof pseudo !== "string" || !pseudo.trim()) return;

    const trimmed = pseudo.trim();
    const meta = socketMeta.get(socket.id);
    if (meta) meta.pseudo = trimmed;

    log(`  [username] socket=${socket.id} pseudo="${trimmed}"`);

    // Mise à jour en mémoire uniquement — zéro I/O
    updateBufferPseudo(socket.id, trimmed);
  });

  // ───────────────────────────────────────────────────────────────────────────

  socket.on("getMsgRoom", (data) => {
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
    const meta = socketMeta.get(socket.id);
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
    io.to(data.room).emit("sendMsg", payload);
  });
}

module.exports = { registerRoomHandler, flushHistoriqueSync };
