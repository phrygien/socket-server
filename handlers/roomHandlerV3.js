// ─── Room Handler ─────────────────────────────────────────────────────────────
//
//  Sécurité appliquée :
//  1. Validation des entrées (room, type)
//  2. Le socket doit appartenir à data.room (anti-room-forgery)
//  3. Liste blanche des types autorisés (ALLOWED_TYPES)
//  4. Types sensibles réservés à l'admin (ADMIN_ONLY_TYPES)
//  5. listLot inclus dans ADMIN_ONLY_TYPES (un bidder ne peut pas
//     broadcaster une fausse liste de lots à toute la salle)
// ─────────────────────────────────────────────────────────────────────────────

const socketMeta                            = require('../store');
const { log }                               = require('../utils/logger');
const { getAdminOfRoom, broadcastUserList } = require('../services/roomService');
const fs                                    = require('fs');
const path                                  = require('path');

// ── Chemin vers l'historique ──────────────────────────────────────────────────
const HISTORIQUE_PATH = path.resolve(__dirname, '../historique.json');

// ── Listes de contrôle ────────────────────────────────────────────────────────

/**
 * Tous les types qu'un socket peut diffuser via getMsgRoom.
 * Tout type absent est bloqué, même s'il existe côté client.
 */
const ALLOWED_TYPES = new Set([
  'listLot',      // liste complète des lots (admin → salle)
  'numLot',       // changement de lot en cours (admin → screen.php)
  'previousLot',  // lot précédent avec prix adjugé (admin → screen.php)
  'message',      // message texte libre (admin → follow.php)
  'users',        // liste HTML des bidders connectés (admin → follow.php)
  'closeEnchere', // clôture d'une enchère (admin → results.php)
  'updateLot',    // mise à jour d'un lot (admin → results.php)
]);

/**
 * Types réservés à l'admin.
 * Un bidder ou un visiteur qui émet l'un de ces types est bloqué,
 * même s'il a rejoint la salle correctement.
 */
const ADMIN_ONLY_TYPES = new Set([
  'listLot',
  'numLot',
  'previousLot',
  'closeEnchere',
  'updateLot',
]);

// ── Utilitaires historique ────────────────────────────────────────────────────

/**
 * Charge l'historique depuis le fichier JSON.
 * Retourne un tableau vide si le fichier n'existe pas ou est corrompu.
 */
function loadHistorique() {
  try {
    if (!fs.existsSync(HISTORIQUE_PATH)) return [];
    const raw = fs.readFileSync(HISTORIQUE_PATH, 'utf8');
    return JSON.parse(raw);
  } catch {
    return [];
  }
}

/**
 * Ajoute une entrée à historique.json.
 * @param {object} entry
 */
function appendHistorique(entry) {
  try {
    const data = loadHistorique();
    data.push(entry);
    fs.writeFileSync(HISTORIQUE_PATH, JSON.stringify(data, null, 2), 'utf8');
  } catch (err) {
    log(`  [historique] ERREUR écriture : ${err.message}`);
  }
}

/**
 * Met à jour le pseudo dans historique.json pour le socketId donné.
 * Cherche la dernière entrée joinroom de ce socket et y injecte le pseudo.
 * @param {string} socketId
 * @param {string} pseudo
 */
function updateHistoriquePseudo(socketId, pseudo) {
  try {
    const data = loadHistorique();

    // Parcourt depuis la fin pour trouver la dernière entrée joinroom du socket
    for (let i = data.length - 1; i >= 0; i--) {
      if (data[i].socketId === socketId && data[i].event === 'joinroom') {
        data[i].pseudo = pseudo;
        break;
      }
    }

    fs.writeFileSync(HISTORIQUE_PATH, JSON.stringify(data, null, 2), 'utf8');
    log(`  [historique] pseudo mis à jour → socketId=${socketId} pseudo="${pseudo}"`);
  } catch (err) {
    log(`  [historique] ERREUR mise à jour pseudo : ${err.message}`);
  }
}

/**
 * Récupère l'IP réelle du socket.
 * Prend en compte les proxies via x-forwarded-for.
 * @param {object} socket
 * @returns {string}
 */
function getClientIp(socket) {
  const forwarded = socket.handshake.headers['x-forwarded-for'];
  if (forwarded) {
    // x-forwarded-for peut contenir plusieurs IPs séparées par une virgule
    // La première est l'IP du client d'origine
    return forwarded.split(',')[0].trim();
  }
  return socket.handshake.address || 'inconnue';
}

// ─────────────────────────────────────────────────────────────────────────────

function registerRoomHandler(io, socket) {

  /**
   * Rejoindre une salle.
   * room = "auctav<saleId>"  ex: "auctav42"
   */
  socket.on('joinroom', (room) => {

    // ── Contrôle : room doit être une string non vide ─────────────────────
    if (typeof room !== 'string' || !room.trim()) {
      log(`  [joinroom] REFUSÉ room invalide – socket=${socket.id}`);
      return;
    }

    const meta     = socketMeta.get(socket.id);
    const clientIp = getClientIp(socket);

    // Quitter l'ancienne salle si nécessaire
    if (meta?.room) {
      const oldRoom = meta.room;
      socket.leave(oldRoom);
      if (meta.isAdmin) broadcastUserList(io, oldRoom);
    }

    socket.join(room);
    if (meta) meta.room = room;

    // ── Affichage IP dans les logs ─────────────────────────────────────────
    log(`  [joinroom] socket=${socket.id} → room="${room}" ip=${clientIp} admin=${meta?.isAdmin}`);

    // ── Sauvegarde dans historique.json (pseudo = inconnu pour l'instant) ─
    // Il sera mis à jour dès réception de l'événement 'username'
    appendHistorique({
      event     : 'joinroom',
      socketId  : socket.id,
      ip        : clientIp,
      room      : room,
      pseudo    : meta?.pseudo || 'inconnu',
      isAdmin   : meta?.isAdmin ?? false,
      timestamp : new Date().toISOString(),
    });

    if (meta?.isAdmin) {
      // Admin rejoint → tous les bidders peuvent maintenant enchérir
      broadcastUserList(io, room);
    } else {
      // Bidder/visiteur rejoint → lui envoyer en privé l'admin actuel
      const adminId = getAdminOfRoom(room);
      socket.emit('userList', { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || 'none'}`);
    }
  });

  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Réception du pseudo du client.
   * Arrive juste après joinroom côté client (socket.emit('username', pseudo)).
   * On met à jour socketMeta ET l'entrée historique correspondante.
   */
  socket.on('username', (pseudo) => {

    if (typeof pseudo !== 'string' || !pseudo.trim()) return;

    const meta = socketMeta.get(socket.id);
    if (meta) meta.pseudo = pseudo.trim();

    log(`  [username] socket=${socket.id} pseudo="${pseudo.trim()}"`);

    // Mise à jour de l'entrée joinroom dans historique.json
    updateHistoriquePseudo(socket.id, pseudo.trim());
  });

  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Diffusion d'un message vers toute la salle.
   *
   * data = { room, type, msg, name }
   *
   * Types émis par l'admin (switcher.php) via getMsgRoom :
   *   listLot      → liste des lots de la vente
   *   numLot       → changement de lot en cours    (→ screen.php)
   *   previousLot  → lot précédent avec prix adjugé (→ screen.php)
   *   message      → message texte libre            (→ follow.php)
   *   users        → liste HTML des bidders         (→ follow.php)
   *   closeEnchere → clôture d'une enchère          (→ results.php)
   *   updateLot    → mise à jour d'un lot           (→ results.php)
   */
  socket.on('getMsgRoom', (data) => {

    // ── Contrôle 1 : présence et format des champs obligatoires ──────────
    if (!data || typeof data.room !== 'string' || !data.room.trim()) {
      log(`  [getMsgRoom] REFUSÉ champ room absent/invalide – socket=${socket.id}`);
      return;
    }
    if (typeof data.type !== 'string' || !data.type.trim()) {
      log(`  [getMsgRoom] REFUSÉ champ type absent/invalide – socket=${socket.id}`);
      return;
    }

    // ── Contrôle 2 : le socket doit appartenir à la salle déclarée ───────
    if (!socket.rooms.has(data.room)) {
      log(`  [getMsgRoom] REFUSÉ – ${socket.id} n'appartient pas à la salle "${data.room}"`);
      return;
    }

    // ── Contrôle 3 : type dans la liste blanche ───────────────────────────
    if (!ALLOWED_TYPES.has(data.type)) {
      log(`  [getMsgRoom] REFUSÉ – type non autorisé "${data.type}" depuis ${socket.id}`);
      return;
    }

    // ── Contrôle 4 : types sensibles réservés à l'admin ──────────────────
    const meta = socketMeta.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(`  [getMsgRoom] REFUSÉ – type admin "${data.type}" émis par un non-admin ${socket.id}`);
      return;
    }

    // ── Payload nettoyé ───────────────────────────────────────────────────
    const payload = {
      type : data.type,
      msg  : data.msg  || {},
      name : data.name || meta?.pseudo || 'unknown',
      from : socket.id,
    };

    log(`  [room→${data.room}] type="${data.type}" from=${socket.id} (admin=${meta?.isAdmin})`);

    // Diffuse à TOUS les membres de la salle, y compris l'émetteur
    io.to(data.room).emit('sendMsg', payload);
  });
}

module.exports = { registerRoomHandler };