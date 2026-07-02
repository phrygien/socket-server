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

const socketMeta                              = require('../store');
const { log }                                 = require('../utils/logger');
const { getAdminOfRoom, broadcastUserList }   = require('../services/roomService');

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
 *
 * listLot est intentionnellement ici : sans ce contrôle, un bidder
 *     pourrait broadcaster une liste falsifiée (faux prix, faux statuts)
 *     visible par tous les participants de la salle.
 */
const ADMIN_ONLY_TYPES = new Set([
  'listLot',
  'numLot',
  'previousLot',
  'closeEnchere',
  'updateLot',
]);

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

    const meta = socketMeta.get(socket.id);

    // Quitter l'ancienne salle si nécessaire
    if (meta?.room) {
      const oldRoom = meta.room;
      socket.leave(oldRoom);
      if (meta.isAdmin) broadcastUserList(io, oldRoom);
    }

    socket.join(room);
    if (meta) meta.room = room;

    log(`  [joinroom] : ${socket.id} → ${room} (admin=${meta?.isAdmin})`);

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
    // Empêche un attaquant de forger data.room pour cibler une autre vente.
    if (!socket.rooms.has(data.room)) {
      log(`  [getMsgRoom] REFUSÉ – ${socket.id} n'appartient pas à la salle "${data.room}"`);
      return;
    }

    // ── Contrôle 3 : type dans la liste blanche ───────────────────────────
    // Bloque tout type inconnu ou inventé côté client.
    if (!ALLOWED_TYPES.has(data.type)) {
      log(`  [getMsgRoom] REFUSÉ – type non autorisé "${data.type}" depuis ${socket.id}`);
      return;
    }

    // ── Contrôle 4 : types sensibles réservés à l'admin ──────────────────
    // Un bidder ou un visiteur ne peut jamais émettre listLot, numLot,
    // closeEnchere, etc., même s'il est membre de la salle.
    const meta = socketMeta.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(`  [getMsgRoom] REFUSÉ – type admin "${data.type}" émis par un non-admin ${socket.id}`);
      return;
    }

    // ── Payload nettoyé — from toujours issu du serveur, jamais du client ─
    const payload = {
      type : data.type,
      msg  : data.msg  || {},
      name : data.name || meta?.pseudo || 'unknown',
      from : socket.id,   // identité réelle, non spoofable
    };

    log(`  [room→${data.room}] type="${data.type}" from=${socket.id} (admin=${meta?.isAdmin})`);

    // Diffuse à TOUS les membres de la salle, y compris l'émetteur
    io.to(data.room).emit('sendMsg', payload);
  });
}

module.exports = { registerRoomHandler };