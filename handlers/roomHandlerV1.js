// ─── Room Handler ─────────────────────────────────────────────────────────────

const socketMeta                              = require('../store');
const { log }                                 = require('../utils/logger');
const { getAdminOfRoom, broadcastUserList }   = require('../services/roomService');

function registerRoomHandler(io, socket) {
  /**
   * Rejoindre une salle.
   * room = "auctav<saleId>"  ex: "auctav42"
   */
  socket.on('joinroom', (room) => {
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
      // Bidder rejoint → lui envoyer en privé l'admin actuel
      const adminId = getAdminOfRoom(room);
      socket.emit('userList', { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || 'none'}`);
    }
  });

  /**
   * Diffusion d'un message vers toute la salle.
   * data = { room, type, msg, name }
   *
   * Types émis par l'admin (switcher.php) via getMsgRoom :
   *   listLot      → liste des lots de la vente
   *   numLot       → changement de lot en cours (→ screen.php)
   *   previousLot  → lot précédent avec prix adjugé (→ screen.php)
   *   message      → message texte libre (→ follow.php)
   *   users        → liste HTML des bidders connectés (→ follow.php)
   *   closeEnchere → clôture d'une enchère { numLot, statut, price, toid }
   *                  (→ results.php : met à jour statut + prix du lot)
   *   updateLot    → mise à jour d'un lot en cours { numLot, statut, price, toid }
   *                  (→ results.php : même traitement que closeEnchere)
   */
  socket.on('getMsgRoom', (data) => {
    if (!data || !data.room) return;

    const payload = {
      type : data.type || '',
      msg  : data.msg  || {},
      name : data.name || socketMeta.get(socket.id)?.pseudo || 'unknown',
      from : socket.id
    };

    log(`  [room→${data.room}] type="${data.type}" from=${socket.id}`);

    // Diffuse à TOUS les membres de la salle, y compris l'émetteur
    io.to(data.room).emit('sendMsg', payload);
  });
}

module.exports = { registerRoomHandler };
