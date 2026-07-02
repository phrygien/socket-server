// ─── Disconnect Handler ───────────────────────────────────────────────────────

const socketMeta            = require('../store');
const { log }               = require('../utils/logger');
const { broadcastUserList } = require('../services/roomService');

function registerDisconnectHandler(io, socket) {
  socket.on('disconnect', (reason) => {

    const meta = socketMeta.get(socket.id);

    // Supprimer du store EN PREMIER pour que broadcastUserList
    // ne retrouve plus cet utilisateur dans les listes
    socketMeta.delete(socket.id);

    if (!meta?.room) return;

    try {

      // Notifie la salle qu'un utilisateur est parti
      io.to(meta.room).emit('sendMsg', {
        type : 'exit',
        msg  : { room: meta.room, email: meta.email || '' },
        name : meta.pseudo || 'unknown',
        from : socket.id
      });

      // Rebroadcast la liste utilisateurs (admin ET bidder)
      broadcastUserList(io, meta.room);

      // Log spécifique si c'était l'admin
      if (meta.isAdmin) {
        log(`[ADMIN OFFLINE] room=${meta.room} socket=${socket.id}`);
      }

    } catch (err) {
      log(`[ERROR] disconnect handler socket=${socket.id} : ${err.message}`);
    }

  });
}

module.exports = { registerDisconnectHandler };