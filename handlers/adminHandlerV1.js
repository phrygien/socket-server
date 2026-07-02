// ─── Admin Handler ────────────────────────────────────────────────────────────
// Événements émis par switcher.php

const socketMeta               = require('../store');
const { log }                  = require('../utils/logger');
const { broadcastUserList }    = require('../services/roomService');

function registerAdminHandler(io, socket) {
  /**
   * Identification de l'admin — émis avant joinroom.
   * socket.emit('admin', pseudo)
   */
  socket.on('admin', (pseudo) => {
    const meta = socketMeta.get(socket.id);
    if (meta) {
      meta.pseudo  = pseudo || 'Admin';
      meta.isAdmin = true;
    }
    log(`  [admin]    : ${socket.id} → "${pseudo}"`);

    // Si l'admin était déjà dans une salle (reconnexion rapide), notifier
    if (meta?.room) broadcastUserList(io, meta.room);
  });
}

module.exports = { registerAdminHandler };
