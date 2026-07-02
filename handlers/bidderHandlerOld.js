// ─── Bidder Handler ───────────────────────────────────────────────────────────
// Événements émis par vente_list.php (participants)

const socketMeta            = require('../store');
const { log }               = require('../utils/logger');
const { getAdminOfRoom }    = require('../services/roomService');

function registerBidderHandler(io, socket) {
  /**
   * Identification du bidder.
   * socket.emit('username', pseudo)
   * Répond immédiatement avec l'ID de l'admin de la salle.
   */
  socket.on('username', (pseudo) => {
    const meta = socketMeta.get(socket.id);
    if (meta) meta.pseudo = pseudo || 'Bidder';
    log(`  [username] : ${socket.id} → "${pseudo}"`);

    const room = meta?.room;
    if (room) {
      const adminId = getAdminOfRoom(room);
      socket.emit('userList', { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || 'none'}`);
    }
  });

  /**
   * Connexion initiale du bidder.
   * socket.emit('connected', { name, email, room })
   */
  socket.on('connected', (data) => {
    const meta = socketMeta.get(socket.id);
    if (meta && data) {
      meta.pseudo = data.name  || meta.pseudo;
      meta.email  = data.email || '';
      if (data.room) {
        socket.join(data.room);
        meta.room = data.room;
      }
    }

    const room = meta?.room;
    if (!room) return;

    log(`  [connected]: ${socket.id} "${data?.name}" (${data?.email}) → ${room}`);

    io.to(room).emit('sendMsg', {
      type : 'connected',
      msg  : data || {},
      name : meta?.pseudo || 'unknown',
      from : socket.id
    });
  });

  /**
   * Reconnexion d'un bidder (changement de device / rechargement).
   * socket.emit('reconnection', { name, email, room })
   */
  socket.on('reconnection', (data) => {
    const meta = socketMeta.get(socket.id);
    if (meta && data) {
      meta.pseudo = data.name  || meta.pseudo;
      meta.email  = data.email || '';
      if (data.room) {
        socket.join(data.room);
        meta.room = data.room;
      }
    }

    const room = meta?.room;
    if (!room) return;

    log(`  [reconnect]: ${socket.id} "${data?.name}" (${data?.email}) → ${room}`);

    io.to(room).emit('sendMsg', {
      type : 'reconnection',
      msg  : data || {},
      name : meta?.pseudo || 'unknown',
      from : socket.id
    });
  });

  /**
   * Demande de la liste des lots (bidder vient de charger la page).
   * socket.emit('getEncheresList', { room })
   */
  socket.on('getEncheresList', (data) => {
    const room = socketMeta.get(socket.id)?.room || data?.room;
    if (!room) return;

    log(`  [getList]  : ${socket.id} → ${room}`);

    io.to(room).emit('sendMsg', {
      type : 'getEncheresList',
      msg  : data || {},
      name : socketMeta.get(socket.id)?.pseudo || 'unknown',
      from : socket.id
    });
  });

  /**
   * Enchère placée par un bidder.
   * socket.emit('doEncheres', { lot, myEnchere, room })
   */
  socket.on('doEncheres', (data) => {
    const meta = socketMeta.get(socket.id);
    const room = meta?.room || data?.room;
    if (!room) return;

    log(`  [enchère]  : ${socket.id} lot=${data?.lot} montant=${data?.myEnchere}`);

    io.to(room).emit('sendMsg', {
      type : 'doEncheres',
      msg  : data || {},
      name : meta?.pseudo || 'unknown',
      from : socket.id
    });
  });

  /**
   * Vérification de présence (heartbeat).
   * socket.emit('follow', data)
   */
  socket.on('follow', (data) => {
    const meta = socketMeta.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    io.to(room).emit('sendMsg', {
      type : 'follow',
      msg  : data || {},
      name : meta?.pseudo || 'unknown',
      from : socket.id
    });
  });
  
}

module.exports = { registerBidderHandler };
