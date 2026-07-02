// ─── Message Handler ──────────────────────────────────────────────────────────

const socketMeta = require('../store');
const { log }    = require('../utils/logger');

function registerMessageHandler(io, socket) {
  /**
   * Message privé ciblé vers un socket précis.
   * data = { toid, type, msg, name }
   *
   * Types envoyés par le bidder (vente_list.php) → admin :
   *   reconnection  { room, email }        bidder rechargé / changement device
   *   doEncheres    { room, myEnchere, lot, email }  enchère (mode live)
   *   exit          { room, email }        bidder quitte volontairement
   *   connected     { room, email }        réponse au heartbeat 'follow' de l'admin
   *
   * Types envoyés par l'admin → bidder :
   *   confirmEnchere  { lot, state, manuel }   enchère acceptée ou refusée
   *   validEnchere    { lot }                  enchère adjugée au bidder
   *   changeDevice    {}                       déconnexion forcée (autre device détecté)
   *   noActivity      {}                       déconnexion forcée (inactivité)
   *   listLot         { … }                    envoi de la liste des lots
   *   follow          {}                       heartbeat admin → demande présence
   *
   * Types envoyés par le follower → admin :
   *   follow          { state: true }          heartbeat toutes les 3 min
   *   getScreen       {}                       demande état du lot courant (screen.php)
   */
  socket.on('getMsgPrivate', (data) => {
    if (!data || !data.toid) return;

    const payload = {
      type : data.type || '',
      msg  : data.msg  || {},
      name : data.name || socketMeta.get(socket.id)?.pseudo || 'unknown',
      from : socket.id
    };

    log(`  [private→${data.toid}] type="${data.type}" from=${socket.id}`);

    io.to(data.toid).emit('sendMsg', payload);
  });
}

module.exports = { registerMessageHandler };
