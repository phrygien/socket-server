// ─── Follow Handler ───────────────────────────────────────────────────────────
// Gère la salle "auctav_follow" utilisée par follow.php
//
// Flux :
//   follow.php se connecte en tant que "Follower" (non-admin)
//   → joinroom('auctav_follow')
//   → username(pseudo)
//   → getMsgPrivate({ toid: idAdmin, type: 'follow', msg: { state: true } })
//        (heartbeat toutes les 3 minutes)
//
//   L'admin (switcher.php) émet vers la salle :
//   → getMsgRoom({ room, type: 'message',  msg: { text, style } })  ← messages texte
//   → getMsgRoom({ room, type: 'users',    msg: { text } })          ← liste HTML des bidders
//   → getMsgRoom({ room, type: 'follow' })                           ← réponse heartbeat
//
// Côté serveur on ne fait rien de spécial de plus :
//   - 'follow' reçu en privé → on le relaye à la salle entière
//     (l'admin voit que le Follower est bien connecté)
//   - 'users' / 'message' viennent déjà de getMsgRoom (roomHandler)
//
// Ce handler ajoute :
//   1. broadcastFollowPing   : quand un follower envoie un ping à l'admin,
//                              le serveur le retransmet à la salle pour que
//                              l'admin puisse répondre en broadcast.
//   2. getFollowStats (REST) : endpoint GET /follow/:room pour le debug.

const socketMeta = require('../store');
const { log }    = require('../utils/logger');

const FOLLOW_ROOM = 'auctav_follow';

function registerFollowHandler(io, socket) {
  /**
   * Ping heartbeat du Follower vers l'admin.
   * Le Follower émet : getMsgPrivate({ toid: idAdmin, type: 'follow', … })
   * → déjà géré par messageHandler (relayé en privé à l'admin).
   *
   * Quand l'admin répond en broadcast (getMsgRoom type:'follow'),
   * → déjà géré par roomHandler (diffusé à toute la salle).
   *
   * Ici on gère le cas où le Follower envoie directement 'follow'
   * à la salle (sans passer par getMsgPrivate).
   */
  socket.on('follow', (data) => {
    const meta = socketMeta.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [follow]   : ${socket.id} "${meta?.pseudo}" → ${room}`);

    io.to(room).emit('sendMsg', {
      type : 'follow',
      msg  : data || {},
      name : meta?.pseudo || 'unknown',
      from : socket.id
    });
  });

  /**
   * Le Follower demande explicitement la liste des users connectés.
   * L'admin doit répondre avec getMsgRoom({ type:'users', msg:{ text } }).
   */
  socket.on('getUsers', (data) => {
    const meta = socketMeta.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    log(`  [getUsers] : ${socket.id} → ${room}`);

    io.to(room).emit('sendMsg', {
      type : 'getUsers',
      msg  : data || {},
      name : meta?.pseudo || 'unknown',
      from : socket.id
    });
  });
}

/**
 * Retourne les followers actuellement connectés dans une salle.
 * Utilisé par l'endpoint REST GET /follow/:room
 */
function getFollowersInRoom(room) {
  const followers = [];
  for (const [id, meta] of socketMeta.entries()) {
    if (meta.room === room && !meta.isAdmin) {
      followers.push({
        id,
        pseudo    : meta.pseudo,
        isAdmin   : meta.isAdmin,
        isFollower: meta.pseudo?.startsWith('Follow_')
      });
    }
  }
  return followers;
}

module.exports = { registerFollowHandler, getFollowersInRoom, FOLLOW_ROOM };
