// tests/helpers/createServer.js
// ─── Fixture serveur ──────────────────────────────────────────────────────────
// Lance une instance du serveur Socket.IO sur un port aléatoire (port 0)
// pour l'isolation totale entre suites de tests.

const http       = require('http');
const express    = require('express');
const { Server } = require('socket.io');
const { io: Client } = require('socket.io-client');

const socketMeta = require('../../store');

const { registerAdminHandler }      = require('../../handlers/adminHandler');
const { registerBidderHandler }     = require('../../handlers/bidderHandler');
const { registerRoomHandler }       = require('../../handlers/roomHandler');
const { registerMessageHandler }    = require('../../handlers/messageHandler');
const { registerFollowHandler }     = require('../../handlers/followHandler');
const { registerScreenHandler }     = require('../../handlers/screenHandler');
const { registerDisconnectHandler } = require('../../handlers/disconnectHandler');

/**
 * Crée et démarre un serveur de test.
 * @returns {{ io, httpServer, url, stop }}
 */
function createTestServer() {
  const app    = express();
  const httpServer = http.createServer(app);

  const io = new Server(httpServer, {
    cors: { origin: '*' },
    allowEIO3: true
  });

  io.on('connection', (socket) => {
    socketMeta.set(socket.id, { pseudo: 'unknown', room: null, isAdmin: false });
    registerAdminHandler(io, socket);
    registerBidderHandler(io, socket);
    registerRoomHandler(io, socket);
    registerMessageHandler(io, socket);
    registerFollowHandler(io, socket);
    registerScreenHandler(io, socket);
    registerDisconnectHandler(io, socket);
  });

  return new Promise((resolve) => {
    httpServer.listen(0, () => {
      const { port } = httpServer.address();
      const url = `http://localhost:${port}`;

      resolve({
        io,
        httpServer,
        url,
        /** Arrête proprement le serveur et vide le store. */
        stop: () => new Promise((res) => {
          io.close();
          httpServer.close(() => {
            socketMeta.clear();
            res();
          });
        })
      });
    });
  });
}

/**
 * Connecte un client socket.io et attend l'événement 'connect'.
 * @param {string} url
 * @param {object} [opts]  options socket.io-client
 * @returns {Promise<Socket>}
 */
function connectClient(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const socket = Client(url, {
      transports: ['websocket'],
      forceNew  : true,
      ...opts
    });
    socket.once('connect', () => resolve(socket));
    socket.once('connect_error', reject);
  });
}

/**
 * Déconnecte un client et attend la fermeture complète.
 * @param {import('socket.io-client').Socket} socket
 */
function disconnectClient(socket) {
  return new Promise((resolve) => {
    if (!socket.connected) return resolve();
    socket.once('disconnect', resolve);
    socket.disconnect();
  });
}

/**
 * Attend qu'un événement soit reçu sur un socket (avec timeout).
 * @param {Socket} socket
 * @param {string} event
 * @param {number} [timeout=2000]
 */
function waitFor(socket, event, timeout = 2000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timeout waiting for "${event}"`)),
      timeout
    );
    socket.once(event, (data) => {
      clearTimeout(timer);
      resolve(data);
    });
  });
}

module.exports = { createTestServer, connectClient, disconnectClient, waitFor };
