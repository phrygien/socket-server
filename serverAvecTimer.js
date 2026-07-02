/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION STABLE MOBILE + APACHE + SOCKET.IO v2/v3/v4
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const { PORT } = require('./config');
const socketMeta = require('./store');
const { log } = require('./utils/logger');

const { getRoomStats } = require('./services/roomService');
const {
  getSaleEndRemaining,
  getActiveTimers: getActiveEndTimers
} = require('./services/saleEndService');

const { registerAdminHandler }      = require('./handlers/adminHandler');
const { registerBidderHandler }     = require('./handlers/bidderHandler');
const { registerRoomHandler }       = require('./handlers/roomHandler');
const { registerMessageHandler }    = require('./handlers/messageHandler');
const { registerDisconnectHandler } = require('./handlers/disconnectHandler');

const {
  registerFollowHandler,
  getFollowersInRoom
} = require('./handlers/followHandler');

const {
  registerScreenHandler,
  getScreensInRoom
} = require('./handlers/screenHandler');

// ─────────────────────────────────────────────────────────────
// EXPRESS
// ─────────────────────────────────────────────────────────────

const app    = express();
const server = http.createServer(app);

app.use(express.json());

// ─────────────────────────────────────────────────────────────
// CORS
// ─────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  'https://www.auctav.com',
  'https://auctav.com',
  'https://dev.astucom.com',
  'http://localhost',
  'http://127.0.0.1'
];

// ─────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────

app.get('/', (_req, res) => {
  res.json({
    status  : 'ok',
    uptime  : process.uptime(),
    rooms   : getRoomStats(),
    timers  : getActiveEndTimers(),
    memory  : process.memoryUsage()
  });
});

// Followers debug
app.get('/follow/:room', (req, res) => {
  res.json({
    room      : req.params.room,
    followers : getFollowersInRoom(req.params.room)
  });
});

// Screens debug
app.get('/screen/:room', (req, res) => {
  res.json({
    room    : req.params.room,
    screens : getScreensInRoom(req.params.room)
  });
});

// Sale end timer debug
app.get('/saleend/:room', (req, res) => {
  const remaining = getSaleEndRemaining(req.params.room);
  res.json({
    room             : req.params.room,
    remainingSeconds : remaining,
    active           : remaining !== null
  });
});


// Démarre un timer de test
// GET /test/timer/start/:room/:seconds
// ex: http://localhost:3005/test/timer/start/auctav42/120
app.get('/test/timer/start/:room/:seconds', (req, res) => {

  const room    = req.params.room;
  const seconds = parseInt(req.params.seconds);

  if (!room || isNaN(seconds) || seconds <= 0) {
    return res.status(400).json({ error: 'room et seconds (> 0) requis' });
  }

  const { updateSaleEndTimer } = require('./services/saleEndService');
  updateSaleEndTimer(io, room, seconds);

  res.json({
    ok               : true,
    room,
    remainingSeconds : seconds,
    message          : `Timer démarré pour ${seconds}s dans la room ${room}`
  });
});

// Arrête le timer de test
// GET /test/timer/stop/:room
app.get('/test/timer/stop/:room', (req, res) => {

  const { clearSaleEndTimer } = require('./services/saleEndService');
  clearSaleEndTimer(req.params.room);

  res.json({
    ok      : true,
    room    : req.params.room,
    message : `Timer arrêté pour la room ${req.params.room}`
  });
});
// ─────────────────────────────────────────────────────────────
// SOCKET.IO
// ─────────────────────────────────────────────────────────────

const io = new Server(server, {

  // IMPORTANT MOBILE / RESEAUX LENTS
  pingInterval: 25000,
  pingTimeout : 60000,

  // IMPORTANT GROS PAYLOADS
  maxHttpBufferSize: 1e8,

  // Compression
  perMessageDeflate: {
    threshold: 1024
  },

  // Compatibilité anciens clients
  allowEIO3: true,

  // IMPORTANT:
  // polling + websocket
  // polling aide énormément réseaux mobile
  transports: ['polling', 'websocket'],

  cors: {
    origin: function (origin, callback) {

      // autorise requêtes sans origin
      // apps mobiles / curl / server-to-server
      if (!origin) {
        return callback(null, true);
      }

      if (ALLOWED_ORIGINS.includes(origin)) {
        return callback(null, true);
      }

      log(`CORS bloqué : ${origin}`);

      return callback(new Error('CORS blocked'));
    },

    methods     : ['GET', 'POST'],
    credentials : true
  }
});

// ─────────────────────────────────────────────────────────────
// SOCKET CONNECTION
// ─────────────────────────────────────────────────────────────

io.on('connection', (socket) => {

  log(`+ Connexion : ${socket.id}`);

  socketMeta.set(socket.id, {
    pseudo  : 'unknown',
    room    : null,
    isAdmin : false
  });

  // ==================== MIDDLEWARE ANTI-DOUBLON GLOBAL ====================
  const messageHistory = new Map();

  function checkAndRecord(socketId, eventName, data) {
    const key = `${socketId}_${eventName}_${JSON.stringify(data)}`;
    const now = Date.now();

    if (messageHistory.has(key)) {
      const lastTime = messageHistory.get(key);
      if (now - lastTime < 500) {
        return false; // C'est un doublon
      }
    }

    messageHistory.set(key, now);

    // Nettoyage automatique après 1 seconde
    setTimeout(() => {
      if (messageHistory.get(key) === now) {
        messageHistory.delete(key);
      }
    }, 1000);

    return true; // Message unique
  }

  // Appliquer à tous les événements
  socket.use(([event, ...args], next) => {
    // Vérifier uniquement pour les événements critiques
    const criticalEvents = ['getMsgRoom', 'getMsgPrivate', 'doEncheres'];

    if (criticalEvents.includes(event) && args[0]) {
      if (!checkAndRecord(socket.id, event, args[0])) {
        log(`Doublon bloqué: ${event} de ${socket.id}`);
        return; // Bloque le doublon silencieusement
      }
    }

    next();
  });

  // ───────────────────────────────────────────────────
  // DEBUG TRANSPORT
  // ───────────────────────────────────────────────────

  log(`Transport : ${socket.conn.transport.name}`);

  socket.conn.on('upgrade', () => {
    log(`[UPGRADE] ${socket.id} -> ${socket.conn.transport.name}`);
  });

  // ───────────────────────────────────────────────────
  // DEBUG DISCONNECT
  // ───────────────────────────────────────────────────

  socket.on('disconnect', (reason) => {
    log(`- Déconnexion: ${socket.id} (${reason})`);
  });

  socket.on('connect_error', (err) => {
    log(`Connect error ${socket.id}: ${err.message}`);
  });

  // ───────────────────────────────────────────────────
  // SYNC TIMER À LA CONNEXION
  // Un client qui (re)connecte reçoit immédiatement
  // le temps restant de sa salle s'il est déjà dans une room
  // ───────────────────────────────────────────────────

  socket.on('saleEndSync', () => {
    const meta      = socketMeta.get(socket.id);
    const room      = meta?.room;
    const remaining = room ? getSaleEndRemaining(room) : null;

    if (remaining !== null) {
      socket.emit('saleEndTimer', {
        room,
        remainingSeconds : remaining,
        ended            : remaining <= 0
      });
      log(`  [saleEndSync] → ${socket.id} room=${room} remaining=${remaining}s`);
    } else {
      socket.emit('saleEndTimer', { active: false });
    }
  });

  // ───────────────────────────────────────────────────
  // HANDLERS
  // ───────────────────────────────────────────────────

  registerAdminHandler(io, socket);
  registerBidderHandler(io, socket);
  registerRoomHandler(io, socket);
  registerMessageHandler(io, socket);
  registerFollowHandler(io, socket);
  registerScreenHandler(io, socket);
  registerDisconnectHandler(io, socket);
});

// ─────────────────────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  log(`Socket.IO server démarré sur port ${PORT}`);
  log(`Mode : PRODUCTION`);
  log(`Health    : http://localhost:${PORT}/`);
  log(`Sale end  : http://localhost:${PORT}/saleend/:room`);
});

// ─────────────────────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────

process.on('SIGTERM', () => {
  log('SIGTERM reçu — arrêt propre');
  server.close(() => {
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  log('SIGINT reçu — arrêt propre');
  server.close(() => {
    process.exit(0);
  });
});