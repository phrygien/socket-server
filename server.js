/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION STABLE MOBILE + APACHE + SOCKET.IO v2/v3/v4
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const { PORT } = require("./config");
const socketMeta = require("./store");
const { log } = require("./utils/logger");

const { getRoomStats } = require("./services/roomService");

const { registerAdminHandler } = require("./handlers/adminHandler");
const { registerBidderHandler } = require("./handlers/bidderHandler");
const { registerRoomHandler } = require("./handlers/roomHandler");
const { registerMessageHandler } = require("./handlers/messageHandler");
const { registerDisconnectHandler } = require("./handlers/disconnectHandler");

const {
  registerFollowHandler,
  getFollowersInRoom,
} = require("./handlers/followHandler");

const {
  registerScreenHandler,
  getScreensInRoom,
} = require("./handlers/screenHandler");

// ─────────────────────────────────────────────────────────────
// EXPRESS
// ─────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);

app.use(express.json());

// ─────────────────────────────────────────────────────────────
// CORS
// ─────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://www.auctav.com",
  "https://auctav.com",
  "https://dev.astucom.com",
  "http://localhost",
  "http://127.0.0.1",
];

// ─────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────

app.get("/", (_req, res) => {
  res.json({
    status: "ok",
    uptime: process.uptime(),
    //rooms   : getRoomStats(),
    memory: process.memoryUsage(),
    sockets: socketMeta.size,
    connPerIP: connPerIP.size, // utile pour monitorer les IPs actives
  });
});

// Followers debug
app.get("/follow/:room", (req, res) => {
  res.json({
    room: req.params.room,
    followers: getFollowersInRoom(req.params.room),
  });
});

// Screens debug
app.get("/screen/:room", (req, res) => {
  res.json({
    room: req.params.room,
    screens: getScreensInRoom(req.params.room),
  });
});

// ─────────────────────────────────────────────────────────────
// SOCKET.IO
// ─────────────────────────────────────────────────────────────

const io = new Server(server, {
  // MOBILE / RÉSEAUX LENTS
  pingInterval: 10000,
  pingTimeout: 20000,

  // Timeout handshake — coupe les connexions qui traînent
  connectTimeout: 10000,

  // GROS PAYLOADS
  maxHttpBufferSize: 1e7, // 10Mo

  // Compression — seuil relevé pour éviter de compresser les petits messages
  perMessageDeflate: {
    threshold: 8192,
  },

  // Compatibilité anciens clients
  allowEIO3: true,

  // polling + websocket
  transports: ["polling", "websocket"],

  cors: {
    origin: function (origin, callback) {
      if (!origin) {
        return callback(null, true);
      }

      if (ALLOWED_ORIGINS.includes(origin)) {
        return callback(null, true);
      }

      log(`CORS bloqué : ${origin}`);

      return callback(new Error("CORS blocked"));
    },

    methods: ["GET", "POST"],
    credentials: true,
  },
});

// ─────────────────────────────────────────────────────────────
// RATE LIMITING PAR IP
// ─────────────────────────────────────────────────────────────

const connPerIP = new Map();
const MAX_CONN = 5;

// Nettoyage périodique — évite la fuite mémoire sur sockets zombies
setInterval(() => {
  let cleaned = 0;
  for (const [ip, count] of connPerIP.entries()) {
    if (count <= 0) {
      connPerIP.delete(ip);
      cleaned++;
    }
  }
  if (cleaned > 0) {
    log(`[RATE LIMIT] Nettoyage : ${cleaned} entrées supprimées`);
  }
}, 60_000);

io.use((socket, next) => {
  const ip =
    socket.handshake.headers["x-forwarded-for"] || socket.handshake.address;

  const count = connPerIP.get(ip) || 0;

  if (count >= MAX_CONN) {
    log(`[RATE LIMIT] IP bloquée : ${ip} (${count} connexions)`);
    return next(new Error("Too many connections"));
  }

  connPerIP.set(ip, count + 1);

  socket.on("disconnect", () => {
    const n = (connPerIP.get(ip) || 1) - 1;
    n <= 0 ? connPerIP.delete(ip) : connPerIP.set(ip, n);
  });

  next();
});

// ─────────────────────────────────────────────────────────────
// SOCKET CONNECTION
// ─────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  log(`+ Connexion : ${socket.id}`);

  socketMeta.set(socket.id, {
    pseudo: "unknown",
    room: null,
    isAdmin: false,
  });

  // ───────────────────────────────────────────────────
  // DEBUG TRANSPORT
  // ───────────────────────────────────────────────────

  log(`Transport : ${socket.conn.transport.name}`);

  socket.conn.on("upgrade", () => {
    log(`[UPGRADE] ${socket.id} -> ${socket.conn.transport.name}`);
  });

  // ───────────────────────────────────────────────────
  // DEBUG DISCONNECT
  // ───────────────────────────────────────────────────

  socket.on("disconnect", (reason) => {
    log(`- Déconnexion: ${socket.id} (${reason})`);
  });

  socket.on("connect_error", (err) => {
    log(`Connect error ${socket.id}: ${err.message}`);
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
  log(`Health : http://localhost:${PORT}/`);
});

// ─────────────────────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────

process.on("SIGTERM", () => {
  log("SIGTERM reçu — arrêt propre");
  server.close(() => process.exit(0));
});

process.on("SIGINT", () => {
  log("SIGINT reçu — arrêt propre");
  server.close(() => process.exit(0));
});

// ─────────────────────────────────────────────────────────────
// PROTECTION GLOBALE CONTRE LES CRASHS
// Ajout de process.exit(1) — laisse PM2 redémarrer proprement
// plutôt que continuer dans un état corrompu
// ─────────────────────────────────────────────────────────────

process.on("uncaughtException", (err) => {
  log(`[FATAL] uncaughtException: ${err.message}`);
  log(err.stack || "");
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  log(`[FATAL] unhandledRejection: ${reason}`);
  process.exit(1);
});
