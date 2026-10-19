/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION CLUSTERISÉE — Docker + Traefik + Redis adapter (multi-instances)
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { createAdapter } = require("@socket.io/redis-adapter");
const Redis = require("ioredis");

const { PORT } = require("./config");
const store = require("./store");
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
  "https://socket-auctav.astucom.com",
  "http://localhost",
  "http://127.0.0.1",
];

// ─────────────────────────────────────────────────────────────
// RATE LIMITING PAR IP
//
// NOTE clustering : connPerIP reste un Map LOCAL à chaque instance.
// Un même client (même IP) peut donc ouvrir jusqu'à MAX_CONN connexions
// PAR INSTANCE (donc potentiellement 4x MAX_CONN au total réparties sur
// app1..app4), pas MAX_CONN au global. Avec le sticky session, un même
// navigateur reste normalement collé à la même instance donc l'effet est
// limité en pratique, mais ce n'est plus une vraie limite globale par IP.
// Si tu veux une limite stricte globale, il faudrait migrer connPerIP vers
// un compteur Redis partagé (INCR/DECR par IP), sur le même principe que
// store.js.
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

// ─────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────

app.get("/", async (_req, res) => {
  res.json({
    status: "ok",
    instance: process.env.INSTANCE_ID || "unknown",
    uptime: process.uptime(),
    // rooms   : await getRoomStats(),
    memory: process.memoryUsage(),
    sockets: await store.size(),
    connPerIP: connPerIP.size, // utile pour monitorer les IPs actives sur CETTE instance
  });
});

// Followers debug
app.get("/follow/:room", async (req, res) => {
  res.json({
    room: req.params.room,
    followers: await getFollowersInRoom(req.params.room),
  });
});

// Screens debug
app.get("/screen/:room", async (req, res) => {
  res.json({
    room: req.params.room,
    screens: await getScreensInRoom(req.params.room),
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
// REDIS ADAPTER — propagation des events entre app1..app4
//
// Sans cet adapter, io.to(room).emit(...) et io.emit(...) ne touchent QUE
// les clients connectés à l'instance courante. Avec l'adapter, chaque
// instance publie/écoute sur un canal Redis pub/sub partagé : un event émis
// sur app1 est bien reçu par les clients connectés sur app2, app3, app4.
//
// Ce mécanisme est indépendant de store.js (qui partage les métadonnées) —
// les deux sont nécessaires pour un clustering correct.
// ─────────────────────────────────────────────────────────────

const pubClient = new Redis(process.env.REDIS_URL || "redis://redis:6379");
const subClient = pubClient.duplicate();

pubClient.on("error", (err) =>
  log(`[REDIS ADAPTER] Erreur pubClient : ${err.message}`),
);
subClient.on("error", (err) =>
  log(`[REDIS ADAPTER] Erreur subClient : ${err.message}`),
);

io.adapter(createAdapter(pubClient, subClient));

log(
  `[REDIS ADAPTER] Adapter Socket.IO branché sur ${process.env.REDIS_URL || "redis://redis:6379"}`,
);

// ─────────────────────────────────────────────────────────────

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

io.on("connection", async (socket) => {
  log(`+ Connexion : ${socket.id}`);

  await store.set(socket.id, {
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
  log(
    `Mode : PRODUCTION (clusterisé, instance=${process.env.INSTANCE_ID || "unknown"})`,
  );
  log(`Health : http://localhost:${PORT}/`);
});

// ─────────────────────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────

async function shutdown(signal) {
  log(`${signal} reçu — arrêt propre`);
  server.close(async () => {
    try {
      await store.close();
      await pubClient.quit();
      await subClient.quit();
    } catch (err) {
      log(`[SHUTDOWN] Erreur fermeture Redis : ${err.message}`);
    }
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ─────────────────────────────────────────────────────────────
// PROTECTION GLOBALE CONTRE LES CRASHS
// Ajout de process.exit(1) — laisse Docker (restart: unless-stopped)
// redémarrer proprement plutôt que continuer dans un état corrompu
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
