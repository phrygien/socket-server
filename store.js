const Redis = require("ioredis");

const redis = new Redis(process.env.REDIS_URL || "redis://redis:6379");

redis.on("error", (err) => {
  console.error("[store.js] Erreur de connexion Redis:", err.message);
});

redis.on("connect", () => {
  console.log("[store.js] Connecté à Redis");
});

const HASH_KEY = "socketMeta";

const store = {
  /**
   * Enregistre ou met à jour les métadonnées d'un socket.
   * Équivalent de map.set(socketId, data)
   */
  async set(socketId, data) {
    await redis.hset(HASH_KEY, socketId, JSON.stringify(data));
  },

  /**
   * Récupère les métadonnées d'un socket.
   * Équivalent de map.get(socketId)
   * @returns {object|undefined}
   */
  async get(socketId) {
    const raw = await redis.hget(HASH_KEY, socketId);
    return raw ? JSON.parse(raw) : undefined;
  },

  /**
   * Vérifie si un socket a des métadonnées enregistrées.
   * Équivalent de map.has(socketId)
   * @returns {boolean}
   */
  async has(socketId) {
    return (await redis.hexists(HASH_KEY, socketId)) === 1;
  },

  /**
   * Supprime les métadonnées d'un socket (à appeler au disconnect).
   * Équivalent de map.delete(socketId)
   */
  async delete(socketId) {
    await redis.hdel(HASH_KEY, socketId);
  },

  /**
   * Retourne toutes les métadonnées enregistrées (toutes instances confondues).
   * Équivalent de [...map.values()]
   * @returns {object[]}
   */
  async values() {
    const all = await redis.hgetall(HASH_KEY);
    return Object.values(all).map((v) => JSON.parse(v));
  },

  /**
   * Retourne toutes les paires [socketId, data] (toutes instances confondues).
   * Équivalent de [...map.entries()]
   * @returns {[string, object][]}
   */
  async entries() {
    const all = await redis.hgetall(HASH_KEY);
    return Object.entries(all).map(([socketId, v]) => [
      socketId,
      JSON.parse(v),
    ]);
  },

  /**
   * Retourne le nombre total de sockets enregistrées.
   * Équivalent de map.size
   * @returns {number}
   */
  async size() {
    return redis.hlen(HASH_KEY);
  },

  /**
   * Retourne toutes les métadonnées des sockets appartenant à une room donnée.
   * Utile pour lister les participants d'une room, même répartis sur
   * plusieurs instances.
   * @param {string} room
   * @returns {object[]}
   */
  async getByRoom(room) {
    const all = await this.values();
    return all.filter((meta) => meta.room === room);
  },

  /**
   * Ferme proprement la connexion Redis (utile pour les tests ou un
   * arrêt gracieux du serveur).
   */
  async close() {
    await redis.quit();
  },
};

module.exports = store;
