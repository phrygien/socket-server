// ─── Comparaison de chaînes en temps constant ─────────────────────────────────
// Évite les attaques par timing lors de la vérification d'une clé secrète.

const crypto = require("crypto");

function safeEqual(a, b) {
    const ba = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

module.exports = safeEqual;