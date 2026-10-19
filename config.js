// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3005;

// ── Origines autorisées à ouvrir un socket ───────────────────────────────────
// Surchargeable via ALLOWED_ORIGINS="https://a.com,https://b.com".
// Le joker "*" est toujours ignoré.
const DEFAULT_ORIGINS = [
    "https://www.auctav.com",
    "https://auctav.com",
    "https://socket-auctav.astucom.com",
];

const ALLOWED_ORIGINS = (
    process.env.ALLOWED_ORIGINS
        ? process.env.ALLOWED_ORIGINS.split(",")
        : DEFAULT_ORIGINS
)
    .map((o) => o.trim())
    .filter((o) => o && o !== "*");

// Développement local uniquement — NE PAS activer en production
if (process.env.ALLOW_DEV_ORIGINS === "true") {
    ALLOWED_ORIGINS.push("http://localhost", "http://127.0.0.1");
}

// ── Clé admin ────────────────────────────────────────────────────────────────
// Secret partagé entre ce serveur et switcher.php. Le client doit l'envoyer :
//     socket.emit("admin", pseudo, ADMIN_KEY)
// Sans clé valide, l'event "admin" est refusé. Si ADMIN_KEY est vide, plus
// aucun admin ne peut se connecter (fail closed).
const ADMIN_KEY = process.env.ADMIN_KEY || "";

// ── Routes de debug (/follow/:room, /screen/:room) ───────────────────────────
// Appelées avec l'en-tête  x-debug-key: <DEBUG_KEY>
// Si DEBUG_KEY est vide, ces routes sont désactivées (404).
const DEBUG_KEY = process.env.DEBUG_KEY || "";

module.exports = { PORT, ALLOWED_ORIGINS, ADMIN_KEY, DEBUG_KEY };