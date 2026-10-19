// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3005;

const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',')
  : ['*'];

module.exports = { PORT, ALLOWED_ORIGINS };
