// ─── Logger ───────────────────────────────────────────────────────────────────

function log(msg, ...args) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`[${ts}] ${msg}`, ...args);
}

module.exports = { log };
