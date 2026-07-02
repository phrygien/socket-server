// ─── Message Handler ──────────────────────────────────────────────────────────
const store = require("../store");
const { log } = require("../utils/logger");

function registerMessageHandler(io, socket) {
  /**
   * Message privé ciblé vers un socket précis.
   * data = { toid, type, msg, name }
   *
   * Types envoyés par le bidder (vente_list.php) → admin :
   *   reconnection  { room, email }        bidder rechargé / changement device
   *   doEncheres    { room, myEnchere, lot, email }  enchère (mode live)
   *   exit          { room, email }        bidder quitte volontairement
   *   connected     { room, email }        réponse au heartbeat 'follow' de l'admin
   *
   * Types envoyés par l'admin → bidder :
   *   confirmEnchere  { lot, state, manuel }   enchère acceptée ou refusée
   *   validEnchere    { lot }                  enchère adjugée au bidder
   *   changeDevice    {}                       déconnexion forcée (autre device détecté)
   *   noActivity      {}                       déconnexion forcée (inactivité)
   *   listLot         { … }                    envoi de la liste des lots
   *   follow          {}                       heartbeat admin → demande présence
   *
   * Types envoyés par le follower → admin :
   *   follow          { state: true }          heartbeat toutes les 3 min
   *   getScreen       {}                       demande état du lot courant (screen.php)
   *
   * NOTE clustering : io.to(data.toid).emit(...) cible un socket.id précis,
   * pas une room. L'adapter Redis Socket.IO gère aussi ce cas (chaque socket
   * a implicitement sa propre room nommée d'après son id) — donc ce message
   * privé traverse correctement les instances même si l'émetteur et le
   * destinataire sont sur des instances différentes.
   */
  socket.on("getMsgPrivate", async (data) => {
    if (!data || !data.toid) return;

    const meta = await store.get(socket.id);

    const payload = {
      type: data.type || "",
      msg: data.msg || {},
      name: data.name || meta?.pseudo || "unknown",
      from: socket.id,
    };

    log(`  [private→${data.toid}] type="${data.type}" from=${socket.id}`);

    io.to(data.toid).emit("sendMsg", payload);
  });
}

module.exports = { registerMessageHandler };
