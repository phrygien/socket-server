// ─── Bidder Handler ───────────────────────────────────────────────────────────
//
// Adaptation clustering (Redis store) :
//   store.get() retourne un objet désérialisé à chaque appel — le muter
//   localement (meta.pseudo = ..., meta.room = ...) n'a aucun effet sur
//   Redis. Chaque changement d'état est désormais persisté explicitement
//   via store.set(socketId, meta) après modification.
//
//   updateSaleEndTimer n'utilise pas le store ici — laissé synchrone/tel
//   quel, à vérifier si saleEndService.js consomme lui aussi le store et
//   nécessiterait le même traitement.

const store = require("../store");
const { log } = require("../utils/logger");
const { getAdminOfRoom } = require("../services/roomService");
const { updateSaleEndTimer } = require("../services/saleEndService");

function registerBidderHandler(io, socket) {
  /**
   * Identification du bidder.
   * socket.emit('username', pseudo)
   * Répond immédiatement avec l'ID de l'admin de la salle.
   */
  socket.on("username", async (pseudo) => {
    const meta = await store.get(socket.id);
    if (meta) {
      meta.pseudo = pseudo || "Bidder";
      await store.set(socket.id, meta);
    }

    log(`  [username] : ${socket.id} → "${pseudo}"`);

    const room = meta?.room;
    if (room) {
      const adminId = await getAdminOfRoom(room);
      socket.emit("userList", { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || "none"}`);
    }
  });

  /**
   * Connexion initiale du bidder.
   * socket.emit('connected', { name, email, room })
   */
  socket.on("connected", async (data) => {
    const meta = await store.get(socket.id);

    if (meta && data) {
      meta.pseudo = data.name || meta.pseudo;
      meta.email = data.email || "";
      if (data.room) {
        socket.join(data.room);
        meta.room = data.room;
      }
      await store.set(socket.id, meta);
    }

    const room = meta?.room;
    if (!room) return;

    log(
      `  [connected]: ${socket.id} "${data?.name}" (${data?.email}) → ${room}`,
    );

    io.to(room).emit("sendMsg", {
      type: "connected",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Reconnexion d'un bidder (changement de device / rechargement).
   * socket.emit('reconnection', { name, email, room })
   */
  socket.on("reconnection", async (data) => {
    const meta = await store.get(socket.id);

    if (meta && data) {
      meta.pseudo = data.name || meta.pseudo;
      meta.email = data.email || "";
      if (data.room) {
        socket.join(data.room);
        meta.room = data.room;
      }
      await store.set(socket.id, meta);
    }

    const room = meta?.room;
    if (!room) return;

    log(
      `  [reconnect]: ${socket.id} "${data?.name}" (${data?.email}) → ${room}`,
    );

    io.to(room).emit("sendMsg", {
      type: "reconnection",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Demande de la liste des lots (bidder vient de charger la page).
   * Émis par vente_list.php  : socket.emit('getEncheresList', { room })
   * Émis par switcher_list.php côté bidder : type reçu 'getEncheres'
   */
  socket.on("getEncheresList", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room || data?.room;
    if (!room) return;

    log(`  [getList]  : ${socket.id} → ${room}`);

    io.to(room).emit("sendMsg", {
      type: "getEncheresList",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Quand un bidder se connecte sur une vente de type "list",
   * L'admin (switcher_list.php) reçoit ce sendMsg et répond
   * en privé avec getMsgPrivate({ type: 'numLot', … }).
   */
  socket.on("getEncheres", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room || data?.room;
    if (!room) return;

    log(`  [getEnch]  : ${socket.id} → ${room}`);

    io.to(room).emit("sendMsg", {
      type: "getEncheres",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Enchère placée par un bidder.
   * socket.emit('doEncheres', { lot, myEnchere, room })
   */
  socket.on("doEncheres", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room || data?.room;
    if (!room) return;

    log(
      `  [enchère]  : ${socket.id} lot=${data?.lot} montant=${data?.myEnchere}`,
    );

    io.to(room).emit("sendMsg", {
      type: "doEncheres",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  /**
   * Vérification de présence (heartbeat).
   * socket.emit('follow', data)
   */
  socket.on("follow", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room;
    if (!room) return;

    io.to(room).emit("sendMsg", {
      type: "follow",
      msg: data || {},
      name: meta?.pseudo || "unknown",
      from: socket.id,
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // INTERCEPTION sendMsg → mise à jour timer de fin de vente
  // Capture tous les messages qui transitent via getMsgPrivate (pattern
  // existant dans vente.php : socket.emit('getMsgPrivate', {toid, type, msg})
  // ─────────────────────────────────────────────────────────────────────────

  /**
   * Message privé admin ↔ bidder.
   * Utilisé par vente.php pour router les messages vers l'admin ou un bidder.
   * socket.emit('getMsgPrivate', { toid, type, msg, name })
   *
   * C'est ici que transitent les numLot et listLot envoyés par l'admin
   * → on en profite pour mettre à jour le timer de fin de vente.
   */
  socket.on("getMsgPrivate", async (data) => {
    if (!data) return;

    const { toid, type, msg, name } = data;
    const meta = await store.get(socket.id);
    const room = meta?.room;

    log(`  [getMsgPrv]: from=${socket.id} to=${toid || "room"} type=${type}`);

    // ── Mise à jour timer de fin de vente ──────────────────────────────────

    // Cas 1 : un seul lot mis à jour (ex: l'admin ouvre/met à jour un lot)
    if (type === "numLot" && msg?.time > 0 && room) {
      log(
        `  [saleEnd]  : numLot lot=${msg.numLot} time=${msg.time}s room=${room}`,
      );
      updateSaleEndTimer(io, room, msg.time);
    }

    // Cas 2 : liste complète des lots envoyée à un bidder qui reconnecte
    // msg.list est un tableau de lots, chacun avec un champ time
    if (type === "listLot" && Array.isArray(msg?.list) && room) {
      const maxTime = msg.list.reduce((max, lot) => {
        return lot?.time > max ? lot.time : max;
      }, 0);

      if (maxTime > 0) {
        log(`  [saleEnd]  : listLot maxTime=${maxTime}s room=${room}`);
        updateSaleEndTimer(io, room, maxTime);
      }
    }

    // ── FIX : confirmEnchere — s'assurer que price est transmis au bidder ──
    //
    // L'admin envoie confirmEnchere via getMsgPrivate vers le socket du bidder.
    // Si le payload msg contient myEnchere (le montant validé) mais pas price,
    // on le mappe ici pour que le client puisse mettre à jour son affichage
    // sans attendre le broadcast numLot suivant.
    //
    // Structure attendue côté admin :
    //   getMsgPrivate({ toid: bidderSocketId, type: 'confirmEnchere',
    //                   msg: { lot, state, price, manuel } })
    //
    // Si price est absent mais myEnchere est présent (ancien format admin),
    // on le copie dans price pour assurer la compatibilité ascendante.
    if (type === "confirmEnchere" && msg) {
      if (msg.price === undefined && msg.myEnchere !== undefined) {
        msg.price = msg.myEnchere;
        log(
          `  [confirmEnchere] price absent → copié depuis myEnchere=${msg.price} lot=${msg.lot}`,
        );
      }
    }
    // ──────────────────────────────────────────────────────────────────────

    // ── Routage du message ─────────────────────────────────────────────────

    // Destinataire précis → message privé
    if (toid) {
      io.to(toid).emit("sendMsg", {
        type,
        msg: msg || {},
        name: name || meta?.pseudo || "unknown",
        from: socket.id,
      });
      return;
    }

    // Pas de destinataire → broadcast à toute la salle
    if (room) {
      io.to(room).emit("sendMsg", {
        type,
        msg: msg || {},
        name: name || meta?.pseudo || "unknown",
        from: socket.id,
      });
    }
  });
}

module.exports = { registerBidderHandler };
