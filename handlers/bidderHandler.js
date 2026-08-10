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
//
// FIX (audit complémentaire au bug "enchère validée puis refusée") :
//   Deux autres events étaient écoutés en double sur le même socket,
//   exactement le même pattern que le bug 'getMsgPrivate' déjà corrigé :
//
//   1. 'username' était aussi écouté dans roomHandler.js.
//      Le client émet 'joinroom' puis 'username' juste après (voir
//      entete_cheval.php / vente_list.php) → les deux handlers tournaient
//      en parallèle, chacun avec son propre store.get()/store.set() lu à
//      quelques ms d'écart → risque de race condition sur le Hash Redis
//      partagé (l'un pouvant écraser une mutation de l'autre pas encore
//      persistée). Supprimé ici : la logique canonique 'username' vit
//      désormais uniquement dans roomHandler.js (qui trim + valide + met
//      à jour le buffer d'historique + notifie l'admin courant).
//
//   2. 'follow' était aussi écouté dans followHandler.js, avec un code
//      quasi identique → chaque heartbeat déclenchait DEUX 'sendMsg' vers
//      la salle. Supprimé ici : la logique canonique 'follow' vit
//      désormais uniquement dans followHandler.js.

const store = require("../store");
const { log } = require("../utils/logger");
const { getAdminOfRoom } = require("../services/roomService");
const { updateSaleEndTimer } = require("../services/saleEndService");

function registerBidderHandler(io, socket) {
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
   *
   * NOTE : ceci est désormais le SEUL listener 'getMsgPrivate' enregistré
   * (messageHandler.js a été vidé pour éviter le doublon — voir son en-tête).
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