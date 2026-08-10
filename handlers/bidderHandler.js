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
//
// FIX (bug "enchère encore saisissable après refresh sur un lot déjà
// vendu") :
//   Le serveur relayait aveuglément toute enchère ('doEncheres', que ce
//   soit émis directement ou routé via 'getMsgPrivate' — c'est ce second
//   chemin qu'utilise réellement le formulaire de vente_list.php) sans
//   jamais vérifier si le lot ciblé était encore ouvert aux enchères.
//   Un refresh de page pendant qu'un lot vient d'être vendu/retiré (ou
//   une requête forgée côté client) pouvait donc faire passer une
//   enchère invalide.
//
//   On consulte désormais l'état du lot actif de la room (persisté par
//   roomHandler.js à chaque 'numLot'/'closeEnchere' admin) AVANT tout
//   relai d'enchère, quel que soit le chemin emprunté. Si le lot ciblé
//   n'est pas le lot actif ou n'est pas ouvert, l'enchère est rejetée et
//   un message 'enchereRefusee' est renvoyé au bidder à la place.

const store = require("../store");
const { log } = require("../utils/logger");
const { getAdminOfRoom } = require("../services/roomService");
const { updateSaleEndTimer } = require("../services/saleEndService");

/**
 * Vérifie que le lot visé par une enchère est bien le lot actif de la room.
 * Retourne { ok: true } si l'enchère peut être relayée, ou
 * { ok: false, lotState } si elle doit être rejetée.
 */
async function checkLotIsBiddable(room, lot) {
  const lotState = await store.getLotState(room);

  const ok =
      !!lotState &&
      lotState.active === true &&
      String(lotState.numLot) === String(lot);

  return { ok, lotState };
}

/**
 * Notifie le bidder que son enchère a été refusée (lot clos/inactif).
 */
function emitEnchereRefusee(socket, lot, lotState) {
  socket.emit("sendMsg", {
    type: "enchereRefusee",
    msg: {
      lot,
      reason: "lot_closed",
      statut: lotState?.statut || null,
    },
    name: "system",
    from: "server",
  });
}

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

    // ── FIX : renvoyer l'état du lot actif au bidder qui se reconnecte ──
    // Complément côté affichage à la validation serveur ci-dessous : le
    // client peut ainsi désactiver son propre formulaire tout de suite,
    // sans attendre un nouvel événement numLot/closeEnchere de l'admin.
    const lotState = await store.getLotState(room);
    if (lotState) {
      socket.emit("sendMsg", {
        type: "lotState",
        msg: lotState,
        name: "system",
        from: "server",
      });
    }
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
   *
   * NOTE : ce chemin direct n'est a priori plus utilisé par vente_list.php
   * (qui passe par 'getMsgPrivate', voir plus bas), mais on applique la
   * même validation ici par sécurité si un autre client l'utilise encore.
   */
  socket.on("doEncheres", async (data) => {
    const meta = await store.get(socket.id);
    const room = meta?.room || data?.room;
    if (!room) return;

    // ── FIX : validation serveur avant relai de l'enchère ──────────────────
    const { ok, lotState } = await checkLotIsBiddable(room, data?.lot);
    if (!ok) {
      log(
          `  [enchère] REFUSÉE (lot clos/inactif) : ${socket.id} lot=${data?.lot} état=${JSON.stringify(lotState)}`,
      );
      emitEnchereRefusee(socket, data?.lot, lotState);
      return;
    }
    // ─────────────────────────────────────────────────────────────────────

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
   * C'est aussi ici que transite l'enchère du bidder envoyée par
   * vente_list.php :
   *   socket.emit("getMsgPrivate",{toid:idAdmin, type:'doEncheres', ...})
   * → c'est donc ICI qu'il faut impérativement valider l'état du lot
   * avant de relayer quoi que ce soit à l'admin (voir FIX plus bas).
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

    // ── FIX : persistance état du lot si numLot/closeEnchere transite ici ──
    // (au cas où l'admin envoie ces types via getMsgPrivate plutôt que via
    // getMsgRoom — on veut que l'état du lot soit fiable quel que soit le
    // chemin utilisé par le client admin)
    if (type === "numLot" && msg?.numLot !== undefined && room) {
      await store.setLotState(room, {
        numLot: msg.numLot,
        active: msg.start === "ok",
        statut: msg.statut || null,
        price: msg.price ?? null,
      });
    }

    if (type === "closeEnchere" && msg?.numLot !== undefined && room) {
      await store.setLotState(room, {
        numLot: msg.numLot,
        active: false,
        statut: msg.statut || "closed",
        price: msg.price ?? null,
      });
    }
    // ──────────────────────────────────────────────────────────────────────

    // ── FIX : validation serveur de l'enchère avant relai à l'admin ────────
    // C'est LE chemin réellement emprunté par le formulaire de
    // vente_list.php. Sans ce contrôle, une enchère sur un lot déjà
    // vendu/retiré était transmise telle quelle à l'admin, qui pouvait
    // par erreur la valider (bug "enchère validée puis refusée" évoqué
    // en tête de fichier), ou simplement afficher une incohérence.
    if (type === "doEncheres") {
      const { ok, lotState } = await checkLotIsBiddable(room, msg?.lot);
      if (!ok) {
        log(
            `  [enchère] REFUSÉE (lot clos/inactif, via getMsgPrivate) : ${socket.id} lot=${msg?.lot} état=${JSON.stringify(lotState)}`,
        );
        emitEnchereRefusee(socket, msg?.lot, lotState);
        return; // on n'envoie rien à l'admin
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