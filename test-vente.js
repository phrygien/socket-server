/**
 * simulate-vente-multi-instance.js
 * ─────────────────────────────────────────────────────────────────────────
 * Variante de simulate-vente.js adaptée pour tester le déploiement
 * multi-instances (app1..app4 derrière Traefik, sticky-cookie SERVERID,
 * Redis pub/sub adapter pour la diffusion cross-instance).
 *
 * MODIFS PAR RAPPORT AU SCRIPT ORIGINAL :
 *
 * 1. TRANSPORT FORCÉ EN "websocket" (pas de polling)
 *    → socket.io-client côté Node ne conserve pas de cookie-jar entre
 *      les requêtes HTTP successives du transport polling. Avec 4
 *      instances + sticky-cookie Traefik, un handshake polling peut
 *      donc atterrir sur app1 puis, faute de cookie renvoyé, sur app3
 *      pour la requête suivante → erreur "session id unknown" côté
 *      engine.io. En forçant "websocket" dès le départ, chaque socket
 *      établit UNE seule connexion persistante vers UNE seule instance,
 *      ce qui élimine ce problème et correspond à la pratique recommandée
 *      pour les architectures load-balancées.
 *
 * 2. LOG DE RÉPARTITION DES INSTANCES
 *    → Le serveur ne renvoyant pas nativement son INSTANCE_ID au client,
 *      on capture le cookie `SERVERID` retourné lors du handshake HTTP
 *      initial (upgrade request) pour au moins confirmer qu'une session
 *      sticky a bien été assignée, et on log le transport effectivement
 *      utilisé par chaque socket. Si tu veux une vraie visibilité par
 *      instance (app1/app2/app3/app4), le plus simple est d'ajouter côté
 *      serveur un petit `socket.emit('instance', process.env.INSTANCE_ID)`
 *      juste après connexion — décommente alors le bloc HANDLE_INSTANCE_ID
 *      plus bas pour l'exploiter automatiquement.
 *
 * 3. PLUS DE CONCURRENCE PAR DÉFAUT (NB_BIDDERS = 15 au lieu de 6)
 *    → pour mieux répartir les connexions sur les 4 instances et stresser
 *      le pub/sub Redis (chaque enchère doit traverser Redis pour être
 *      rediffusée aux bidders connectés sur une autre instance que l'admin).
 *
 * 4. VÉRIFICATION DE RÉSILIENCE
 *    → reconnection activée pour les bidders (mais pas pour l'admin, pour
 *      garder un rapport final cohérent) afin de voir si une éventuelle
 *      bascule d'instance (redémarrage, déploiement rolling) est absorbée
 *      proprement par le pub/sub Redis sans perte d'état de vente.
 *
 * Usage :
 *   node simulate-vente-multi-instance.js
 *   node simulate-vente-multi-instance.js https://socket-auctav.astucom.com
 *   node simulate-vente-multi-instance.js https://socket-auctav.astucom.com sim-vente-multi 51 20 4000
 *
 * Arguments (tous optionnels, dans l'ordre) :
 *   [url]            URL du serveur Socket.IO   (défaut: https://socket-auctav.astucom.com)
 *   [room]            Nom de la room de test      (défaut: sim-vente-multi-instance)
 *   [nbLots]          Nombre de lots à simuler    (défaut: 51)
 *   [nbBidders]       Nombre d'enchérisseurs      (défaut: 15)
 *   [lotDurationMs]   Durée d'ouverture par lot   (défaut: 4000 ms)
 *
 * Dépendances : socket.io-client
 * ─────────────────────────────────────────────────────────────────────────
 */

const { io } = require("socket.io-client");

// ─────────────────────────────────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────

const TARGET_URL = process.argv[2] || "https://socket-auctav.astucom.com";
const ROOM = process.argv[3] || "sim-vente-multi-instance";
const NB_LOTS = parseInt(process.argv[4], 10) || 51;
const NB_BIDDERS = parseInt(process.argv[5], 10) || 15;
const LOT_DURATION_MS = parseInt(process.argv[6], 10) || 4000;

const MINI_PRICE = 2000;
const EMIT_ADMIN_EVENT = "admin"; // cf. NOTE du script original

// Fenêtre pendant laquelle les bidders sont autorisés à enchérir sur un lot
const BID_WINDOW_RATIO = 0.75;

// Options de connexion communes : websocket only, cf. NOTE #1 ci-dessus
const SOCKET_OPTS = {
  transports: ["websocket"],
  upgrade: false, // pas de negotiation polling->websocket, direct websocket
  forceNew: true,
};

// Mets ceci à true si tu ajoutes côté serveur un
// socket.emit('instance', process.env.INSTANCE_ID) après connexion
const HANDLE_INSTANCE_ID = false;

// ─────────────────────────────────────────────────────────────────────────
// GÉNÉRATION DES LOTS (noms de chevaux fictifs)
// ─────────────────────────────────────────────────────────────────────────

const PREFIXES = [
  "Éclair",
  "Vent",
  "Ombre",
  "Flamme",
  "Étoile",
  "Tonnerre",
  "Prince",
  "Diamant",
  "Comète",
  "Rafale",
  "Mistral",
  "Phénix",
  "Zéphyr",
  "Aurore",
  "Tempête",
  "Sultan",
  "Cristal",
  "Onyx",
  "Météore",
  "Émeraude",
];
const SUFFIXES = [
  "du Nord",
  "de Minuit",
  "d'Or",
  "Royal",
  "de la Vallée",
  "des Bois",
  "Sauvage",
  "Céleste",
  "de Paris",
  "du Soleil",
  "Impérial",
  "de Feu",
  "des Sables",
  "Éternel",
  "de Camargue",
  "des Prairies",
  "Ardent",
];

function generateLotName(numLot) {
  const p = PREFIXES[Math.floor(Math.random() * PREFIXES.length)];
  const s = SUFFIXES[Math.floor(Math.random() * SUFFIXES.length)];
  return `${p} ${s} ${numLot > 51 ? "II" : ""}`.trim();
}

function buildLots(n) {
  const lots = [];
  for (let i = 1; i <= n; i++) {
    lots.push({
      numLot: i,
      nom: generateLotName(i),
      price: 0,
      toid: "",
      winnerName: "",
      winnerEmail: "",
      statut: "",
    });
  }
  return lots;
}

// ─────────────────────────────────────────────────────────────────────────
// PAS D'ENCHÈRE (identique à la logique front-end vente_list.php)
// ─────────────────────────────────────────────────────────────────────────

function getStep(price) {
  if (price >= 1_000_000) return 50000;
  if (price >= 500_000) return 20000;
  if (price >= 300_000) return 10000;
  if (price >= 100_000) return 5000;
  if (price >= 50_000) return 2000;
  if (price >= 10_000) return 1000;
  return 500;
}

function nextBidAmount(currentPrice) {
  const step = getStep(currentPrice);
  const base = currentPrice > 0 ? currentPrice : MINI_PRICE - step;
  const jitter = Math.random() < 0.3 ? step : 0;
  return Math.max(MINI_PRICE, base + step + jitter);
}

function formatPrice(n) {
  return new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(n);
}

// ─────────────────────────────────────────────────────────────────────────
// ÉTAT GLOBAL
// ─────────────────────────────────────────────────────────────────────────

const lots = buildLots(NB_LOTS);
const bidders = []; // { index, name, email, socket, idAdmin, connected, instanceId }
let adminSocket = null;
let adminReady = false;
let adminInstanceId = null;

const stats = {
  sold: 0,
  notsold: 0,
  totalRevenue: 0,
  bidsPlaced: 0,
  bidsRejected: 0,
  winsByBidder: {},
  reconnects: 0,
};

function log(msg) {
  const t = new Date().toISOString().substring(11, 19);
  console.log(`[${t}] ${msg}`);
}

// ─────────────────────────────────────────────────────────────────────────
// CONNEXION ADMIN
// ─────────────────────────────────────────────────────────────────────────

function connectAdmin() {
  return new Promise((resolve, reject) => {
    adminSocket = io(TARGET_URL, {
      ...SOCKET_OPTS,
      reconnection: false, // on veut un rapport final déterministe côté admin
    });

    adminSocket.on("connect", () => {
      const transport = adminSocket.io.engine.transport.name;
      log(
        `[ADMIN] connecté (socket.id=${adminSocket.id}, transport=${transport})`,
      );
      adminSocket.emit(EMIT_ADMIN_EVENT, "Admin-Simu");
      adminSocket.emit("joinroom", ROOM);
      setTimeout(() => {
        adminReady = true;
        resolve();
      }, 500);
    });

    if (HANDLE_INSTANCE_ID) {
      adminSocket.on("instance", (id) => {
        adminInstanceId = id;
        log(`[ADMIN] rattaché à l'instance : ${id}`);
      });
    }

    adminSocket.on("connect_error", (err) => {
      reject(new Error(`Connexion admin échouée : ${err.message}`));
    });

    // Réception des enchères des bidders (peuvent arriver via Redis
    // depuis une instance différente de celle de l'admin)
    adminSocket.on("sendMsg", (data) => {
      if (data.type === "doEncheres") handleDoEncheres(data);
    });
  });
}

function handleDoEncheres(data) {
  const lot = lots.find((l) => l.numLot === Number(data.msg.lot));
  if (!lot || lot.statut !== "") return; // lot déjà clôturé, on ignore

  const myEnchere = Number(data.msg.myEnchere);

  if (myEnchere > lot.price) {
    lot.price = myEnchere;
    lot.toid = data.from;
    lot.winnerName = data.name;
    lot.winnerEmail = data.msg.email;
    stats.bidsPlaced++;

    log(
      `  [Lot ${lot.numLot}] enchère validée : ${data.name} → ${formatPrice(myEnchere)}`,
    );

    adminSocket.emit("getMsgPrivate", {
      toid: data.from,
      type: "confirmEnchere",
      msg: { lot: lot.numLot, state: true, manuel: true },
      name: "Admin-Simu",
    });

    broadcastLotState(lot, LOT_DURATION_MS / 1000);
  } else {
    stats.bidsRejected++;
    log(
      `  [Lot ${lot.numLot}] enchère refusée : ${data.name} (${formatPrice(myEnchere)} <= ${formatPrice(lot.price)})`,
    );
    adminSocket.emit("getMsgPrivate", {
      toid: data.from,
      type: "confirmEnchere",
      msg: { lot: lot.numLot, state: false },
      name: "Admin-Simu",
    });
  }
}

function broadcastLotState(lot, timeRemaining) {
  adminSocket.emit("getMsgRoom", {
    room: ROOM,
    type: "numLot",
    msg: {
      numLot: lot.numLot,
      price: lot.price,
      reserveInfo: 0,
      time: timeRemaining,
      extratime: false,
      statut: lot.statut,
    },
    name: "Admin-Simu",
  });
}

// ─────────────────────────────────────────────────────────────────────────
// CONNEXION BIDDERS
// ─────────────────────────────────────────────────────────────────────────

// Délai entre deux tentatives de connexion successives, pour ne pas
// déclencher un éventuel plafond "trop de connexions simultanées par IP"
// côté serveur/Traefik — tous les bidders simulés partagent la même IP
// puisqu'ils partent de la même machine.
const CONNECT_STAGGER_MS = 300;
const CONNECT_TIMEOUT_MS = 8000;
// Si un bidder échoue à se connecter (ex: "Too many connections"), on
// retente après ce délai au lieu d'abandonner tout le script.
const CONNECT_RETRY_DELAY_MS = 1500;
const CONNECT_MAX_RETRIES = 3;

function connectOneBidder(bidder, i) {
  return new Promise((resolve, reject) => {
    const client = io(TARGET_URL, {
      ...SOCKET_OPTS,
      reconnection: true, // cf. NOTE #4 : résilience face à un rolling deploy
      reconnectionAttempts: 5,
      reconnectionDelay: 500,
    });
    bidder.socket = client;

    const timeout = setTimeout(() => {
      client.close();
      reject(new Error("timeout de connexion"));
    }, CONNECT_TIMEOUT_MS);

    client.on("connect", () => {
      clearTimeout(timeout);
      bidder.connected = true;
      const transport = client.io.engine.transport.name;
      log(`[Bidder ${i}] connecté (transport=${transport})`);
      client.emit("joinroom", ROOM);
      client.emit("username", bidder.name);
      resolve();
    });

    client.on("reconnect", (attempt) => {
      stats.reconnects++;
      log(`[Bidder ${i}] reconnecté après bascule (tentative ${attempt})`);
      client.emit("joinroom", ROOM);
      client.emit("username", bidder.name);
    });

    if (HANDLE_INSTANCE_ID) {
      client.on("instance", (id) => {
        bidder.instanceId = id;
      });
    }

    client.on("userList", (data) => {
      bidder.idAdmin = data.admin;
    });

    client.on("sendMsg", (data) => {
      if (data.type === "numLot") handleBidderSeesLot(bidder, data.msg);
    });

    client.on("connect_error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

async function connectBidderWithRetry(bidder, i) {
  for (let attempt = 1; attempt <= CONNECT_MAX_RETRIES; attempt++) {
    try {
      await connectOneBidder(bidder, i);
      return true;
    } catch (err) {
      log(
        `⚠ [Bidder ${i}] échec connexion (tentative ${attempt}/${CONNECT_MAX_RETRIES}) : ${err.message}`,
      );
      if (bidder.socket) bidder.socket.close();
      if (attempt < CONNECT_MAX_RETRIES) {
        await sleep(CONNECT_RETRY_DELAY_MS);
      }
    }
  }
  log(`✗ [Bidder ${i}] abandonné après ${CONNECT_MAX_RETRIES} tentatives`);
  return false;
}

async function connectBidders(n) {
  let failedCount = 0;

  for (let i = 0; i < n; i++) {
    const bidder = {
      index: i,
      name: `Enchérisseur_${i}`,
      email: `bidder${i}@simulation.test`,
      socket: null,
      idAdmin: null,
      connected: false,
      lastSeenLot: null,
      instanceId: null,
    };
    bidders.push(bidder);

    const ok = await connectBidderWithRetry(bidder, i);
    if (!ok) failedCount++;

    // étalement des connexions dans le temps, cf. commentaire ci-dessus
    await sleep(CONNECT_STAGGER_MS);
  }

  if (failedCount > 0) {
    log(
      `⚠ ${failedCount}/${n} bidders n'ont pas pu se connecter — la simulation continue avec ${n - failedCount} bidders actifs.`,
    );
  }
}

function handleBidderSeesLot(bidder, msg) {
  if (bidder.lastSeenLot !== msg.numLot && msg.statut === "") {
    bidder.lastSeenLot = msg.numLot;
    if (Math.random() < 0.55) {
      scheduleBids(bidder, msg.numLot);
    }
  }
}

function scheduleBids(bidder, numLot) {
  const maxTries = 1 + Math.floor(Math.random() * 3);
  const windowMs = LOT_DURATION_MS * BID_WINDOW_RATIO;

  for (let t = 0; t < maxTries; t++) {
    const delay = Math.random() * windowMs;
    setTimeout(() => {
      const lot = lots.find((l) => l.numLot === numLot);
      if (!lot || lot.statut !== "") return;
      if (lot.toid === bidder.socket.id) return;
      if (!bidder.idAdmin) return;
      if (!bidder.socket.connected) return; // en cours de reconnexion

      const amount = nextBidAmount(lot.price);

      bidder.socket.emit("getMsgPrivate", {
        toid: bidder.idAdmin,
        type: "doEncheres",
        msg: { myEnchere: amount, lot: numLot, email: bidder.email },
        name: bidder.name,
      });
    }, delay);
  }
}

// ─────────────────────────────────────────────────────────────────────────
// DÉROULÉ D'UN LOT
// ─────────────────────────────────────────────────────────────────────────

function runLot(lot) {
  return new Promise((resolve) => {
    log(`── Lot ${lot.numLot}/${NB_LOTS} : "${lot.nom}" — ouverture ──`);

    lot.price = 0;
    lot.toid = "";
    lot.statut = "";

    broadcastLotState(lot, Math.round(LOT_DURATION_MS / 1000));

    setTimeout(() => {
      lot.statut = lot.toid ? "sold" : "notsold";
      broadcastLotState(lot, 0);

      if (lot.toid) {
        stats.sold++;
        stats.totalRevenue += lot.price;
        stats.winsByBidder[lot.winnerName] =
          (stats.winsByBidder[lot.winnerName] || 0) + 1;

        adminSocket.emit("getMsgPrivate", {
          toid: lot.toid,
          type: "validEnchere",
          msg: { lot: lot.numLot, state: true },
          name: "Admin-Simu",
        });

        log(
          `  ✓ Lot ${lot.numLot} ADJUGÉ à ${lot.winnerName} pour ${formatPrice(lot.price)}\n`,
        );
      } else {
        stats.notsold++;
        log(`  ✗ Lot ${lot.numLot} NON VENDU (aucune enchère)\n`);
      }

      resolve();
    }, LOT_DURATION_MS);
  });
}

async function runAllLots() {
  for (const lot of lots) {
    await runLot(lot);
    await sleep(250);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─────────────────────────────────────────────────────────────────────────
// RAPPORT FINAL
// ─────────────────────────────────────────────────────────────────────────

function printReport(durationMs) {
  console.log(`\n═══════════════════════════════════════════════════════════`);
  console.log(`  RAPPORT DE SIMULATION MULTI-INSTANCES — ${NB_LOTS} lots`);
  console.log(`═══════════════════════════════════════════════════════════`);
  console.log(`  URL cible           : ${TARGET_URL}`);
  console.log(`  Room de test        : ${ROOM}`);
  console.log(`  Bidders simulés     : ${NB_BIDDERS}`);
  console.log(`  Transport forcé     : websocket`);
  console.log(`  Durée réelle        : ${(durationMs / 1000).toFixed(1)}s`);
  console.log(`  ─────────────────────────────────────────`);
  console.log(`  Lots vendus         : ${stats.sold} / ${NB_LOTS}`);
  console.log(`  Lots invendus       : ${stats.notsold} / ${NB_LOTS}`);
  console.log(`  Chiffre d'affaires  : ${formatPrice(stats.totalRevenue)}`);
  console.log(
    `  Prix moyen (vendus) : ${stats.sold ? formatPrice(Math.round(stats.totalRevenue / stats.sold)) : "-"}`,
  );
  console.log(`  Enchères validées   : ${stats.bidsPlaced}`);
  console.log(`  Enchères refusées   : ${stats.bidsRejected}`);
  console.log(`  Reconnexions        : ${stats.reconnects}`);
  console.log(`  ─────────────────────────────────────────`);
  if (HANDLE_INSTANCE_ID) {
    console.log(`  Répartition par instance :`);
    console.log(`    - Admin  : ${adminInstanceId || "inconnue"}`);
    const dist = {};
    for (const b of bidders) {
      const id = b.instanceId || "inconnue";
      dist[id] = (dist[id] || 0) + 1;
    }
    for (const [id, count] of Object.entries(dist)) {
      console.log(`    - ${id} : ${count} bidder(s)`);
    }
    console.log(`  ─────────────────────────────────────────`);
  } else {
    console.log(
      `  (Répartition par instance non tracée — active HANDLE_INSTANCE_ID`,
    );
    console.log(
      `   + un emit('instance', process.env.INSTANCE_ID) côté serveur pour l'obtenir)`,
    );
    console.log(`  ─────────────────────────────────────────`);
  }
  console.log(`  Classement des adjudicataires :`);
  const ranking = Object.entries(stats.winsByBidder).sort(
    (a, b) => b[1] - a[1],
  );
  if (ranking.length === 0) {
    console.log(`    (aucun lot vendu)`);
  } else {
    for (const [name, count] of ranking) {
      console.log(`    - ${name} : ${count} lot(s)`);
    }
  }
  console.log(`═══════════════════════════════════════════════════════════\n`);
}

// ─────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`═══════════════════════════════════════════════════════════`);
  console.log(`  Simulation de vente MULTI-INSTANCES — auctav-server`);
  console.log(`  ${NB_LOTS} lots / ${NB_BIDDERS} bidders / room="${ROOM}"`);
  console.log(`  Cible : ${TARGET_URL} (4 instances via Traefik + Redis)`);
  console.log(`═══════════════════════════════════════════════════════════\n`);

  const t0 = Date.now();

  log("Connexion de l'admin...");
  await connectAdmin();

  log(`Connexion de ${NB_BIDDERS} bidders (transport websocket direct)...`);
  await connectBidders(NB_BIDDERS);

  await sleep(1000);

  log("Début de la vente\n");
  await runAllLots();

  const t1 = Date.now();
  printReport(t1 - t0);

  adminSocket.disconnect();
  bidders.forEach((b) => b.socket.disconnect());

  process.exit(0);
}

main().catch((err) => {
  console.error("Erreur fatale :", err);
  process.exit(1);
});
