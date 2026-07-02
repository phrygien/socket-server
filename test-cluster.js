/**
 * test-cluster.js
 * ─────────────────────────────────────────────────────────────────────────
 * Vérifie que le clustering auctav-server (4 instances + Traefik + Redis
 * adapter) fonctionne correctement :
 *
 *   1. Répartition des connexions entre instances (sticky sessions Traefik)
 *      → interroge /  plusieurs fois sans réutiliser de cookie, relève
 *        le champ "instance" retourné par chaque instance.
 *
 *   2. Propagation des events Socket.IO entre instances (Redis adapter)
 *      → ouvre plusieurs clients socket.io indépendants (donc a priori
 *        répartis sur des instances différentes), les fait rejoindre la
 *        même room, puis vérifie qu'un message émis par UN client est bien
 *        reçu par TOUS les autres — même s'ils sont connectés à une
 *        instance différente de l'émetteur.
 *
 * Usage :
 *   node test-cluster.js
 *   node test-cluster.js https://socket-auctav.astucom.com
 *
 * Dépendances : socket.io-client (déjà présent en devDependencies)
 * ─────────────────────────────────────────────────────────────────────────
 */

const https = require("https");
const http = require("http");
const { io } = require("socket.io-client");

const TARGET_URL = process.argv[2] || "https://socket-auctav.astucom.com";
const NB_HEALTH_SAMPLES = 20;
const NB_SOCKET_CLIENTS = 8;
const TEST_ROOM = "test-cluster-room";

// ─────────────────────────────────────────────────────────────────────────
// ÉTAPE 1 — Répartition des connexions HTTP entre instances
// ─────────────────────────────────────────────────────────────────────────

function sampleHealthOnce(url) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib
      .get(url, (res) => {
        let body = "";
        res.on("data", (chunk) => (body += chunk));
        res.on("end", () => {
          try {
            resolve(JSON.parse(body));
          } catch (err) {
            reject(err);
          }
        });
      })
      .on("error", reject);
  });
}

async function testHealthDistribution() {
  console.log(`\n── Étape 1 : distribution des connexions HTTP ──`);
  console.log(`   Cible : ${TARGET_URL}/  (${NB_HEALTH_SAMPLES} requêtes)\n`);

  const tally = {};

  for (let i = 0; i < NB_HEALTH_SAMPLES; i++) {
    try {
      const data = await sampleHealthOnce(`${TARGET_URL}/`);
      const instance = data.instance || "inconnu (champ 'instance' absent)";
      tally[instance] = (tally[instance] || 0) + 1;
    } catch (err) {
      console.error(`   ✗ Requête ${i + 1} échouée : ${err.message}`);
    }
  }

  console.log("   Répartition observée :");
  for (const [instance, count] of Object.entries(tally)) {
    console.log(`     - ${instance} : ${count} requête(s)`);
  }

  const distinctInstances = Object.keys(tally).filter(
    (k) => !k.startsWith("inconnu"),
  ).length;

  if (distinctInstances >= 2) {
    console.log(
      `   ✓ ${distinctInstances} instances différentes détectées — le load balancing fonctionne.\n`,
    );
  } else if (distinctInstances === 1) {
    console.log(
      `   ⚠ Une seule instance détectée sur ${NB_HEALTH_SAMPLES} requêtes — normal si le sticky cookie a été réutilisé par le client HTTP, sinon vérifier Traefik.\n`,
    );
  } else {
    console.log(
      `   ⚠ Champ "instance" absent des réponses — vérifie que server.js expose bien process.env.INSTANCE_ID dans la route '/'.\n`,
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────
// ÉTAPE 2 — Propagation des events Socket.IO entre instances (Redis adapter)
// ─────────────────────────────────────────────────────────────────────────

function createClient(index) {
  return io(TARGET_URL, {
    transports: ["websocket", "polling"],
    forceNew: true, // chaque client = connexion indépendante, pas de cookie partagé
    reconnection: false,
    query: { testClientIndex: index },
  });
}

async function testCrossInstanceBroadcast() {
  console.log(`── Étape 2 : propagation des events (Redis adapter) ──`);
  console.log(
    `   Ouverture de ${NB_SOCKET_CLIENTS} clients indépendants sur la room "${TEST_ROOM}"\n`,
  );

  const clients = [];
  const received = new Set();

  return new Promise((resolve) => {
    let connectedCount = 0;

    for (let i = 0; i < NB_SOCKET_CLIENTS; i++) {
      const client = createClient(i);
      clients.push(client);

      client.on("connect", () => {
        connectedCount++;
        console.log(`   [client ${i}] connecté (socket.id=${client.id})`);

        client.emit("joinroom", TEST_ROOM);
        client.emit("username", `TestClient_${i}`);

        // Écoute les messages diffusés à la room
        client.on("sendMsg", (payload) => {
          if (
            payload?.type === "cluster-test-ping" &&
            payload.from !== client.id
          ) {
            received.add(i);
          }
        });

        // Une fois tous les clients connectés, le client 0 émet le ping
        if (connectedCount === NB_SOCKET_CLIENTS) {
          setTimeout(() => {
            console.log(
              `\n   [client 0] émission du ping de test vers la room...\n`,
            );
            clients[0].emit("getMsgRoom", {
              room: TEST_ROOM,
              type: "message",
              msg: { text: "cluster-test-ping-payload" },
            });

            // Laisse le temps à l'event de se propager (Redis pub/sub)
            setTimeout(() => finishTest(), 3000);
          }, 1000);
        }
      });

      client.on("connect_error", (err) => {
        console.error(`   [client ${i}] erreur de connexion : ${err.message}`);
      });
    }

    function finishTest() {
      console.log(`── Résultats ──`);
      console.log(
        `   Clients ayant reçu le ping : ${received.size} / ${NB_SOCKET_CLIENTS - 1} attendus (tous sauf l'émetteur)\n`,
      );

      for (let i = 1; i < NB_SOCKET_CLIENTS; i++) {
        const status = received.has(i) ? "✓ reçu" : "✗ MANQUANT";
        console.log(`     - client ${i} : ${status}`);
      }

      if (received.size === NB_SOCKET_CLIENTS - 1) {
        console.log(
          `\n   ✓ SUCCÈS — tous les clients ont reçu le message, l'adapter Redis propage bien les events entre instances.\n`,
        );
      } else {
        console.log(
          `\n   ✗ ÉCHEC — certains clients n'ont rien reçu. Vérifier :\n` +
            `     - que l'adapter Redis est bien branché dans server.js (io.adapter(createAdapter(...)))\n` +
            `     - les logs de chaque instance (docker compose logs -f)\n` +
            `     - que REDIS_URL est identique sur les 4 instances\n`,
        );
      }

      clients.forEach((c) => c.disconnect());
      resolve();
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`═══════════════════════════════════════════════════════════`);
  console.log(`  Test de clustering — auctav-server`);
  console.log(`═══════════════════════════════════════════════════════════`);

  await testHealthDistribution();
  await testCrossInstanceBroadcast();

  console.log(`═══════════════════════════════════════════════════════════`);
  console.log(`  Fin des tests.`);
  console.log(`═══════════════════════════════════════════════════════════\n`);

  process.exit(0);
}

main().catch((err) => {
  console.error("Erreur fatale :", err);
  process.exit(1);
});
