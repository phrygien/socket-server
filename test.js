const { io } = require("socket.io-client");

// URL DE PRODUCTION
const URL = "https://dev.astucom.com:9022/";
const ROOM = "auctav175";
const ADMIN = "Admin-Test-RateLimit";
const MAX_CONNECTIONS_ALLOWED = 5; // Doit correspondre à MAX_CONN du serveur

console.log("=".repeat(70));
console.log("TEST RATE LIMITING - Socket.IO (PRODUCTION)");
console.log("=".repeat(70));
console.log(`URL: ${URL}`);
console.log(`Room: ${ROOM}`);
console.log(`Admin: ${ADMIN}`);
console.log(`Limite serveur: ${MAX_CONNECTIONS_ALLOWED} connexions/IP`);
console.log(`⚠️  Attention: Test sur serveur de production`);
console.log("=".repeat(70));
console.log("");

const CONFIG = {
  miniPrice: 2000,
  baseTime: 60,
  incrementPerLot: 5,
  extraTimeThreshold: 60,
  extraTimeDuration: 59,
  maxTime: 3600,
};

const metrics = {
  startTime: Date.now(),
  connectionsSuccess: 0,
  connectionsBlocked: 0,
  connectionErrors: 0,
  clientsConnected: 0,
  totalAttempts: 0,
};

function getTimestamp() {
  return new Date().toLocaleTimeString();
}

function log(message, type = "INFO") {
  const icons = {
    INFO: "📘",
    ADMIN: "👑",
    CLIENT: "👤",
    SUCCESS: "✅",
    ERROR: "❌",
    WARNING: "⚠️",
    TEST: "🔧",
    RATE: "🚦",
    CONNECT: "🔌",
  };
  console.log(`${icons[type] || "📘"} [${getTimestamp()}] ${message}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================
// ADMIN SOCKET (optionnel - pour tester la salle)
// ============================================
async function createAdminSocket() {
  log("Création du socket Admin...", "ADMIN");

  const adminSocket = io(URL, {
    transports: ["websocket", "polling"],
    reconnection: false,
    timeout: 10000,
    rejectUnauthorized: false, // Pour les certificats self-signed en dev
  });

  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      log("Timeout connexion admin", "WARNING");
      resolve(null);
    }, 5000);

    adminSocket.on("connect", () => {
      clearTimeout(timeout);
      log(`Admin connecté - ID: ${adminSocket.id}`, "ADMIN");
      adminSocket.emit("joinroom", ROOM);
      adminSocket.emit("admin", ADMIN);
      resolve(adminSocket);
    });

    adminSocket.on("connect_error", (err) => {
      clearTimeout(timeout);
      log(`Admin erreur connexion: ${err.message}`, "ERROR");
      resolve(null);
    });
  });
}

// ============================================
// TEST 1: Atteindre la limite de connexions
// ============================================
async function testRateLimit() {
  log("\n" + "=".repeat(50), "TEST");
  log("TEST 1: Atteindre la limite de connexions", "TEST");
  log("=".repeat(50), "TEST");

  const sockets = [];
  const blockedSockets = [];
  const totalAttempts = MAX_CONNECTIONS_ALLOWED + 3;

  log(`Tentative de création de ${totalAttempts} connexions...`, "RATE");
  log(
    `(devrait réussir: ${MAX_CONNECTIONS_ALLOWED}, bloquées: ${totalAttempts - MAX_CONNECTIONS_ALLOWED})`,
    "INFO",
  );

  // Créer des connexions jusqu'à dépasser la limite
  for (let i = 0; i < totalAttempts; i++) {
    metrics.totalAttempts++;

    const testSocket = io(URL, {
      transports: ["websocket", "polling"],
      reconnection: false,
      timeout: 5000,
      rejectUnauthorized: false, // Pour les certificats self-signed
    });

    let wasBlocked = false;
    let connected = false;

    // Promise pour gérer la connexion/erreur
    const connectionResult = new Promise((resolve) => {
      const timeout = setTimeout(() => {
        resolve("timeout");
      }, 3000);

      testSocket.on("connect", () => {
        clearTimeout(timeout);
        connected = true;
        log(
          `Socket ${i} CONNECTE (${metrics.connectionsSuccess + 1}/${MAX_CONNECTIONS_ALLOWED})`,
          "SUCCESS",
        );
        metrics.connectionsSuccess++;
        resolve("success");
      });

      testSocket.on("connect_error", (err) => {
        clearTimeout(timeout);
        if (
          err.message.includes("Too many connections") ||
          err.message.includes("transport error") ||
          err.message.includes("server error")
        ) {
          wasBlocked = true;
          log(`Socket ${i} BLOQUE par rate limit: ${err.message}`, "RATE");
          metrics.connectionsBlocked++;
          blockedSockets.push(testSocket);
          resolve("blocked");
        } else {
          log(`Socket ${i} Erreur: ${err.message}`, "ERROR");
          metrics.connectionErrors++;
          resolve("error");
        }
      });
    });

    await connectionResult;

    if (connected) {
      // Ajouter un petit délai pour que le serveur enregistre la connexion
      await sleep(100);

      // Stocker pour déconnexion ultérieure
      sockets.push(testSocket);

      // Tenter de joindre une room pour voir si la connexion est vraiment fonctionnelle
      testSocket.emit("joinroom", ROOM);
      testSocket.emit("username", `TestUser_${i}`);
      metrics.clientsConnected++;
    } else if (!wasBlocked) {
      testSocket.disconnect();
    }

    // Pause entre les connexions pour éviter de surcharger
    await sleep(500);
  }

  log("\n--- RÉSULTATS TEST 1 ---", "TEST");
  log(`Tentatives totales: ${metrics.totalAttempts}`, "INFO");
  log(
    `Connexions réussies: ${metrics.connectionsSuccess}`,
    metrics.connectionsSuccess === MAX_CONNECTIONS_ALLOWED
      ? "SUCCESS"
      : "WARNING",
  );
  log(
    `Connexions bloquées: ${metrics.connectionsBlocked}`,
    metrics.connectionsBlocked > 0 ? "SUCCESS" : "WARNING",
  );
  log(
    `Erreurs: ${metrics.connectionErrors}`,
    metrics.connectionErrors === 0 ? "SUCCESS" : "WARNING",
  );

  if (
    metrics.connectionsSuccess === MAX_CONNECTIONS_ALLOWED &&
    metrics.connectionsBlocked > 0
  ) {
    log(
      "✅ RATE LIMITING FONCTIONNEL - Les connexions supplémentaires ont été bloquées",
      "SUCCESS",
    );
  } else if (metrics.connectionsSuccess > MAX_CONNECTIONS_ALLOWED) {
    log("❌ RATE LIMITING INEFFICACE - Trop de connexions autorisées", "ERROR");
    log(
      `   Attendu: max ${MAX_CONNECTIONS_ALLOWED}, Reçu: ${metrics.connectionsSuccess}`,
      "ERROR",
    );
  } else if (metrics.connectionsBlocked === 0) {
    log("⚠️ RATE LIMITING NON TESTÉ - Aucune connexion bloquée", "WARNING");
    log("   Possible causes:", "INFO");
    log("   - Le serveur a une limite plus élevée", "INFO");
    log("   - Les IPs sont différentes (proxy, NAT)", "INFO");
    log("   - Le rate limiting n'est pas activé", "INFO");
  }

  return { sockets, blockedSockets };
}

// ============================================
// TEST 2: Vérifier que les connexions existantes restent actives
// ============================================
async function testExistingConnections(sockets) {
  log("\n" + "=".repeat(50), "TEST");
  log("TEST 2: Vérification des connexions actives", "TEST");
  log("=".repeat(50), "TEST");

  let activeCount = 0;
  let responsiveCount = 0;

  for (let i = 0; i < sockets.length; i++) {
    const socket = sockets[i];
    if (socket && socket.connected) {
      activeCount++;
      log(`Socket ${i} toujours connecté`, "SUCCESS");

      // Tester si la socket répond
      let responded = false;
      const testEvent = `test_ping_${i}`;

      socket.once(testEvent, () => {
        responded = true;
      });

      socket.emit("ping", testEvent);
      await sleep(100);

      if (responded) {
        responsiveCount++;
      }
    } else {
      log(`Socket ${i} déconnecté inopinément`, "WARNING");
    }
  }

  log(
    `\nConnexions actives: ${activeCount}/${sockets.length}`,
    activeCount === sockets.length ? "SUCCESS" : "WARNING",
  );
  log(
    `Connexions réactives: ${responsiveCount}/${sockets.length}`,
    responsiveCount === sockets.length ? "SUCCESS" : "WARNING",
  );

  return { activeCount, responsiveCount };
}

// ============================================
// TEST 3: Déconnexion et reconnexion
// ============================================
async function testReconnectionAfterDisconnect(sockets) {
  log("\n" + "=".repeat(50), "TEST");
  log("TEST 3: Libération d'un slot et reconnexion", "TEST");
  log("=".repeat(50), "TEST");

  if (sockets.length === 0) {
    log("Aucune socket à déconnecter", "WARNING");
    return false;
  }

  // Déconnecter la première socket
  const firstSocket = sockets[0];
  log("Déconnexion de la première socket...", "INFO");
  firstSocket.disconnect();
  await sleep(1000);

  // Tenter de créer une nouvelle connexion
  log("Tentative de nouvelle connexion (slot libéré)...", "TEST");

  const newSocket = io(URL, {
    transports: ["websocket", "polling"],
    reconnection: false,
    timeout: 5000,
    rejectUnauthorized: false,
  });

  let reconnected = false;
  let wasBlocked = false;
  let errorMessage = "";

  await new Promise((resolve) => {
    const timeout = setTimeout(() => {
      resolve();
    }, 5000);

    newSocket.on("connect", () => {
      clearTimeout(timeout);
      reconnected = true;
      log("NOUVELLE CONNEXION RÉUSSIE après libération du slot!", "SUCCESS");
      resolve();
    });

    newSocket.on("connect_error", (err) => {
      clearTimeout(timeout);
      errorMessage = err.message;
      if (
        err.message.includes("Too many connections") ||
        err.message.includes("transport error")
      ) {
        wasBlocked = true;
        log("Nouvelle connexion encore bloquée", "WARNING");
      } else {
        log(`Erreur: ${err.message}`, "ERROR");
      }
      resolve();
    });
  });

  if (reconnected) {
    log(
      "✅ RECONNEXION FONCTIONNELLE - Les slots sont bien libérés",
      "SUCCESS",
    );
    newSocket.disconnect();
    return true;
  } else if (wasBlocked) {
    log(
      "❌ RECONNEXION ÉCHOUÉE - Les slots ne sont pas correctement libérés",
      "ERROR",
    );
    log(`   Message d'erreur: ${errorMessage}`, "ERROR");
    return false;
  } else {
    log("⚠️ RECONNEXION INCERTAINE - Vérifier les logs serveur", "WARNING");
    return false;
  }
}

// ============================================
// TEST 4: Test de charge avec messages
// ============================================
async function testLoadWithMessages(sockets) {
  log("\n" + "=".repeat(50), "TEST");
  log("TEST 4: Test de charge avec messages", "TEST");
  log("=".repeat(50), "TEST");

  if (sockets.length === 0) {
    log("Aucune socket disponible pour le test de charge", "WARNING");
    return;
  }

  let messagesSent = 0;
  let messagesReceived = 0;

  // Écouter les messages
  sockets.forEach((socket, idx) => {
    socket.on("sendMsg", () => {
      messagesReceived++;
    });
  });

  // Envoyer des messages depuis chaque socket
  log(`Envoi de messages depuis ${sockets.length} sockets...`, "TEST");

  for (let i = 0; i < sockets.length; i++) {
    const socket = sockets[i];
    if (socket && socket.connected) {
      socket.emit("getMsgRoom", {
        room: ROOM,
        type: "message",
        msg: { text: `Test message from socket ${i}` },
        name: `TestUser_${i}`,
      });
      messagesSent++;
      await sleep(100);
    }
  }

  await sleep(1000);

  log(`Messages envoyés: ${messagesSent}`, "INFO");
  log(`Messages reçus: ${messagesReceived}`, "INFO");

  const success = messagesSent === messagesReceived;
  if (success) {
    log(
      "✅ COMMUNICATION FONCTIONNELLE - Les messages circulent correctement",
      "SUCCESS",
    );
  } else {
    log(
      "⚠️ COMMUNICATION PARTIELLE - Certains messages n'ont pas été reçus",
      "WARNING",
    );
  }

  return success;
}

// ============================================
// RAPPORT FINAL
// ============================================
function generateReport(results) {
  const duration = (Date.now() - metrics.startTime) / 1000;

  console.log("\n");
  console.log("=".repeat(70));
  console.log("RAPPORT FINAL - TEST RATE LIMITING (PRODUCTION)");
  console.log("=".repeat(70));

  console.log("\n--- STATISTIQUES ---");
  console.log(`  Serveur: ${URL}`);
  console.log(
    `  Durée: ${Math.floor(duration / 60)}m ${Math.floor(duration % 60)}s`,
  );
  console.log(`  Connexions réussies: ${metrics.connectionsSuccess}`);
  console.log(`  Connexions bloquées: ${metrics.connectionsBlocked}`);
  console.log(`  Erreurs: ${metrics.connectionErrors}`);
  console.log(`  Limite configurée: ${MAX_CONNECTIONS_ALLOWED}`);

  console.log("\n--- RÉSULTATS DES TESTS ---");

  let finalScore = 100;
  const issues = [];

  // Test 1: Rate limit atteint
  if (
    metrics.connectionsSuccess === MAX_CONNECTIONS_ALLOWED &&
    metrics.connectionsBlocked > 0
  ) {
    console.log("  ✅ TEST 1: Rate limiting fonctionnel");
  } else if (metrics.connectionsSuccess > MAX_CONNECTIONS_ALLOWED) {
    console.log("  ❌ TEST 1: Rate limiting non fonctionnel");
    finalScore -= 40;
    issues.push(
      `Rate limiting ne bloque pas - ${metrics.connectionsSuccess}/${MAX_CONNECTIONS_ALLOWED} connexions autorisées`,
    );
  } else if (metrics.connectionsBlocked === 0) {
    console.log(
      "  ⚠️ TEST 1: Impossible de vérifier - peut-être limite plus haute ou IP différente",
    );
    finalScore -= 20;
    issues.push("Test de rate limiting non concluant");
  } else {
    console.log("  ✅ TEST 1: Rate limiting partiellement fonctionnel");
  }

  // Test 2: Connexions existantes stables
  if (
    results.existingActive === results.socketsLength &&
    results.existingActive > 0
  ) {
    console.log("  ✅ TEST 2: Connexions existantes stables");
  } else if (results.existingActive > 0) {
    console.log("  ⚠️ TEST 2: Certaines connexions se sont déconnectées");
    finalScore -= 15;
    issues.push(
      `${results.socketsLength - results.existingActive} connexions perdues`,
    );
  } else {
    console.log("  ❌ TEST 2: Toutes les connexions ont été perdues");
    finalScore -= 30;
    issues.push("Stabilité des connexions problématique");
  }

  // Test 3: Reconnexion après libération
  if (results.reconnectionSuccess) {
    console.log("  ✅ TEST 3: Reconnexion après libération OK");
  } else {
    console.log("  ❌ TEST 3: Reconnexion après libération échouée");
    finalScore -= 25;
    issues.push("Les slots ne sont pas correctement libérés");
  }

  // Test 4: Communication fonctionnelle
  if (results.communicationSuccess) {
    console.log("  ✅ TEST 4: Communication fonctionnelle");
  } else {
    console.log("  ⚠️ TEST 4: Problèmes de communication détectés");
    finalScore -= 10;
    issues.push("Les messages ne circulent pas correctement");
  }

  console.log(`\n  SCORE FINAL: ${Math.max(0, finalScore)}/100`);

  if (issues.length > 0) {
    console.log("\n--- PROBLÈMES DETECTÉS ---");
    issues.forEach((issue) => console.log(`  - ${issue}`));
  }

  console.log("\n--- RECOMMANDATIONS ---");
  if (metrics.connectionsBlocked === 0 && metrics.connectionsSuccess > 0) {
    console.log(
      "  🔧 Le rate limiting pourrait ne pas être actif ou configuré différemment",
    );
    console.log("  🔧 Vérifier la valeur de MAX_CONN dans config.js");
    console.log(
      "  🔧 Vérifier que le middleware de rate limiting est bien chargé",
    );
  }
  if (results.reconnectionSuccess === false) {
    console.log("  🔧 Vérifier l'événement 'disconnect' dans connPerIP");
    console.log("  🔧 S'assurer que les sockets sont bien retirées de la Map");
  }
  if (results.existingActive < results.socketsLength) {
    console.log("  🔧 Vérifier les timeouts et pingInterval/pingTimeout");
    console.log("  🔧 Augmenter pingTimeout si les connexions sont instables");
  }

  console.log("\n--- NOTE DE SÉCURITÉ ---");
  console.log("  ⚠️ Ce test a été effectué sur un serveur de production");
  console.log("  📊 Les métriques peuvent être consultées sur:");
  console.log(`  📊 Health check: ${URL.replace("/socket.io", "")}`);

  console.log("\n" + "=".repeat(70));
  console.log("FIN DU TEST RATE LIMITING");
  console.log("=".repeat(70));
}

// ============================================
// NETTOYAGE
// ============================================
async function cleanup(sockets, adminSocket = null) {
  log("Nettoyage des connexions...", "INFO");

  if (adminSocket) {
    adminSocket.disconnect();
    log("Admin déconnecté", "INFO");
  }

  for (const socket of sockets) {
    if (socket && socket.connected) {
      socket.disconnect();
    }
  }

  await sleep(500);
  log("Nettoyage terminé", "SUCCESS");
}

// ============================================
// EXÉCUTION PRINCIPALE
// ============================================
async function main() {
  log("🚦 DÉMARRAGE DU TEST DE RATE LIMITING", "TEST");
  log(`🌐 Serveur cible: ${URL}`, "INFO");
  log(`⏰ ${new Date().toLocaleString()}`, "INFO");

  let adminSocket = null;

  try {
    // Optionnel: Créer un admin pour initialiser la room
    adminSocket = await createAdminSocket();
    await sleep(1000);

    // Test 1: Atteindre la limite
    const { sockets, blockedSockets } = await testRateLimit();

    if (sockets.length === 0) {
      log("Aucune connexion établie, arrêt du test", "ERROR");
      await cleanup([], adminSocket);
      process.exit(1);
    }

    // Test 2: Vérifier les connexions existantes
    const { activeCount, responsiveCount } =
      await testExistingConnections(sockets);

    // Test 3: Test de reconnexion
    const reconnectionSuccess = await testReconnectionAfterDisconnect(sockets);

    // Test 4: Test de communication
    const communicationSuccess = await testLoadWithMessages(sockets);

    // Générer rapport
    generateReport({
      existingActive: activeCount,
      responsiveCount: responsiveCount,
      socketsLength: sockets.length,
      reconnectionSuccess: reconnectionSuccess,
      communicationSuccess: communicationSuccess,
    });

    // Nettoyage
    await cleanup(sockets, adminSocket);
  } catch (error) {
    log(`Erreur fatale: ${error.message}`, "ERROR");
    console.error(error.stack);
    await cleanup([], adminSocket);
    process.exit(1);
  }

  setTimeout(() => {
    process.exit(0);
  }, 2000);
}

// Gestion des erreurs non capturées
process.on("uncaughtException", (err) => {
  log(`Exception non capturée: ${err.message}`, "ERROR");
  console.error(err.stack);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  log(`Promesse non gérée: ${reason}`, "ERROR");
  process.exit(1);
});

// Lancer les tests
main();
