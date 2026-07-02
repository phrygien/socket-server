// tests/integration/admin.test.js
// ─── Tests d'intégration : flux Admin ────────────────────────────────────────

const socketMeta = require('../../store');
const { createTestServer, connectClient, disconnectClient, waitFor } = require('../helpers/createServer');

let server, url;

beforeAll(async () => {
  server = await createTestServer();
  url    = server.url;
});

afterAll(() => server.stop());

afterEach(async () => {
  // Déconnecter tous les clients restants entre les tests
  await server.io.fetchSockets().then((sockets) =>
    Promise.all(sockets.map((s) => s.disconnect(true)))
  );
  socketMeta.clear();
});

// ─── Identification admin ─────────────────────────────────────────────────────

describe('événement "admin"', () => {
  test('marque le socket comme admin dans le store', async () => {
    const client = await connectClient(url);
    client.emit('admin', 'SuperAdmin');
    await new Promise((r) => setTimeout(r, 100));

    const meta = [...socketMeta.values()].find((m) => m.pseudo === 'SuperAdmin');
    expect(meta).toBeDefined();
    expect(meta.isAdmin).toBe(true);

    await disconnectClient(client);
  });

  test('utilise "Admin" comme pseudo par défaut si aucun pseudo fourni', async () => {
    const client = await connectClient(url);
    client.emit('admin', '');
    await new Promise((r) => setTimeout(r, 100));

    const meta = [...socketMeta.values()].find((m) => m.pseudo === 'Admin');
    expect(meta?.isAdmin).toBe(true);

    await disconnectClient(client);
  });
});

// ─── joinroom admin ───────────────────────────────────────────────────────────

describe('joinroom — admin', () => {
  test('broadcast userList({admin: socketId}) à la salle quand l\'admin rejoint', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    bidder.emit('joinroom', 'auctav42');
    await new Promise((r) => setTimeout(r, 100));

    // On écoute APRÈS que le bidder est dans la salle,
    // pour capturer uniquement le broadcast dû au joinroom de l'admin
    const userListPromise = waitFor(bidder, 'userList');
    admin.emit('joinroom', 'auctav42');

    const data = await userListPromise;
    expect(data.admin).toBeDefined();
    expect(typeof data.admin).toBe('string');

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });

  test('broadcast userList({admin: null}) si l\'admin quitte la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav99');
    bidder.emit('joinroom', 'auctav99');
    await new Promise((r) => setTimeout(r, 100));

    const userListPromise = waitFor(bidder, 'userList');
    await disconnectClient(admin);

    const data = await userListPromise;
    expect(data.admin).toBeNull();

    await disconnectClient(bidder);
  });
});
