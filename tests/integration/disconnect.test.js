// tests/integration/disconnect.test.js
// ─── Tests d'intégration : déconnexion ───────────────────────────────────────

const socketMeta = require('../../store');
const { createTestServer, connectClient, disconnectClient, waitFor } = require('../helpers/createServer');

let server, url;

beforeAll(async () => {
  server = await createTestServer();
  url    = server.url;
});

afterAll(() => server.stop());

afterEach(async () => {
  await server.io.fetchSockets().then((sockets) =>
    Promise.all(sockets.map((s) => s.disconnect(true)))
  );
  socketMeta.clear();
});

describe('déconnexion', () => {
  test('supprime le socket du store à la déconnexion', async () => {
    const client = await connectClient(url);
    const id = client.id;
    await new Promise((r) => setTimeout(r, 100));

    expect(socketMeta.has(id)).toBe(true);
    await disconnectClient(client);
    await new Promise((r) => setTimeout(r, 100));

    expect(socketMeta.has(id)).toBe(false);
  });

  test('diffuse sendMsg(type:exit) à la salle quand un membre part', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav30');
    bidder.emit('joinroom', 'auctav30');
    bidder.emit('username', 'Exiting');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    await disconnectClient(bidder);

    const msg = await msgPromise;
    expect(msg.type).toBe('exit');
    expect(msg.name).toBe('Exiting');

    await disconnectClient(admin);
  });

  test('broadcast userList({admin: null}) quand l\'admin se déconnecte', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav31');
    bidder.emit('joinroom', 'auctav31');
    await new Promise((r) => setTimeout(r, 100));

    // On attend userList (pas sendMsg exit, qui arrive aussi)
    const userListPromise = waitFor(bidder, 'userList');
    await disconnectClient(admin);

    const data = await userListPromise;
    expect(data.admin).toBeNull();

    await disconnectClient(bidder);
  });

  test('un bidder qui se déconnecte n\'envoie pas userList à la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav32');
    bidder.emit('joinroom', 'auctav32');
    await new Promise((r) => setTimeout(r, 100));

    let userListReceived = false;
    admin.on('userList', () => { userListReceived = true; });

    await disconnectClient(bidder);
    await new Promise((r) => setTimeout(r, 150));

    expect(userListReceived).toBe(false);

    await disconnectClient(admin);
  });
});
