// tests/integration/bidder.test.js
// ─── Tests d'intégration : flux Bidder ───────────────────────────────────────

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

// ─── username ─────────────────────────────────────────────────────────────────

describe('événement "username"', () => {
  test('enregistre le pseudo dans le store', async () => {
    const client = await connectClient(url);
    client.emit('joinroom', 'auctav1');
    client.emit('username', 'Alice');
    await new Promise((r) => setTimeout(r, 100));

    const meta = [...socketMeta.values()].find((m) => m.pseudo === 'Alice');
    expect(meta).toBeDefined();

    await disconnectClient(client);
  });

  test('répond userList en privé avec l\'admin de la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav1');
    bidder.emit('joinroom', 'auctav1');
    await new Promise((r) => setTimeout(r, 100));

    const userListPromise = waitFor(bidder, 'userList');
    bidder.emit('username', 'Alice');

    const data = await userListPromise;
    expect(data.admin).not.toBeNull();

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });

  test('répond userList({admin: null}) si pas d\'admin dans la salle', async () => {
    const bidder = await connectClient(url);
    bidder.emit('joinroom', 'auctav_vide');

    const userListPromise = waitFor(bidder, 'userList');
    bidder.emit('username', 'Bob');

    const data = await userListPromise;
    expect(data.admin).toBeNull();

    await disconnectClient(bidder);
  });
});

// ─── doEncheres ───────────────────────────────────────────────────────────────

describe('événement "doEncheres"', () => {
  test('diffuse sendMsg(type:doEncheres) à toute la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav5');
    bidder.emit('joinroom', 'auctav5');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    bidder.emit('doEncheres', { lot: '12', myEnchere: 5000, room: 'auctav5' });

    const msg = await msgPromise;
    expect(msg.type).toBe('doEncheres');
    expect(msg.msg.lot).toBe('12');
    expect(msg.msg.myEnchere).toBe(5000);

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });

  test('n\'émet rien si le bidder n\'est dans aucune salle', async () => {
    const other  = await connectClient(url);
    const bidder = await connectClient(url);
    other.emit('joinroom', 'auctav5');
    await new Promise((r) => setTimeout(r, 100));

    let received = false;
    other.on('sendMsg', () => { received = true; });

    // bidder sans salle, sans room dans data
    bidder.emit('doEncheres', { lot: '12', myEnchere: 5000 });
    await new Promise((r) => setTimeout(r, 150));

    expect(received).toBe(false);

    await disconnectClient(other);
    await disconnectClient(bidder);
  });
});

// ─── connected ────────────────────────────────────────────────────────────────

describe('événement "connected"', () => {
  test('diffuse sendMsg(type:connected) à toute la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav3');
    bidder.emit('joinroom', 'auctav3');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    bidder.emit('connected', { name: 'Charlie', email: 'charlie@test.com', room: 'auctav3' });

    const msg = await msgPromise;
    expect(msg.type).toBe('connected');
    expect(msg.msg.name).toBe('Charlie');
    expect(msg.msg.email).toBe('charlie@test.com');

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });
});

// ─── reconnection ─────────────────────────────────────────────────────────────

describe('événement "reconnection"', () => {
  test('diffuse sendMsg(type:reconnection) à toute la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav7');
    bidder.emit('joinroom', 'auctav7');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    bidder.emit('reconnection', { name: 'Dana', email: 'dana@test.com', room: 'auctav7' });

    const msg = await msgPromise;
    expect(msg.type).toBe('reconnection');
    expect(msg.name).toBe('Dana');

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });
});

// ─── getEncheresList ──────────────────────────────────────────────────────────

describe('événement "getEncheresList"', () => {
  test('diffuse sendMsg(type:getEncheresList) à la salle', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav8');
    bidder.emit('joinroom', 'auctav8');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    bidder.emit('getEncheresList', {});

    const msg = await msgPromise;
    expect(msg.type).toBe('getEncheresList');

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });
});
