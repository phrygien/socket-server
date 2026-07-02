// tests/integration/follow-screen.test.js
// ─── Tests d'intégration : Follow & Screen ───────────────────────────────────

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

// ─── Flux Follow ──────────────────────────────────────────────────────────────

describe('flux Follow (follow.php)', () => {
  test('le follower reçoit userList({admin}) au joinroom', async () => {
    const admin    = await connectClient(url);
    const follower = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_follow');
    await new Promise((r) => setTimeout(r, 100));

    const userListPromise = waitFor(follower, 'userList');
    follower.emit('joinroom', 'auctav_follow');

    const data = await userListPromise;
    expect(data.admin).not.toBeNull();

    await disconnectClient(admin);
    await disconnectClient(follower);
  });

  test('le follower reçoit userList({admin: null}) si pas d\'admin', async () => {
    const follower = await connectClient(url);

    const userListPromise = waitFor(follower, 'userList');
    follower.emit('joinroom', 'auctav_follow');

    const data = await userListPromise;
    expect(data.admin).toBeNull();

    await disconnectClient(follower);
  });

  test('heartbeat follow → diffusé à toute la salle', async () => {
    const admin    = await connectClient(url);
    const follower = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_follow');
    follower.emit('joinroom', 'auctav_follow');
    follower.emit('username', 'Follow_123');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    follower.emit('follow', { state: true });

    const msg = await msgPromise;
    expect(msg.type).toBe('follow');
    expect(msg.msg.state).toBe(true);

    await disconnectClient(admin);
    await disconnectClient(follower);
  });

  test('le follower reçoit les messages broadcast (type:message)', async () => {
    const admin    = await connectClient(url);
    const follower = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_follow');
    follower.emit('joinroom', 'auctav_follow');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(follower, 'sendMsg');
    admin.emit('getMsgRoom', {
      room: 'auctav_follow',
      type: 'message',
      msg : { text: '<p>Lot 5 adjugé</p>', style: 'ok' }
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('message');
    expect(msg.msg.text).toContain('Lot 5');

    await disconnectClient(admin);
    await disconnectClient(follower);
  });

  test('le follower reçoit la liste des users (type:users)', async () => {
    const admin    = await connectClient(url);
    const follower = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_follow');
    follower.emit('joinroom', 'auctav_follow');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(follower, 'sendMsg');
    admin.emit('getMsgRoom', {
      room: 'auctav_follow',
      type: 'users',
      msg : { text: '<li>Alice</li><li>Bob</li>' }
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('users');
    expect(msg.msg.text).toContain('Alice');

    await disconnectClient(admin);
    await disconnectClient(follower);
  });
});

// ─── Flux Screen ──────────────────────────────────────────────────────────────

describe('flux Screen (screen.php)', () => {
  test('le screen reçoit userList({admin}) au joinroom', async () => {
    const admin  = await connectClient(url);
    const screen = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_screen');
    await new Promise((r) => setTimeout(r, 100));

    const userListPromise = waitFor(screen, 'userList');
    screen.emit('joinroom', 'auctav_screen');

    const data = await userListPromise;
    expect(data.admin).not.toBeNull();

    await disconnectClient(admin);
    await disconnectClient(screen);
  });

  test('le screen reçoit sendMsg(type:numLot) depuis l\'admin', async () => {
    const admin  = await connectClient(url);
    const screen = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_screen');
    screen.emit('joinroom', 'auctav_screen');
    screen.emit('username', 'Screen_123');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(screen, 'sendMsg');
    admin.emit('getMsgRoom', {
      room: 'auctav_screen',
      type: 'numLot',
      msg : {
        numLot       : '8',
        nom          : 'Tornado',
        pere         : 'Stallion',
        mere         : 'Marylin',
        presentateur : 'Haras du Nord',
        infos_suppl  : 'Hongre 4 ans',
        tva          : 100,
        from         : null,
        img          : 'https://www.auctav.com/vignette.jpg',
        prices       : ['12 000 €', '0', '0', '0']
      }
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('numLot');
    expect(msg.msg.nom).toBe('Tornado');
    expect(msg.msg.numLot).toBe('8');
    expect(msg.msg.prices[0]).toBe('12 000 €');

    await disconnectClient(admin);
    await disconnectClient(screen);
  });

  test('le screen reçoit sendMsg(type:previousLot)', async () => {
    const admin  = await connectClient(url);
    const screen = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_screen');
    screen.emit('joinroom', 'auctav_screen');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(screen, 'sendMsg');
    admin.emit('getMsgRoom', {
      room: 'auctav_screen',
      type: 'previousLot',
      msg : { numLot: '7', prices: ['9 000 €', '0', '0', '0'] }
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('previousLot');
    expect(msg.msg.numLot).toBe('7');

    await disconnectClient(admin);
    await disconnectClient(screen);
  });

  test('l\'admin reçoit getScreen quand le screen le demande', async () => {
    const admin  = await connectClient(url);
    const screen = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_screen');
    screen.emit('joinroom', 'auctav_screen');
    screen.emit('username', 'Screen_456');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(admin, 'sendMsg');
    screen.emit('getScreen', {});

    const msg = await msgPromise;
    expect(msg.type).toBe('getScreen');

    await disconnectClient(admin);
    await disconnectClient(screen);
  });

  test('le screen se cache si l\'admin se déconnecte (userList null)', async () => {
    const admin  = await connectClient(url);
    const screen = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav_screen');
    screen.emit('joinroom', 'auctav_screen');
    await new Promise((r) => setTimeout(r, 100));

    const userListPromise = waitFor(screen, 'userList');
    await disconnectClient(admin);

    const data = await userListPromise;
    expect(data.admin).toBeNull();

    await disconnectClient(screen);
  });
});
