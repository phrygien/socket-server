// tests/integration/messaging.test.js
// ─── Tests d'intégration : getMsgRoom & getMsgPrivate ────────────────────────

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

// ─── getMsgRoom ───────────────────────────────────────────────────────────────

describe('événement "getMsgRoom"', () => {
  test('diffuse sendMsg à tous les membres de la salle (émetteur inclus)', async () => {
    const admin  = await connectClient(url);
    const bidder = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav10');
    bidder.emit('joinroom', 'auctav10');
    await new Promise((r) => setTimeout(r, 100));

    const adminMsg  = waitFor(admin,  'sendMsg');
    const bidderMsg = waitFor(bidder, 'sendMsg');

    admin.emit('getMsgRoom', {
      room: 'auctav10',
      type: 'numLot',
      msg : { numLot: '5', nom: 'Épona' },
      name: 'Admin'
    });

    const [a, b] = await Promise.all([adminMsg, bidderMsg]);
    expect(a.type).toBe('numLot');
    expect(b.type).toBe('numLot');
    expect(a.msg.nom).toBe('Épona');

    await disconnectClient(admin);
    await disconnectClient(bidder);
  });

  test('n\'émet rien si data.room est absent', async () => {
    const admin = await connectClient(url);
    admin.emit('joinroom', 'auctav10');
    await new Promise((r) => setTimeout(r, 100));

    let received = false;
    admin.on('sendMsg', () => { received = true; });
    admin.emit('getMsgRoom', { type: 'numLot', msg: {} }); // pas de room
    await new Promise((r) => setTimeout(r, 150));

    expect(received).toBe(false);
    await disconnectClient(admin);
  });

  test('diffuse closeEnchere (→ results.php) correctement', async () => {
    const admin   = await connectClient(url);
    const results = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav11');
    results.emit('joinroom', 'auctav11');
    results.emit('username', 'RESULTS');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(results, 'sendMsg');
    admin.emit('getMsgRoom', {
      room: 'auctav11',
      type: 'closeEnchere',
      msg : { numLot: '3', statut: 'sold', price: 12000, toid: null }
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('closeEnchere');
    expect(msg.msg.statut).toBe('sold');
    expect(msg.msg.price).toBe(12000);

    await disconnectClient(admin);
    await disconnectClient(results);
  });

  test('diffuse updateLot (→ results.php) correctement', async () => {
    const admin   = await connectClient(url);
    const results = await connectClient(url);

    admin.emit('admin', 'Admin');
    admin.emit('joinroom', 'auctav12');
    results.emit('joinroom', 'auctav12');
    await new Promise((r) => setTimeout(r, 100));

    const msgPromise = waitFor(results, 'sendMsg');
    admin.emit('getMsgRoom', {
      room: 'auctav12',
      type: 'updateLot',
      msg : { numLot: '7', statut: 'inprogress', price: 8000, toid: 'online' }
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('updateLot');
    expect(msg.msg.numLot).toBe('7');

    await disconnectClient(admin);
    await disconnectClient(results);
  });
});

// ─── getMsgPrivate ────────────────────────────────────────────────────────────

describe('événement "getMsgPrivate"', () => {
  test('envoie sendMsg uniquement au destinataire ciblé', async () => {
    const sender    = await connectClient(url);
    const recipient = await connectClient(url);
    const other     = await connectClient(url);

    sender.emit('joinroom', 'auctav20');
    recipient.emit('joinroom', 'auctav20');
    other.emit('joinroom', 'auctav20');
    await new Promise((r) => setTimeout(r, 100));

    const recipientId = [...socketMeta.entries()]
      .find(([, m]) => m.room === 'auctav20' && m !== socketMeta.get(sender.id))
      ?.[0];

    let otherReceived = false;
    other.on('sendMsg', () => { otherReceived = true; });

    const msgPromise = waitFor(recipient, 'sendMsg');
    sender.emit('getMsgPrivate', {
      toid: recipient.id,
      type: 'confirmEnchere',
      msg : { lot: '4', price: 3000 },
      name: 'Admin'
    });

    const msg = await msgPromise;
    expect(msg.type).toBe('confirmEnchere');
    expect(msg.msg.lot).toBe('4');

    await new Promise((r) => setTimeout(r, 100));
    expect(otherReceived).toBe(false);

    await disconnectClient(sender);
    await disconnectClient(recipient);
    await disconnectClient(other);
  });

  test('n\'émet rien si toid est absent', async () => {
    const client = await connectClient(url);
    client.emit('joinroom', 'auctav20');
    await new Promise((r) => setTimeout(r, 100));

    let received = false;
    client.on('sendMsg', () => { received = true; });
    client.emit('getMsgPrivate', { type: 'confirmEnchere', msg: {} }); // pas de toid
    await new Promise((r) => setTimeout(r, 150));

    expect(received).toBe(false);
    await disconnectClient(client);
  });
});
