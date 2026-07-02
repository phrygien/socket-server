// tests/unit/roomService.test.js
// ─── Tests unitaires : roomService ───────────────────────────────────────────

const socketMeta = require('../../store');
const { getAdminOfRoom, broadcastUserList, getRoomStats } = require('../../services/roomService');

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Fabrique un io mock minimaliste. */
const mockIo = () => {
  const emitted = [];
  return {
    to: (room) => ({
      emit: (event, data) => emitted.push({ room, event, data })
    }),
    emitted
  };
};

// ─── Setup / teardown ─────────────────────────────────────────────────────────

beforeEach(() => socketMeta.clear());
afterEach(()  => socketMeta.clear());

// ─── getAdminOfRoom ───────────────────────────────────────────────────────────

describe('getAdminOfRoom', () => {
  test('retourne null si aucun socket dans la salle', () => {
    expect(getAdminOfRoom('auctav1')).toBeNull();
  });

  test('retourne null si la salle existe mais sans admin', () => {
    socketMeta.set('bidder1', { pseudo: 'Alice', room: 'auctav1', isAdmin: false });
    expect(getAdminOfRoom('auctav1')).toBeNull();
  });

  test('retourne le socketId de l\'admin de la salle', () => {
    socketMeta.set('admin1', { pseudo: 'Admin', room: 'auctav1', isAdmin: true });
    socketMeta.set('bidder1', { pseudo: 'Alice', room: 'auctav1', isAdmin: false });
    expect(getAdminOfRoom('auctav1')).toBe('admin1');
  });

  test('ne retourne pas l\'admin d\'une autre salle', () => {
    socketMeta.set('admin1', { pseudo: 'Admin', room: 'auctav2', isAdmin: true });
    expect(getAdminOfRoom('auctav1')).toBeNull();
  });

  test('retourne le premier admin si plusieurs sont présents', () => {
    socketMeta.set('admin1', { pseudo: 'Admin1', room: 'auctav1', isAdmin: true });
    socketMeta.set('admin2', { pseudo: 'Admin2', room: 'auctav1', isAdmin: true });
    const result = getAdminOfRoom('auctav1');
    expect(['admin1', 'admin2']).toContain(result);
  });
});

// ─── broadcastUserList ────────────────────────────────────────────────────────

describe('broadcastUserList', () => {
  test('émet userList({ admin: null }) si aucun admin dans la salle', () => {
    const io = mockIo();
    broadcastUserList(io, 'auctav1');
    expect(io.emitted).toHaveLength(1);
    expect(io.emitted[0]).toMatchObject({
      room  : 'auctav1',
      event : 'userList',
      data  : { admin: null }
    });
  });

  test('émet userList({ admin: socketId }) si un admin est présent', () => {
    socketMeta.set('admin1', { pseudo: 'Admin', room: 'auctav1', isAdmin: true });
    const io = mockIo();
    broadcastUserList(io, 'auctav1');
    expect(io.emitted[0].data).toEqual({ admin: 'admin1' });
  });

  test('cible bien la salle passée en paramètre', () => {
    const io = mockIo();
    broadcastUserList(io, 'auctav_follow');
    expect(io.emitted[0].room).toBe('auctav_follow');
  });
});

// ─── getRoomStats ─────────────────────────────────────────────────────────────

describe('getRoomStats', () => {
  test('retourne un objet vide si aucun socket', () => {
    expect(getRoomStats()).toEqual({});
  });

  test('compte correctement les sockets par salle', () => {
    socketMeta.set('s1', { pseudo: 'Admin', room: 'auctav1', isAdmin: true });
    socketMeta.set('s2', { pseudo: 'Alice', room: 'auctav1', isAdmin: false });
    socketMeta.set('s3', { pseudo: 'Bob',   room: 'auctav2', isAdmin: false });

    const stats = getRoomStats();
    expect(stats['auctav1'].count).toBe(2);
    expect(stats['auctav1'].admins).toContain('Admin');
    expect(stats['auctav2'].count).toBe(1);
    expect(stats['auctav2'].admins).toHaveLength(0);
  });

  test('classe les sockets sans salle sous la clé "none"', () => {
    socketMeta.set('s1', { pseudo: 'Ghost', room: null, isAdmin: false });
    const stats = getRoomStats();
    expect(stats['none'].count).toBe(1);
  });
});
