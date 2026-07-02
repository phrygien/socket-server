// ─── Sale End Service ─────────────────────────────────────────────────────────

const { log } = require('../utils/logger');

// Map : room -> { maxTime, updatedAt, intervalId, io }
const roomEndTimers = new Map();

function updateSaleEndTimer(io, room, time) {

    if (!time || time <= 0) return;

    const existing = roomEndTimers.get(room);

    const currentRemainingMs = existing
        ? existing.maxTime - Math.floor((Date.now() - existing.updatedAt) / 1000)
        : 0;

    if (!existing || time > currentRemainingMs) {

        log(`[SALE-END] Nouveau max room=${room} : ${time}s`);

        if (existing?.intervalId) {
            clearInterval(existing.intervalId);
        }

        const entry = {
            maxTime   : time,
            updatedAt : Date.now(),
            io,
            intervalId: null
        };

        entry.intervalId = setInterval(() => {

            const elapsed   = Math.floor((Date.now() - entry.updatedAt) / 1000);
            const remaining = entry.maxTime - elapsed;

            if (remaining <= 0) {
                clearInterval(entry.intervalId);
                roomEndTimers.delete(room);
                log(`[SALE-END] Vente terminée : room=${room}`);
                io.to(room).emit('saleEndTimer', {
                    room,
                    remainingSeconds : 0,
                    ended            : true
                });
                return;
            }

            io.to(room).emit('saleEndTimer', {
                room,
                remainingSeconds : remaining,
                ended            : false
            });

        }, 30000);

        roomEndTimers.set(room, entry);

        // Diffuse immédiatement
        io.to(room).emit('saleEndTimer', {
            room,
            remainingSeconds : time,
            ended            : false
        });
    }
}

function clearSaleEndTimer(room) {
    const entry = roomEndTimers.get(room);
    if (!entry) return;
    clearInterval(entry.intervalId);
    roomEndTimers.delete(room);
    log(`[SALE-END] Timer supprimé : room=${room}`);
}

function getSaleEndRemaining(room) {
    const entry = roomEndTimers.get(room);
    if (!entry) return null;
    const elapsed = Math.floor((Date.now() - entry.updatedAt) / 1000);
    return Math.max(0, entry.maxTime - elapsed);
}

function getActiveTimers() {
    const result = {};
    for (const [room, entry] of roomEndTimers.entries()) {
        const elapsed = Math.floor((Date.now() - entry.updatedAt) / 1000);
        result[room] = {
            maxTime          : entry.maxTime,
            remainingSeconds : Math.max(0, entry.maxTime - elapsed)
        };
    }
    return result;
}

module.exports = {
    updateSaleEndTimer,
    clearSaleEndTimer,
    getSaleEndRemaining,
    getActiveTimers
};