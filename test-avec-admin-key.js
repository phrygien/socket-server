/**
 * test-avec-admin-key.js — Test fonctionnel complet du serveur Socket.IO auctav
 * ─────────────────────────────────────────────────────────────────────────
 * Usage :
 *   node test-avec-admin-key.js
 *   node test-avec-admin-key.js https://socket-auctav.astucom.com https://www.auctav.com
 *
 * Variables d'environnement :
 *   ADMIN_KEY  : clé admin (active les tests admin + le scénario de vente live)
 *   DEBUG_KEY  : clé des routes /follow/:room et /screen/:room (optionnel)
 *
 * Options :
 *   --big      : envoie aussi un payload de 11 Mo (doit couper la connexion)
 *
 * SÉCURITÉ PRODUCTION : tout passe par une room unique "test-live-xxxx"
 * (jamais auctav<id> ni auctav_follow), donc aucune vente réelle n'est touchée.
 *
 * Tests :
 *   1. Health HTTP + répartition + détection de redémarrage d'instance
 *   2. Routes debug protégées (404 sans clé)
 *   3. Handshake HTTP polling : CORS + origine valide / invalide
 *   4. WebSocket : sans Origin / mauvaise Origin refusées, bonne Origin acceptée
 *   5. Event "admin" : mauvaise clé, clé non-string, déconnexion après 3 échecs, bonne clé
 *   6. Scénario de vente live complet (admin + 3 enchérisseurs + 1 hors-room) :
 *        userList, connected, getEncheresList → listLot privé, listLot/numLot
 *        en broadcast, doEncheres, confirmEnchere (+ copie myEnchere → price),
 *        isolation privée / par room, gros payload, payloads malformés,
 *        remplacement d'admin, perte d'admin → userList vide
 *   7. Le serveur est toujours vivant et aucune instance n'a redémarré
 * ─────────────────────────────────────────────────────────────────────────
 */

const https = require("https");
const http = require("http");
const { io } = require("socket.io-client");

const argv = process.argv.slice(2);
const args = argv.filter((a) => !a.startsWith("--"));
const flags = new Set(argv.filter((a) => a.startsWith("--")));

const TARGET_URL = args[0] || "https://socket-auctav.astucom.com";
const ORIGIN = args[1] || process.env.ORIGIN || "https://www.auctav.com";
const ADMIN_KEY = process.env.ADMIN_KEY || "";
const DEBUG_KEY = process.env.DEBUG_KEY || "";
const TIMEOUT = 8000;
const T0 = Date.now();

// ─── Reporting ────────────────────────────────────────────────────────────
const stats = { pass: 0, fail: 0, warn: 0, skip: 0 };
const log = (m) => console.log(m);
const title = (t) => log(`\n── ${t} ──`);
const ok = (n, d = "") => (stats.pass++, log(`   ✓ ${n}${d ? " — " + d : ""}`));
const ko = (n, d = "") => (stats.fail++, log(`   ✗ ${n}${d ? " — " + d : ""}`));
const warn = (n, d = "") => (stats.warn++, log(`   ⚠ ${n}${d ? " — " + d : ""}`));
const skip = (n) => (stats.skip++, log(`   ○ ${n}`));
const check = (n, cond, d = "") => (cond ? ok(n, d) : ko(n, d));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function describeErr(err) {
    if (!err) return "erreur inconnue";
    const d = err.description;
    const detail = d && (d.message || d.error?.message || (typeof d === "string" ? d : ""));
    const status = err.context?.statusCode || err.context?.status;
    return [err.message, detail, status && `HTTP ${status}`].filter(Boolean).join(" | ");
}

// ─── HTTP helper ──────────────────────────────────────────────────────────
function httpGet(url, headers = {}) {
    return new Promise((resolve, reject) => {
        const lib = url.startsWith("https") ? https : http;
        const req = lib.get(url, { headers }, (res) => {
            let body = "";
            res.on("data", (c) => (body += c));
            res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body }));
        });
        req.on("error", reject);
        req.setTimeout(TIMEOUT, () => req.destroy(new Error("timeout")));
    });
}

// ─── Client de test : journalise tous les events reçus ────────────────────
class TC {
    constructor(name, { origin = ORIGIN } = {}) {
        this.name = name;
        this.events = [];
        this.socket = io(TARGET_URL, {
            transports: ["websocket"],
            forceNew: true,
            reconnection: false,
            timeout: TIMEOUT,
            extraHeaders: origin ? { Origin: origin } : {},
        });
        this.socket.onAny((ev, ...a) => this.events.push({ ev, data: a[0] }));
        this.socket.on("disconnect", (reason) => this.events.push({ ev: "__disconnect", data: reason }));
    }
    get id() { return this.socket.id; }
    get connected() { return this.socket.connected; }
    mark() { return this.events.length; }
    emit(...a) { this.socket.emit(...a); }
    close() { this.socket.disconnect(); }

    connect(retries = 3) {
        return new Promise((resolve) => {
            const s = this.socket;
            let left = retries;
            const cleanup = () => { s.off("connect", onOk); s.off("connect_error", onErr); };
            const onOk = () => { cleanup(); resolve(true); };
            const onErr = (err) => {
                this.lastErr = err;
                if (left-- > 0 && /Too many|Server error/i.test(err.message)) {
                    setTimeout(() => s.connect(), 500);
                } else { cleanup(); resolve(false); }
            };
            s.on("connect", onOk);
            s.on("connect_error", onErr);
            if (s.connected) onOk();
        });
    }

    async waitFor(ev, pred = () => true, { since = 0, timeout = TIMEOUT } = {}) {
        const end = Date.now() + timeout;
        const safe = (d) => { try { return pred(d); } catch { return false; } };
        for (;;) {
            const e = this.events.slice(since).find((x) => x.ev === ev && safe(x.data));
            if (e) return e.data;
            if (Date.now() >= end) return null;
            await sleep(40);
        }
    }
    msg(type, pred = () => true, opts) {
        return this.waitFor("sendMsg", (d) => d && d.type === type && pred(d), opts);
    }
    async none(ev, pred, ms = 1500, since = 0) {
        return (await this.waitFor(ev, pred, { since, timeout: ms })) === null;
    }
    async ack(event, ...a) {
        return new Promise((resolve) => {
            const t = setTimeout(() => resolve(null), TIMEOUT);
            this.socket.emit(event, ...a, (res) => { clearTimeout(t); resolve(res); });
        });
    }
}

async function openClient(name, opts) {
    const c = new TC(name, opts);
    const connected = await c.connect();
    return { c, connected };
}

// ═════════════════════════════════════════════════════════════════════════
// 1. Health
// ═════════════════════════════════════════════════════════════════════════
async function testHealth() {
    title(`1. Health HTTP (${TARGET_URL}/)`);
    const tally = {};
    let last = null;
    for (let i = 0; i < 20; i++) {
        try {
            const r = await httpGet(`${TARGET_URL}/`);
            last = JSON.parse(r.body);
            tally[last.instance || "inconnu"] = (tally[last.instance || "inconnu"] || 0) + 1;
        } catch (e) { log(`   requête ${i + 1} échouée : ${e.message}`); }
    }
    if (!last) return ko("Serveur joignable");
    ok("Serveur joignable", `status=${last.status}, sockets=${last.sockets}`);
    log("   Répartition : " + Object.entries(tally).map(([k, v]) => `${k}=${v}`).join(", "));
    const n = Object.keys(tally).filter((k) => k !== "inconnu").length;
    n >= 2 ? ok("Load balancing", `${n} instances`) : warn("Une seule instance vue");
}

// ═════════════════════════════════════════════════════════════════════════
// 2. Routes debug
// ═════════════════════════════════════════════════════════════════════════
async function testDebugRoutes() {
    title("2. Routes debug protégées");
    for (const path of ["/follow/test-room", "/screen/test-room"]) {
        const r = await httpGet(`${TARGET_URL}${path}`).catch((e) => ({ status: e.message }));
        check(`${path} sans clé → 404`, r.status === 404, `status=${r.status}`);
        const r2 = await httpGet(`${TARGET_URL}${path}`, { "x-debug-key": "mauvaise" }).catch((e) => ({ status: e.message }));
        check(`${path} mauvaise clé → 404`, r2.status === 404, `status=${r2.status}`);
        if (DEBUG_KEY) {
            const r3 = await httpGet(`${TARGET_URL}${path}`, { "x-debug-key": DEBUG_KEY }).catch((e) => ({ status: e.message }));
            check(`${path} bonne clé → 200`, r3.status === 200, `status=${r3.status}`);
        }
    }
    if (!DEBUG_KEY) skip("Test avec bonne DEBUG_KEY (définis DEBUG_KEY pour l'activer)");
}

// ═════════════════════════════════════════════════════════════════════════
// 3. Handshake HTTP / CORS
// ═════════════════════════════════════════════════════════════════════════
async function testHandshakeHttp() {
    title("3. Handshake HTTP polling + CORS");
    const url = `${TARGET_URL}/socket.io/?EIO=4&transport=polling`;
    try {
        const good = await httpGet(url, { Origin: ORIGIN });
        check("Origin valide → 200", good.status === 200, `status=${good.status}`);
        check("Corps de handshake Engine.IO", good.body.startsWith("0{"), good.body.slice(0, 40));
        check("CORS allow-origin", good.headers["access-control-allow-origin"] === ORIGIN,
            String(good.headers["access-control-allow-origin"]));
        check("CORS allow-credentials", good.headers["access-control-allow-credentials"] === "true",
            String(good.headers["access-control-allow-credentials"]));
        log(`   (cookie sticky : ${good.headers["set-cookie"] ? "présent" : "absent"})`);
    } catch (e) { ko("Handshake valide", e.message); }

    for (const [label, headers] of [
        ["Origin étrangère", { Origin: "https://evil.example.com" }],
        ["Sans Origin", {}],
    ]) {
        try {
            const r = await httpGet(url, headers);
            check(`${label} → refusée`, r.status >= 400, `status=${r.status}`);
        } catch (e) { ko(label, e.message); }
    }
}

// ═════════════════════════════════════════════════════════════════════════
// 4. WebSocket + Origin
// ═════════════════════════════════════════════════════════════════════════
async function testWsOrigin() {
    title("4. WebSocket et Origin");
    for (const [label, origin] of [["sans Origin", null], ["Origin étrangère", "https://evil.example.com"]]) {
        const { c, connected } = await openClient(label, { origin });
        connected ? ko(`${label} refusée`, "connexion ACCEPTÉE") : ok(`${label} refusée`, describeErr(c.lastErr));
        c.close();
    }
    const { c, connected } = await openClient("valide");
    connected
        ? ok("Origin valide acceptée", `id=${c.id}`)
        : ko("Origin valide acceptée", describeErr(c.lastErr) + " (ALLOWED_ORIGINS ?)");
    c.close();
}

// ═════════════════════════════════════════════════════════════════════════
// 5. Event admin
// ═════════════════════════════════════════════════════════════════════════
async function testAdminEvent() {
    title("5. Event \"admin\"");
    const { c, connected } = await openClient("admin-bad");
    if (!connected) return ko("Connexion", describeErr(c.lastErr));

    const r1 = await c.ack("admin", "Hacker", "mauvaise-cle");
    check("Mauvaise clé refusée", r1?.ok === false, JSON.stringify(r1));
    const r2 = await c.ack("admin", "Hacker", 12345);
    check("Clé non-string refusée", r2?.ok === false, JSON.stringify(r2));
    c.emit("admin", "Hacker"); // 3e échec (sans clé, sans ack)
    const kicked = await c.waitFor("__disconnect", () => true, { timeout: 4000 });
    check("Déconnexion après 3 échecs", kicked !== null, String(kicked));
    c.close();

    if (!ADMIN_KEY) return skip("Admin avec bonne clé (définis ADMIN_KEY)");
    const g = await openClient("admin-good");
    if (!g.connected) return ko("Connexion admin", describeErr(g.c.lastErr));
    const r3 = await g.c.ack("admin", "TestAdmin", ADMIN_KEY);
    check("Bonne clé acceptée", r3?.ok === true, JSON.stringify(r3));
    g.c.close();
}

// ═════════════════════════════════════════════════════════════════════════
// 6. Scénario de vente live
// ═════════════════════════════════════════════════════════════════════════
async function testLiveScenario() {
    title("6. Scénario de vente live complet");
    if (!ADMIN_KEY) return skip("Scénario ignoré : lance avec ADMIN_KEY=... pour l'activer");

    const room = `test-live-${Date.now().toString(36)}`;
    const otherRoom = `${room}-other`;
    const all = [];
    const open = async (name) => {
        const { c, connected } = await openClient(name);
        all.push(c);
        if (!connected) throw new Error(`${name} : connexion impossible (${describeErr(c.lastErr)})`);
        return c;
    };

    const lots = [1, 2].map((n) => ({
        numLot: String(n), price: "0", time: "600", extratime: "false",
        statut: "sold", toid: "", reserveInfo: "0", email: "", name: "",
    }));

    try {
        // ── Participants ──
        const admin = await open("admin");
        const ack = await admin.ack("admin", "TestAdmin", ADMIN_KEY);
        if (ack?.ok !== true) throw new Error(`admin refusé (${JSON.stringify(ack)}) — ADMIN_KEY correcte ?`);
        admin.emit("joinroom", room);
        const adminUL = await admin.waitFor("userList", (d) => d?.admin === admin.id);
        check("Admin enregistré dans userList", !!adminUL);

        const bidders = [];
        for (let i = 1; i <= 3; i++) {
            const b = await open(`bidder${i}`);
            b.pseudo = `Bidder${i}`;
            b.email = `b${i}@test.local`;
            b.emit("joinroom", room);
            b.emit("username", b.pseudo);
            bidders.push(b);
        }
        const outsider = await open("outsider");
        outsider.emit("joinroom", otherRoom);
        outsider.emit("username", "Outsider");
        const [b1, b2, b3] = bidders;

        // ── userList / connected ──
        let allSeeAdmin = true;
        for (const b of bidders) {
            const ul = await b.waitFor("userList", (d) => d?.admin === admin.id);
            if (!ul) allSeeAdmin = false;
        }
        check("userList : les 3 enchérisseurs voient l'admin (cross-instance)", allSeeAdmin);

        b1.emit("connected", { name: b1.pseudo, email: b1.email, room });
        const conn = await admin.msg("connected", (d) => d.from === b1.id && d.msg?.email === b1.email);
        check("Event connected → reçu par l'admin", !!conn);

        // ── getEncheresList → listLot privé ──
        const m1 = b1.mark(), m2 = b2.mark();
        let t = Date.now();
        b1.emit("getMsgPrivate", { toid: admin.id, type: "getEncheresList", msg: { room }, name: b1.pseudo });
        const gel = await admin.msg("getEncheresList", (d) => d.from === b1.id);
        check("getEncheresList bidder → admin", !!gel, `${Date.now() - t} ms`);

        admin.emit("getMsgPrivate", { toid: b1.id, type: "listLot", msg: { list: lots }, name: "TestAdmin" });
        const priv = await b1.msg("listLot", (d) => d.from === admin.id && d.msg?.list?.length === 2, { since: m1 });
        check("listLot privé reçu par le bidder demandeur", !!priv);
        check("listLot privé NON reçu par un autre bidder",
            await b2.none("sendMsg", (d) => d?.type === "listLot", 1200, m2));

        // ── Broadcast room ──
        const mo = outsider.mark();
        const marks = bidders.map((b) => b.mark());
        admin.emit("getMsgRoom", { room, type: "listLot", msg: { list: lots }, name: "TestAdmin" });
        const got = await Promise.all(bidders.map((b, i) =>
            b.msg("listLot", (d) => d.from === admin.id, { since: marks[i] })));
        check("getMsgRoom listLot → tous les enchérisseurs", got.every(Boolean),
            `${got.filter(Boolean).length}/3`);

        const marks2 = bidders.map((b) => b.mark());
        admin.emit("getMsgRoom", {
            room, type: "numLot", name: "TestAdmin",
            msg: { numLot: "1", price: "1500", reserveInfo: 1, time: "590", extratime: "false", statut: "sold" },
        });
        const got2 = await Promise.all(bidders.map((b, i) =>
            b.msg("numLot", (d) => d.msg?.price === "1500", { since: marks2[i] })));
        check("getMsgRoom numLot → tous les enchérisseurs", got2.every(Boolean),
            `${got2.filter(Boolean).length}/3`);
        check("Isolation de room : l'outsider ne reçoit rien",
            await outsider.none("sendMsg", (d) => ["listLot", "numLot"].includes(d?.type), 1200, mo));

        // ── Enchère ──
        const ma = admin.mark();
        t = Date.now();
        b1.emit("getMsgPrivate", {
            toid: admin.id, type: "doEncheres", name: b1.pseudo,
            msg: { myEnchere: 2500, lot: "1", email: b1.email },
        });
        const bid = await admin.msg("doEncheres", (d) => d.from === b1.id && d.msg?.myEnchere === 2500, { since: ma });
        check("doEncheres bidder → admin", !!bid, `${Date.now() - t} ms`);
        check("Nom de l'enchérisseur transmis", bid?.name === b1.pseudo, String(bid?.name));

        const mb1 = b1.mark(), mb2 = b2.mark();
        admin.emit("getMsgPrivate", {
            toid: b1.id, type: "confirmEnchere", name: "TestAdmin",
            msg: { lot: "1", state: true, myEnchere: 2500 },
        });
        const conf = await b1.msg("confirmEnchere", (d) => d.msg?.state === true, { since: mb1 });
        check("confirmEnchere reçu par le gagnant", !!conf);
        check("confirmEnchere : myEnchere copié dans price (compat)", conf?.msg?.price === 2500,
            `price=${conf?.msg?.price}`);
        check("confirmEnchere NON reçu par un autre bidder",
            await b2.none("sendMsg", (d) => d?.type === "confirmEnchere", 1200, mb2));

        admin.emit("getMsgPrivate", {
            toid: b2.id, type: "confirmEnchere", name: "TestAdmin", msg: { lot: "1", state: false },
        });
        const refused = await b2.msg("confirmEnchere", (d) => d.msg?.state === false, { since: mb2 });
        check("confirmEnchere refus (state=false) reçu par le perdant", !!refused);

        // ── Events directs du bidderHandler ──
        const ma2 = admin.mark();
        b2.emit("doEncheres", { lot: "2", myEnchere: 3000, room });
        check("Event direct doEncheres → broadcast room",
            !!(await admin.msg("doEncheres", (d) => d.from === b2.id && d.msg?.lot === "2", { since: ma2 })));

        b3.emit("reconnection", { name: b3.pseudo, email: b3.email, room });
        check("Event reconnection → reçu par l'admin",
            !!(await admin.msg("reconnection", (d) => d.from === b3.id && d.msg?.email === b3.email)));

        b3.emit("getEncheres", { room });
        check("Event getEncheres → reçu par l'admin",
            !!(await admin.msg("getEncheres", (d) => d.from === b3.id)));

        // ── Gros payload (300 Ko) ──
        const ma3 = admin.mark();
        b1.emit("getMsgPrivate", {
            toid: admin.id, type: "doEncheres", name: b1.pseudo,
            msg: { myEnchere: 1, lot: "1", filler: "x".repeat(300 * 1024) },
        });
        check("Payload de 300 Ko transmis",
            !!(await admin.msg("doEncheres", (d) => d.msg?.filler?.length === 300 * 1024, { since: ma3, timeout: 12000 })));

        if (flags.has("--big")) {
            const bigC = await open("big");
            const closed = bigC.waitFor("__disconnect", () => true, { timeout: 15000 });
            bigC.emit("getMsgPrivate", { toid: admin.id, type: "x", msg: { filler: "x".repeat(11 * 1024 * 1024) } });
            check("Payload 11 Mo → connexion coupée (maxHttpBufferSize)", (await closed) !== null);
        } else {
            skip("Payload 11 Mo (ajoute --big pour l'activer)");
        }

        // ── Payloads malformés (uniquement sur les events dont le code est connu) ──
        for (const ev of ["connected", "reconnection", "getEncheresList", "getEncheres", "doEncheres", "getMsgPrivate"]) {
            for (const bad of [null, undefined, {}, "str", 42, []]) b3.emit(ev, bad);
        }
        b3.emit("admin"); // sans arguments
        await sleep(1500);
        check("Payloads malformés : le client reste connecté", b3.connected);
        const health = await httpGet(`${TARGET_URL}/`).then((r) => r.status).catch(() => 0);
        check("Payloads malformés : serveur toujours en vie", health === 200, `HTTP ${health}`);

        // ── Remplacement d'admin ──
        const admin2 = await open("admin2");
        const ack2 = await admin2.ack("admin", "TestAdmin2", ADMIN_KEY);
        admin2.emit("joinroom", room);
        const evicted = await admin.waitFor("__disconnect", () => true, { timeout: 5000 });
        if (ack2?.ok && evicted !== null) {
            ok("Remplacement d'admin : l'ancien admin est expulsé");
        } else {
            warn("Remplacement d'admin : ancien admin non expulsé",
                "attendu si les deux admins sont sur des instances différentes (limite connue, cf. roomHandler)");
        }
        const ulNew = await b1.waitFor("userList", (d) => d?.admin === admin2.id, { timeout: 4000 });
        check("userList pointe vers le nouvel admin", !!ulNew);

        // ── Perte d'admin ──
        const mUL = bidders.map((b) => b.mark());
        admin2.close();
        admin.close();
        const lost = await Promise.all(bidders.map((b, i) =>
            b.waitFor("userList", (d) => !d?.admin, { since: mUL[i], timeout: 12000 })));
        check("Perte de l'admin → userList sans admin pour tous", lost.every(Boolean),
            `${lost.filter(Boolean).length}/3`);
    } catch (err) {
        ko("Scénario interrompu", err.message);
    } finally {
        all.forEach((c) => c.close());
    }
}

// ═════════════════════════════════════════════════════════════════════════
// 7. Santé finale
// ═════════════════════════════════════════════════════════════════════════
async function testFinalHealth() {
    title("7. Santé finale / crash d'instance");
    const elapsed = (Date.now() - T0) / 1000;
    const seen = {};
    for (let i = 0; i < 16; i++) {
        try {
            const d = JSON.parse((await httpGet(`${TARGET_URL}/`)).body);
            seen[d.instance || "inconnu"] = d.uptime;
        } catch { /* ignore */ }
    }
    const names = Object.keys(seen);
    if (!names.length) return ko("Serveur joignable après les tests");
    ok("Serveur joignable après les tests", `${names.length} instance(s) vue(s)`);
    const restarted = names.filter((n) => seen[n] < elapsed - 2);
    restarted.length
        ? ko("Instance redémarrée pendant le test (crash ?)", restarted.map((n) => `${n} uptime=${Math.round(seen[n])}s`).join(", "))
        : ok("Aucune instance n'a redémarré pendant le test");
}

// ═════════════════════════════════════════════════════════════════════════
(async () => {
    log("═".repeat(64));
    log(`  Test Socket.IO complet — ${TARGET_URL}`);
    log(`  Origin : ${ORIGIN}   |   ADMIN_KEY : ${ADMIN_KEY ? "oui" : "non"}`);
    log("═".repeat(64));

    await testHealth();
    await testDebugRoutes();
    await testHandshakeHttp();
    await testWsOrigin();
    await testAdminEvent();
    await testLiveScenario();
    await testFinalHealth();

    log("\n" + "═".repeat(64));
    log(`  ✓ ${stats.pass} OK   ✗ ${stats.fail} échec(s)   ⚠ ${stats.warn} avertissement(s)   ○ ${stats.skip} ignoré(s)`);
    log("═".repeat(64) + "\n");
    process.exit(stats.fail ? 1 : 0);
})().catch((err) => {
    console.error("Erreur fatale :", err);
    process.exit(1);
});