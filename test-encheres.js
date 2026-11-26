/**
 * test-encheres.js — Test du circuit d'enchères de bout en bout
 * ─────────────────────────────────────────────────────────────────────────
 * Un "admin bot" reproduit la logique de switcher.php (doEncheres → comparaison
 * au prix courant → numLot en broadcast → confirmEnchere privé → timer →
 * validEnchere à la clôture, extra time, reconnection/changeDevice, etc.).
 * Des enchérisseurs simulés jouent le rôle de vente_live.php.
 * Le serveur n'est qu'un relais : ce test vérifie que ce relais livre tout,
 * dans le bon ordre, au bon destinataire, à travers les 4 instances.
 *
 * Usage :
 *   $env:ADMIN_KEY="xxxx"; node .\test-encheres.js
 *   node .\test-encheres.js https://socket-auctav.astucom.com https://www.auctav.com
 *
 * Tout se passe dans une room unique "test-enchere-xxxx" : aucune vente réelle
 * n'est touchée.
 * ─────────────────────────────────────────────────────────────────────────
 */

const { io } = require("socket.io-client");

const args = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const TARGET_URL = args[0] || "https://socket-auctav.astucom.com";
const ORIGIN = args[1] || process.env.ORIGIN || "https://www.auctav.com";
const ADMIN_KEY = process.env.ADMIN_KEY || "27868291c8a069539dbc76c4b19b2f96630a98b91d42629d158f0eb8d62b9129";
const TIMEOUT = 8000;

const stats = { pass: 0, fail: 0, warn: 0 };
const log = (m) => console.log(m);
const title = (t) => log(`\n── ${t} ──`);
const ok = (n, d = "") => (stats.pass++, log(`   ✓ ${n}${d ? " — " + d : ""}`));
const ko = (n, d = "") => (stats.fail++, log(`   ✗ ${n}${d ? " — " + d : ""}`));
const check = (n, c, d = "") => (c ? ok(n, d) : ko(n, d));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function describeErr(err) {
    if (!err) return "erreur inconnue";
    const d = err.description;
    const detail = d && (d.message || d.error?.message || (typeof d === "string" ? d : ""));
    const status = err.context?.statusCode || err.context?.status;
    return [err.message, detail, status && `HTTP ${status}`].filter(Boolean).join(" | ");
}

// ─── Client de test ───────────────────────────────────────────────────────
class TC {
    constructor(name) {
        this.name = name;
        this.events = [];
        this.socket = io(TARGET_URL, {
            transports: ["websocket"], forceNew: true, reconnection: false,
            timeout: TIMEOUT, extraHeaders: { Origin: ORIGIN },
        });
        this.socket.onAny((ev, ...a) => this.events.push({ ev, data: a[0] }));
        this.socket.on("disconnect", (r) => this.events.push({ ev: "__disconnect", data: r }));
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
                if (left-- > 0 && /Too many|Server error/i.test(err.message)) setTimeout(() => s.connect(), 500);
                else { cleanup(); resolve(false); }
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
            await sleep(30);
        }
    }
    msg(type, pred = () => true, opts) {
        return this.waitFor("sendMsg", (d) => d && d.type === type && pred(d), opts);
    }
    async none(type, pred = () => true, ms = 1500, since = 0) {
        return (await this.msg(type, pred, { since, timeout: ms })) === null;
    }
    // Tous les numLot reçus pour un lot depuis `since`
    numSeq(lot, since = 0) {
        return this.events.slice(since)
            .filter((e) => e.ev === "sendMsg" && e.data?.type === "numLot" && String(e.data.msg?.numLot) === String(lot))
            .map((e) => e.data.msg);
    }
    async ack(event, ...a) {
        return new Promise((resolve) => {
            const t = setTimeout(() => resolve(null), TIMEOUT);
            this.socket.emit(event, ...a, (res) => { clearTimeout(t); resolve(res); });
        });
    }
}

// ─── Admin bot : reproduit switcher.php ───────────────────────────────────
class AdminBot {
    constructor(tc, room) {
        this.tc = tc; this.room = room; this.lots = {}; this.timer = null;
    }
    addLot(n, { time = 600, type = "normal" } = {}) {
        this.lots[n] = {
            numLot: String(n), price: 0, time, extratime: "false", statut: "sold",
            toid: "", name: "", email: "", reload: false, end: true, type,
        };
    }
    listInfo() {
        return Object.values(this.lots).map((l) => ({
            numLot: l.numLot, price: String(l.price), time: String(l.time), extratime: String(l.extratime),
            statut: l.statut, toid: l.toid, reserveInfo: "0", email: l.email, name: l.name,
        }));
    }
    priv(toid, type, msg) { this.tc.emit("getMsgPrivate", { toid, type, msg, name: "Admin" }); }
    roomMsg(type, msg) { this.tc.emit("getMsgRoom", { room: this.room, type, msg, name: "Admin" }); }

    // Équivalent du submit du formulaire d'un lot
    submit(l) {
        const reload = l.reload;
        l.reload = false;
        if (l.time > 0 && l.time < 60 && reload && l.type !== "hidden") { l.time = 60; l.extratime = "true"; }
        this.roomMsg("numLot", {
            numLot: l.numLot, price: String(l.price), reserveInfo: l.price > 0 ? 1 : 0,
            time: String(l.time), extratime: l.extratime, statut: l.statut,
        });
        if (l.toid) this.priv(l.toid, "confirmEnchere", { lot: l.numLot, state: true, manuel: true });
        if (l.time === 0 && l.end) {
            l.end = false;
            if (l.toid && l.type !== "hidden") this.priv(l.toid, "validEnchere", { lot: l.numLot, state: true });
        }
    }

    attach() {
        this.tc.socket.on("sendMsg", (d) => this.onMsg(d));
        this.timer = setInterval(() => {
            for (const l of Object.values(this.lots)) {
                if (l.time > 1) l.time--;
                else if (l.time === 1) { l.time = 0; this.submit(l); }
            }
        }, 1000);
    }
    stop() { clearInterval(this.timer); }

    onMsg(d) {
        if (!d) return;
        if (d.type === "doEncheres") {
            const l = this.lots[String(d.msg?.lot)];
            if (l && l.time > 0) {
                l.reload = true;
                if (Number(d.msg.myEnchere) > Number(l.price)) {
                    l.price = Number(d.msg.myEnchere); l.toid = d.from; l.name = d.name; l.email = d.msg.email;
                    this.submit(l);
                } else this.priv(d.from, "confirmEnchere", { lot: d.msg.lot, state: false });
            } else this.priv(d.from, "confirmEnchere", { lot: d.msg?.lot, state: false });
        }
        if (d.type === "reconnection") {
            let changeDevice = false;
            for (const l of Object.values(this.lots)) {
                if (l.email && l.email === d.msg?.email) {
                    if (l.toid !== d.from) changeDevice = l.toid;
                    l.toid = d.from;
                    this.submit(l);
                }
            }
            if (changeDevice) this.priv(changeDevice, "changeDevice", { manuel: true });
            this.priv(d.from, "listLot", { list: this.listInfo() });
        }
        if (d.type === "getEncheresList" || d.type === "requestRefresh") {
            this.priv(d.from, "listLot", { list: this.listInfo() });
        }
    }

    // Actions de l'admin
    resync() { this.roomMsg("listLot", { list: this.listInfo() }); }
    setTime(n, t) { this.lots[n].time = t; }
    manual(n, { price, statut }) {
        const l = this.lots[n];
        if (price !== undefined) l.price = price;
        if (statut) l.statut = statut;
        this.submit(l);
    }
    adjust(val) { // adjustTime(val) : temps -= val
        const out = {};
        for (const l of Object.values(this.lots)) {
            if (l.time > 0) { l.time = Math.max(0, l.time - val); out[l.numLot] = l.time; this.submit(l); }
        }
        return out;
    }
    endSale() {
        const out = {};
        for (const l of Object.values(this.lots)) {
            if (l.time > 0) { l.time = 60; out[l.numLot] = 60; this.submit(l); }
        }
        return out;
    }
}

// ─── Scénario ─────────────────────────────────────────────────────────────
const latencies = [];

(async () => {
    log("═".repeat(64));
    log(`  Test des enchères — ${TARGET_URL}`);
    log(`  Origin : ${ORIGIN}`);
    log("═".repeat(64));

    if (!ADMIN_KEY) {
        console.error("\nADMIN_KEY requis :  $env:ADMIN_KEY=\"xxxx\"; node .\\test-encheres.js\n");
        process.exit(2);
    }

    const room = `test-enchere-${Date.now().toString(36)}`;
    const all = [];
    let bot;

    const open = async (name) => {
        const c = new TC(name);
        all.push(c);
        if (!(await c.connect())) throw new Error(`${name} : connexion impossible (${describeErr(c.lastErr)})`);
        return c;
    };

    try {
        // ── 0. Mise en place ────────────────────────────────────────────────
        title("0. Mise en place (admin + 3 enchérisseurs)");
        const adminTC = await open("admin");
        const ack = await adminTC.ack("admin", "TestAdmin", ADMIN_KEY);
        if (ack?.ok !== true) throw new Error(`admin refusé (${JSON.stringify(ack)}) — ADMIN_KEY correcte ?`);
        adminTC.emit("joinroom", room);
        bot = new AdminBot(adminTC, room);
        bot.addLot(1);
        bot.addLot(2);
        bot.addLot(3);
        bot.addLot(4);
        bot.addLot(5, { type: "hidden" });
        bot.addLot(6);
        bot.attach();
        await adminTC.waitFor("userList", (d) => d?.admin === adminTC.id);

        const bidders = [];
        for (let i = 1; i <= 3; i++) {
            const b = await open(`bidder${i}`);
            b.pseudo = `Bidder${i}`;
            b.email = `b${i}@test.local`;
            b.emit("joinroom", room);
            b.emit("username", b.pseudo);
            bidders.push(b);
        }
        const [b1, b2, b3] = bidders;
        const adminId = adminTC.id;

        const seen = await Promise.all(bidders.map((b) => b.waitFor("userList", (d) => d?.admin === adminId)));
        check("Les enchérisseurs voient l'admin", seen.every(Boolean));

        // Comme vente_live.php : reconnection → admin → listLot privé
        const lists = await Promise.all(bidders.map(async (b) => {
            const m = b.mark();
            b.emit("getMsgPrivate", { toid: adminId, type: "reconnection", msg: { email: b.email, room }, name: b.pseudo });
            return b.msg("listLot", (d) => d.msg?.list?.length === 6, { since: m });
        }));
        check("Chaque enchérisseur reçoit la liste des 6 lots", lists.every(Boolean));

        // Helpers
        const bid = async (b, lot, amount) => {
            const m = b.mark();
            const t = Date.now();
            b.emit("getMsgPrivate", {
                toid: adminId, type: "doEncheres", name: b.pseudo,
                msg: { myEnchere: amount, lot: String(lot), email: b.email },
            });
            const d = await b.msg("confirmEnchere", (x) => String(x.msg?.lot) === String(lot), { since: m });
            if (d) latencies.push(Date.now() - t);
            return d ? d.msg.state : null;
        };
        const allSee = async (lot, pred, since) =>
            (await Promise.all(bidders.map((b, i) =>
                b.msg("numLot", (d) => String(d.msg.numLot) === String(lot) && pred(d.msg), { since: since[i] })))).every(Boolean);
        const marks = () => bidders.map((b) => b.mark());

        // ── 1. Surenchères ──────────────────────────────────────────────────
        title("1. Surenchères sur le lot 1");
        let mk = marks();
        check("b1 enchérit 1000 → validée", (await bid(b1, 1, 1000)) === true);
        check("numLot 1000 reçu par tous", await allSee(1, (m) => Number(m.price) === 1000, mk));

        mk = marks();
        check("b2 enchérit 1500 → validée", (await bid(b2, 1, 1500)) === true);
        check("numLot 1500 reçu par tous", await allSee(1, (m) => Number(m.price) === 1500, mk));

        check("b1 enchérit 1500 (égal) → refusée", (await bid(b1, 1, 1500)) === false);
        check("b1 enchérit 1200 (inférieur) → refusée", (await bid(b1, 1, 1200)) === false);
        check("Le prix reste à 1500 côté enchérisseurs", Number(b3.numSeq(1).pop()?.price) === 1500);

        mk = marks();
        check("b1 enchérit 2000 → validée", (await bid(b1, 1, 2000)) === true);
        check("numLot 2000 reçu par tous", await allSee(1, (m) => Number(m.price) === 2000, mk));

        // Qui détient l'enchère : via listLot (toid)
        const mr = marks();
        bot.resync();
        const rl = await Promise.all(bidders.map((b, i) => b.msg("listLot", () => true, { since: mr[i] })));
        const holder = rl[1]?.msg?.list?.find((l) => l.numLot === "1");
        check("listLot de resynchro : toid = b1 (détenteur)", holder?.toid === b1.id, `toid=${holder?.toid}`);
        check("listLot de resynchro : prix = 2000", Number(holder?.price) === 2000);

        // ── 2. Extra time & soumission cachée ───────────────────────────────
        title("2. Extra time (lot 2) et soumission cachée (lot 5)");
        bot.setTime(2, 40);
        mk = marks();
        check("Enchère sur lot 2 à 40 s → validée", (await bid(b2, 2, 1000)) === true);
        const extra = await b3.msg("numLot", (d) => String(d.msg.numLot) === "2" && Number(d.msg.price) === 1000, { since: mk[2] });
        check("Extra time : temps remis à 60 s", Number(extra?.msg?.time) === 60, `time=${extra?.msg?.time}`);
        check("Extra time : flag extratime=true", String(extra?.msg?.extratime) === "true");

        bot.setTime(5, 40);
        mk = marks();
        check("Enchère sur lot caché à 40 s → validée", (await bid(b3, 5, 800)) === true);
        const hid = await b1.msg("numLot", (d) => String(d.msg.numLot) === "5", { since: mk[0] });
        check("Lot caché : pas d'extra time", Number(hid?.msg?.time) <= 40 && String(hid?.msg?.extratime) !== "true",
            `time=${hid?.msg?.time} extratime=${hid?.msg?.extratime}`);

        // ── 3. Enchères simultanées ─────────────────────────────────────────
        title("3. Enchères simultanées sur le lot 3");
        mk = marks();
        const r1 = await Promise.all(bidders.map((b) => bid(b, 3, 5000)));
        check("3 enchères égales (5000) en même temps → exactement 1 validée",
            r1.filter((s) => s === true).length === 1 && r1.filter((s) => s === false).length === 2,
            JSON.stringify(r1));

        mk = marks();
        const r2 = await Promise.all([bid(b1, 3, 6000), bid(b2, 3, 7000), bid(b3, 3, 8000)]);
        log(`   (réponses : ${JSON.stringify(r2)})`);
        await sleep(2000);
        check("Prix final = 8000 (le plus haut gagne)", bot.lots[3].price === 8000);
        check("Détenteur final = b3", bot.lots[3].toid === b3.id);
        check("Tous les enchérisseurs voient 8000 (cohérence cross-instance)",
            bidders.every((b) => Number(b.numSeq(3).pop()?.price) === 8000));
        const mono = bidders.every((b) => {
            const p = b.numSeq(3, mk[0]).map((m) => Number(m.price));
            return p.every((v, i) => i === 0 || v >= p[i - 1]);
        });
        check("Ordre des numLot préservé (prix croissants) chez chaque enchérisseur", mono);

        // ── 4. Clôture ──────────────────────────────────────────────────────
        title("4. Clôture du lot 4");
        check("b2 enchérit 2000 → validée", (await bid(b2, 4, 2000)) === true);
        mk = marks();
        bot.setTime(4, 2);
        const closed = await Promise.all(bidders.map((b, i) =>
            b.msg("numLot", (d) => String(d.msg.numLot) === "4" && Number(d.msg.time) === 0, { since: mk[i], timeout: 12000 })));
        check("numLot time=0 reçu par tous", closed.every(Boolean));
        const win = await b2.msg("validEnchere", (d) => String(d.msg.lot) === "4", { since: mk[1], timeout: 5000 });
        check("validEnchere reçu par le gagnant (b2)", !!win);
        check("validEnchere NON reçu par b1", await b1.none("validEnchere", () => true, 1200, mk[0]));
        check("validEnchere NON reçu par b3", await b3.none("validEnchere", () => true, 1200, mk[2]));
        check("Enchère après clôture (b1 3000) → refusée", (await bid(b1, 4, 3000)) === false);
        check("Prix du lot clôturé inchangé (2000)", bot.lots[4].price === 2000);

        // ── 5. Validation manuelle admin ────────────────────────────────────
        title("5. Validation manuelle par l'admin (✔ Valider)");
        mk = marks();
        const mh = b1.mark();
        bot.manual(1, { price: 2500, statut: "amicable" });
        check("numLot (2500, amicable) reçu par tous",
            await allSee(1, (m) => Number(m.price) === 2500 && m.statut === "amicable", mk));
        const conf = await b1.msg("confirmEnchere", (d) => String(d.msg.lot) === "1" && d.msg.manuel === true, { since: mh });
        check("Le détenteur reçoit confirmEnchere (manuel)", !!conf && conf.msg.state === true);

        // ── 6. Changement d'appareil ────────────────────────────────────────
        title("6. Changement d'appareil (même email, nouveau socket)");
        const b1b = await open("bidder1-device2");
        b1b.pseudo = b1.pseudo; b1b.email = b1.email;
        b1b.emit("joinroom", room);
        b1b.emit("username", b1b.pseudo);
        const m1 = b1.mark(), m1b = b1b.mark();
        b1b.emit("getMsgPrivate", { toid: adminId, type: "reconnection", msg: { email: b1.email, room }, name: b1b.pseudo });
        check("Ancien appareil reçoit changeDevice", !!(await b1.msg("changeDevice", () => true, { since: m1, timeout: 6000 })));
        const nl = await b1b.msg("listLot", (d) => d.msg?.list?.length === 6, { since: m1b });
        check("Nouvel appareil reçoit listLot", !!nl);
        check("Le lot est rattaché au nouvel appareil (toid)", nl?.msg?.list?.find((l) => l.numLot === "1")?.toid === b1b.id);
        check("Nouvel appareil reçoit confirmEnchere (état restauré)",
            !!(await b1b.msg("confirmEnchere", (d) => String(d.msg.lot) === "1" && d.msg.state === true, { since: m1b })));

        // ── 7. Demande de refresh, follow, noActivity ───────────────────────
        title("7. requestRefresh, follow, noActivity");
        let m = b3.mark();
        b3.emit("getMsgPrivate", { toid: adminId, type: "requestRefresh", msg: { room, email: b3.email }, name: b3.pseudo });
        check("requestRefresh → listLot privé", !!(await b3.msg("listLot", () => true, { since: m })));

        m = b2.mark();
        const ma = adminTC.mark();
        bot.priv(b2.id, "follow", { statut: true });
        check("follow reçu par l'enchérisseur", !!(await b2.msg("follow", () => true, { since: m })));
        b2.emit("getMsgPrivate", { toid: adminId, type: "connected", msg: { email: b2.email, room }, name: b2.pseudo });
        check("Réponse 'connected' reçue par l'admin",
            !!(await adminTC.msg("connected", (d) => d.from === b2.id, { since: ma })));

        m = b3.mark();
        bot.priv(b3.id, "noActivity", { manuel: true });
        check("noActivity reçu par l'enchérisseur", !!(await b3.msg("noActivity", () => true, { since: m })));
        const ma2 = adminTC.mark();
        b3.emit("getMsgPrivate", { toid: adminId, type: "exit", msg: { email: b3.email, room }, name: b3.pseudo });
        check("Réponse 'exit' reçue par l'admin",
            !!(await adminTC.msg("exit", (d) => d.from === b3.id, { since: ma2 })));

        // ── 8. Rafale de numLot ─────────────────────────────────────────────
        title("8. Rafale de 30 numLot (lot 6) — complétude et ordre");
        mk = marks();
        for (let i = 1; i <= 30; i++) {
            bot.roomMsg("numLot", {
                numLot: "6", price: String(i * 100), reserveInfo: 1, time: "600", extratime: "false", statut: "sold",
            });
        }
        const t0 = Date.now();
        for (;;) {
            if (bidders.every((b, i) => b.numSeq(6, mk[i]).length >= 30) || Date.now() - t0 > 10000) break;
            await sleep(50);
        }
        bidders.forEach((b, i) => {
            const p = b.numSeq(6, mk[i]).map((x) => Number(x.price));
            const inOrder = p.every((v, j) => v === (j + 1) * 100);
            check(`${b.name} : 30/30 numLot reçus, dans l'ordre`, p.length === 30 && inOrder, `${p.length}/30`);
        });

        // ── 9. Ajuster le temps / terminer la vente ─────────────────────────
        title("9. Ajuster le temps et terminer la vente");
        mk = marks();
        const adj = bot.adjust(-120);
        const gotAdj = await b3.msg("numLot", (d) => String(d.msg.numLot) === "3", { since: mk[2] });
        check("Ajustement +120 s transmis (lot 3)", String(gotAdj?.msg?.time) === String(adj["3"]),
            `reçu=${gotAdj?.msg?.time} attendu=${adj["3"]}`);

        mk = marks();
        const end = bot.endSale();
        const gotEnd = await Promise.all(bidders.map((b, i) =>
            b.msg("numLot", (d) => String(d.msg.numLot) === "3" && Number(d.msg.time) === 60, { since: mk[i] })));
        check("Terminer la vente : lot 3 passé à 60 s chez tous", gotEnd.every(Boolean));
        check("Lot clôturé (4) non rouvert par 'terminer la vente'", end["4"] === undefined);

        // ── Bilan réseau ────────────────────────────────────────────────────
        title("Latence aller-retour des enchères (bidder → admin → confirmation)");
        if (latencies.length) {
            const s = [...latencies].sort((a, b) => a - b);
            const avg = Math.round(s.reduce((a, b) => a + b, 0) / s.length);
            log(`   ${s.length} mesures — moyenne ${avg} ms, médiane ${s[Math.floor(s.length / 2)]} ms, max ${s[s.length - 1]} ms`);
            s[s.length - 1] > 2000 ? (stats.warn++, log("   ⚠ latence max > 2 s")) : ok("Latences correctes (< 2 s)");
        }

        // Events/types inattendus (ex : events du service de fin de vente)
        const known = new Set(["sendMsg", "userList", "__disconnect"]);
        const knownTypes = new Set(["numLot", "listLot", "confirmEnchere", "validEnchere", "doEncheres", "reconnection",
            "changeDevice", "follow", "connected", "noActivity", "exit", "requestRefresh", "getEncheresList", "getEncheres"]);
        const extraEv = new Set(), extraTypes = new Set();
        all.forEach((c) => c.events.forEach((e) => {
            if (!known.has(e.ev)) extraEv.add(e.ev);
            if (e.ev === "sendMsg" && e.data?.type && !knownTypes.has(e.data.type)) extraTypes.add(e.data.type);
        }));
        if (extraEv.size || extraTypes.size) {
            log(`\n   ℹ Events additionnels observés : ${[...extraEv].join(", ") || "-"}  | types sendMsg : ${[...extraTypes].join(", ") || "-"}`);
        }
    } catch (err) {
        ko("Scénario interrompu", err.message);
    } finally {
        if (bot) bot.stop();
        all.forEach((c) => c.close());
    }

    log("\n" + "═".repeat(64));
    log(`  ✓ ${stats.pass} OK   ✗ ${stats.fail} échec(s)   ⚠ ${stats.warn} avertissement(s)`);
    log("═".repeat(64) + "\n");
    process.exit(stats.fail ? 1 : 0);
})();