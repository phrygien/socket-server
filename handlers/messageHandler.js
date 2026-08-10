// ─── Message Handler ──────────────────────────────────────────────────────────
//
// FIX (bug "enchère validée puis refusée") :
//   L'event 'getMsgPrivate' était écouté DEUX FOIS sur le même socket :
//   une fois ici, une fois dans bidderHandler.js. Socket.IO exécute TOUS
//   les listeners enregistrés pour un même event → chaque emit('getMsgPrivate', ...)
//   du bidder déclenchait deux 'sendMsg' vers l'admin (switcher_list.php).
//
//   Côté admin, le handler 'doEncheres' compare myEnchere au prix courant :
//     - 1er sendMsg (dupliqué) : myEnchere > price(ancien) → accepté, price mis à jour
//     - 2e sendMsg (dupliqué)  : myEnchere == price (déjà mis à jour juste avant)
//                                → plus strictement supérieur → branche "refusé"
//                                → confirmEnchere{state:false} renvoyé au bidder
//
//   D'où le "Enchère refusée" affiché juste après "Enchère validée" pour la
//   même mise.
//
//   Toute la logique de 'getMsgPrivate' (routage + mise à jour du timer de
//   fin de vente + fix de compatibilité confirmEnchere.price) est déjà
//   présente et centralisée dans bidderHandler.js. Ce fichier ne doit donc
//   PLUS enregistrer de listener sur 'getMsgPrivate' pour éviter le doublon.
//
//   Ce module est conservé au cas où d'autres events de messagerie privée
//   (non liés à 'getMsgPrivate') seraient ajoutés plus tard, mais n'a
//   actuellement rien à enregistrer.

function registerMessageHandler(io, socket) {
  // Intentionnellement vide : voir note ci-dessus.
  // Ne PAS réenregistrer 'getMsgPrivate' ici — géré exclusivement par
  // bidderHandler.js (registerBidderHandler).
}

module.exports = { registerMessageHandler };