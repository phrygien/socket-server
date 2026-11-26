# syntax=docker/dockerfile:1

FROM node:20-alpine

# Variables d'environnement par défaut (surchargées par docker-compose)
ENV NODE_ENV=production \
    PORT=9022

WORKDIR /app

# 1) Manifests d'abord : le cache Docker est conservé tant que les dépendances ne changent pas
COPY package*.json ./

# 2) Installation reproductible, sans devDependencies, cache npm nettoyé
RUN npm ci --omit=dev && npm cache clean --force

# 3) Code applicatif, avec les bons droits pour l'utilisateur non-root
COPY --chown=node:node . .

# 4) Exécution sans root
USER node

EXPOSE 9022

# 127.0.0.1 plutôt que localhost (évite les soucis IPv6 sous alpine)
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${PORT}/ >/dev/null || exit 1

CMD ["node", "server.js"]