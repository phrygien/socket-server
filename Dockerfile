# syntax=docker/dockerfile:1

FROM node:20-alpine

ENV NODE_ENV=production \
    PORT=9022

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --chown=node:node . .

# Fichier d'historique inscriptible par l'utilisateur node
RUN touch /app/historique.json && chown node:node /app/historique.json

USER node

EXPOSE 9022

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:${PORT}/ >/dev/null || exit 1

CMD ["node", "server.js"]