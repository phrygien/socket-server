FROM node:20-alpine

WORKDIR /app

# On copie d'abord les manifests pour profiter du cache Docker
COPY package*.json ./

RUN npm install --omit=dev

# Copie du reste du code
COPY . .

ENV NODE_ENV=production
ENV PORT=9022

EXPOSE 9022

# Healthcheck simple (adapte la route si tu as un endpoint /health)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:9022/ || exit 1

CMD ["node", "server.js"]
