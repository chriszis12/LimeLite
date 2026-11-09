FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev || npm install --omit=dev
COPY server.js ./
COPY public ./public
ENV NODE_ENV=production PORT=8080
EXPOSE 8080
CMD ["node","server.js"]
