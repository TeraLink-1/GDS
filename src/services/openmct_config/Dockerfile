FROM node:20-alpine
WORKDIR /app
COPY src/services/openmct_config/package.json .
RUN npm install --production
COPY src/services/openmct_config .
EXPOSE 8081
CMD ["node","server.js"]
