FROM node:22-alpine AS build
# Explicit channel for the static operator token on controlled builds; the
# value is inlined into the public bundle by Vite, so treat it accordingly
# and prefer OIDC (no token) whenever possible. Local .env* files are
# excluded from this context via .dockerignore.
ARG VITE_API_TOKEN
ENV VITE_API_TOKEN=$VITE_API_TOKEN
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
# nginx's envsubst entrypoint renders this template on start; set
# ARKFLOW_HUB_UPSTREAM to point the same-origin proxy at the Hub.
COPY nginx.default.conf.template /etc/nginx/templates/default.conf.template
ENV ARKFLOW_HUB_UPSTREAM=http://arkflow-hub:8080
EXPOSE 80
