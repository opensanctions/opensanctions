# Test with e.g.
# docker run -ti -p3000:3000 --rm -e ZAVOD_DATABASE_URI=postgresql://postgres:password@host.docker.internal:5432/dev -e ZAVOD_IAP_AUTH_DISABLED=true zavod-ui

FROM node:24-slim

LABEL org.opencontainers.image.title="Zavod UI"

RUN apt-get update && apt-get install -y --no-install-recommends locales \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build locale definition
RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV LANG="en_US.UTF-8"
ENV TZ="UTC"

RUN useradd -u 10000 -s /bin/false app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

RUN mkdir /app
COPY . /app

WORKDIR /app
RUN npm install --no-fund -g npm && npm install --no-fund --production=false --force
RUN npm run --prefix /app build

EXPOSE 3000
CMD ["npm", "run", "--prefix", "/app", "start"]
