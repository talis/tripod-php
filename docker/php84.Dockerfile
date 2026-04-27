FROM php:8.4.19-cli

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  git \
  unzip \
  zip \
  && rm -rf /var/lib/apt/lists/*

RUN curl -sLo /tmp/mongosh.deb https://downloads.mongodb.com/compass/mongodb-mongosh_2.6.0_amd64.deb \
  && dpkg -i /tmp/mongosh.deb \
  && rm /tmp/mongosh.deb

COPY --from=mlocati/php-extension-installer:2.10.6 /usr/bin/install-php-extensions /usr/local/bin/
COPY --from=composer:2.9.5 /usr/bin/composer /usr/local/bin/

RUN IPE_ICU_EN_ONLY=1 install-php-extensions pcntl mongodb-1.19.4 xhprof
