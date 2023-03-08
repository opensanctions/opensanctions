FROM ubuntu:23.04
LABEL org.opencontainers.image.title "OpenSanctions ETL"
LABEL org.opencontainers.image.licenses MIT
LABEL org.opencontainers.image.source https://github.com/opensanctions/opensanctions

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -qq -y update \
    && apt-get -qq -y upgrade \
    && apt-get -qq -y install locales ca-certificates tzdata curl python3-pip \
    python3-icu python3-cryptography libicu-dev postgresql-client-common \
    postgresql-client libxml2-dev libxslt1-dev python3-dev \
    && apt-get -qq -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && groupadd -g 1000 -r app \
    && useradd -m -u 1000 -s /bin/false -g app app

ENV LANG="en_US.UTF-8" \
    TZ="UTC"

RUN pip3 install --no-cache-dir -U pip six setuptools

COPY . /opensanctions
WORKDIR /opensanctions
RUN pip install --no-cache-dir -e /opensanctions

ENV OPENSANCTIONS_DATA_PATH="/opensanctions/data" \
    OPENSSL_CONF="/opensanctions/contrib/openssl.cnf"

CMD ["opensanctions", "crawl"]