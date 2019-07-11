FROM alephdata/memorious:latest

RUN apk add --no-cache --virtual=build_deps python3-dev g++ musl-dev postgresql-dev && \
    pip3 install --no-cache-dir psycopg2-binary python-levenshtein && \
    apk del build_deps

COPY . /opensanctions
RUN pip install -e /opensanctions
ENV MEMORIOUS_CONFIG_PATH=/opensanctions/opensanctions/config \
    ARCHIVE_TYPE=file \
    ARCHIVE_PATH=/data/archive
