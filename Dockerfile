FROM alephdata/memorious
RUN apk add --no-cache --virtual .build-deps gcc python3-dev postgresql-dev
COPY . /opensanctions
RUN pip install -e /opensanctions
ENV MEMORIOUS_CONFIG_PATH=/opensanctions/opensanctions/config
