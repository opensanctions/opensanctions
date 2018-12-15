FROM alephdata/memorious

RUN pip install --upgrade xlrd attrs

COPY . /opensanctions
RUN pip install -e /opensanctions

ENV MEMORIOUS_CONFIG_PATH=/opensanctions/opensanctions/config \
    MEMORIOUS_EAGER=true

