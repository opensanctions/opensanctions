FROM pudo/opensanctions

RUN mkdir -p /api
RUN mkdir -p /opensanctions/data/state \
    && mkdir -p /opensanctions/data/index
RUN chown -R app:app /opensanctions/data
ADD --chown=app:app https://data.opensanctions.org/state/opensanctions.sqlite /opensanctions/data/state/opensanctions.sqlite

ENV OPENSANCTIONS_DATA_PATH=/opensanctions/data \
    OSAPI_SCOPE_DATASET=default

WORKDIR /api
COPY . /api
RUN pip install -e /api

USER app:app
CMD ["/usr/local/bin/uvicorn", "osapi.app:app", "--port", "8000", "--host", "0.0.0.0"]