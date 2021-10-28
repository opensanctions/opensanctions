FROM pudo/opensanctions


RUN mkdir -p /opensanctions/data/state \
    && mkdir -p /opensanctions/data/index
RUN chown -R app:app /opensanctions/data
ADD --chown=app:app https://data.opensanctions.org/state/opensanctions.sqlite /opensanctions/data/state/opensanctions.sqlite
ADD --chown=app:app https://data.opensanctions.org/index/default.pkl /opensanctions/data/index/default.pkl

ENV OPENSANCTIONS_DATA_PATH=/opensanctions/data \
    OSAPI_SCOPE_DATASET=default

RUN mkdir -p /appliance
COPY . /appliance
RUN pip install -e /appliance

USER app:app
CMD ["/usr/local/bin/uvicorn", "osapi.app:app", "--port", "8000", "--host", "0.0.0.0"]