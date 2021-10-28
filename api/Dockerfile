FROM pudo/opensanctions


RUN mkdir -p /opensanctions/data/state
RUN chown -R app:app /opensanctions/data
ADD --chown=app:app https://data.opensanctions.org/state/opensanctions.sqlite /opensanctions/data/state/opensanctions.sqlite
# RUN chmod 666 /opensanctions/data/state/opensanctions.sqlite

ENV OPENSANCTIONS_DATA_PATH=/opensanctions/data \
    OSAPI_SCOPE_DATASET=default
# RUN opensanctions index $OSAPI_SCOPE_DATASET

RUN mkdir -p /appliance
COPY . /appliance
RUN pip install -e /appliance

USER app:app
CMD ["/usr/local/bin/uvicorn", "osapi.app:app", "--port", "8000", "--host", "0.0.0.0"]