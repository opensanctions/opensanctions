FROM pudo/opensanctions

ENV OPENSANCTIONS_DATA_PATH=/opensanctions/data
RUN mkdir -p /opensanctions/data/state
ADD https://data.opensanctions.org/state/opensanctions.sqlite /opensanctions/data/state/opensanctions.sqlite

# RUN opensanctions index default

RUN mkdir -p /appliance
COPY . /appliance
RUN pip install -e /appliance

CMD ["/usr/local/bin/uvicorn", "osapi.app:app", "--port", "8000", "--host", "0.0.0.0"]