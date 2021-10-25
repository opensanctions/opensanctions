FROM pudo/opensanctions

RUN mkdir -p /opensanctions/data/state
ADD https://data.opensanctions.org/state/opensanctions.sqlite /opensanctions/data/state/opensanctions.sqlite

# RUN opensanctions index default

RUN mkdir -p /appliance
COPY . /appliance
RUN pip install -e /appliance

CMD ["uvicorn", "osapi.app:app"]