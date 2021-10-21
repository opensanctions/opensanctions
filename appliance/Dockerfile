FROM pudo/opensanctions

RUN mkdir -p /opensanctions/data/state
ADD https://data.opensanctions.org/state/opensanctions.sqlite /opensanctions/data/state/opensanctions.sqlite

RUN opensanctions index default