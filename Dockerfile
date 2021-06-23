FROM alephdata/followthemoney

COPY . /opensanctions
WORKDIR /opensanctions
RUN pip install --no-cache-dir -e /opensanctions

ENV OPENSANCTIONS_DATA_PATH /opensanctions/data

CMD ["opensanctions", "run"]