FROM alephdata/followthemoney

COPY . /opensanctions
WORKDIR /opensanctions
RUN pip install --no-cache-dir -q -e /opensanctions

ENV OPENSANCTIONS_DATA_PATH /data

CMD ["opensanctions", "run"]