FROM alephdata/followthemoney

COPY . /opensanctions
RUN pip install -e /opensanctions
