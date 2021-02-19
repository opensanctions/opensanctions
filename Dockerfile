FROM alephdata/followthemoney

COPY . /opensanctions
RUN pip install --no-cache-dir -q -e /opensanctions
