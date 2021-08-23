FROM alephdata/followthemoney

RUN apt-get -qq -y update \
    && apt-get -qq -y upgrade \
    && apt-get -qq -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . /opensanctions
WORKDIR /opensanctions
RUN pip install --no-cache-dir -e /opensanctions

ENV OPENSANCTIONS_DATA_PATH /opensanctions/data

CMD ["opensanctions", "run"]