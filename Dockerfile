FROM pudo/scraper-base
MAINTAINER Friedrich Lindenberg <friedrich@pudo.org>

COPY . /opennames
WORKDIR /opennames
RUN pip install -e .
CMD sh run.sh
