FROM pudo/scraper-base
MAINTAINER Friedrich Lindenberg <friedrich@pudo.org>

COPY . /scraper
WORKDIR /scraper
RUN pip install -e .
CMD sh run.sh
