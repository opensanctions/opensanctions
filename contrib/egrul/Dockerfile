# Custom container image for the contrib/egrul Spark workload on Google Cloud
# Managed Service for Apache Spark (formerly Dataproc Serverless), runtime 3.0
# (Spark 4.0.1, Python 3.12, Java 21).
#
# Extends the root opensanctions image (which already has zavod installed into
# /venv, including google-cloud-storage transitively) with the bits required by
# the Managed Spark custom-container spec — tini/procps/jemalloc, the spark
# user with UID 1099. Spark, Java, and Python are mounted from the host at
# runtime; we only set PYSPARK_PYTHON to point at the root image's venv so
# workers use the Python where zavod is installed.
#
# Build (from the repo root). The root image is amd64-only, so on arm64 hosts
# (M-series Macs) you need --platform=linux/amd64:
#   docker build --platform=linux/amd64 -t egrul-spark -f contrib/egrul/Dockerfile .
#
# Push to Artifact Registry:
#   docker tag egrul-spark <region>-docker.pkg.dev/<project>/opensanctions/egrul-spark:latest
#   docker push <region>-docker.pkg.dev/<project>/opensanctions/egrul-spark:latest
#
# Submit with --container-image=<that URI> --version=3.0.

# The root opensanctions image, pushed by .github/workflows/build.yml on every
# main commit. Override with --build-arg OPENSANCTIONS_IMAGE=... to pin.
ARG OPENSANCTIONS_IMAGE=ghcr.io/opensanctions/opensanctions:latest
FROM ${OPENSANCTIONS_IMAGE}

USER root

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq -y update \
    && apt-get -qq -y install --no-install-recommends \
       procps tini libjemalloc2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# contrib/ is shipped with the rest of the repo by the root image's `COPY . /opensanctions`,
# but we set PYTHONPATH so the egrul modules are importable without --py-files.
ENV PYSPARK_PYTHON=/venv/bin/python3 \
    PYTHONPATH=/opensanctions/contrib/egrul

# Required: workers run as the spark user (UID/GID 1099) per the Managed Service
# for Apache Spark custom-container spec. The root image's "app" user (UID 10023)
# is the wrong UID for this runtime.
RUN groupadd -g 1099 spark \
    && useradd -u 1099 -g 1099 -d /home/spark -m spark \
    && chown -R spark:spark /opensanctions /venv

USER spark
WORKDIR /opensanctions
