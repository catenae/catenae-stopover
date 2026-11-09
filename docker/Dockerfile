FROM cgr.dev/chainguard/wolfi-base@sha256:24f549a7fcace88e69b9557941168c74645688ecc4acf7fb6d734a6b74e9019d AS base
COPY apk-base.lock /tmp/apk-base.lock
RUN apk add --no-cache $(cat /tmp/apk-base.lock)
ENV PATH="/opt/venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

FROM base AS pypkgs
COPY apk-pypkgs.lock /tmp/apk-pypkgs.lock
RUN apk add --no-cache $(cat /tmp/apk-pypkgs.lock)
RUN /usr/bin/python3.15 -m venv --without-pip /opt/venv
ADD catenae.tar /src
RUN /usr/bin/python3.15 -m pip --python /opt/venv install --no-compile --only-binary=google-crc32c,pymongo,rocksdict /src \
    && /usr/bin/python3.15 -m pip --python /opt/venv check

FROM base
COPY --from=pypkgs /opt/venv /opt/venv
WORKDIR /opt/stopover
CMD ["python"]
