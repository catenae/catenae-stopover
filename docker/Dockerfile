FROM cgr.dev/chainguard/wolfi-base:latest AS base
RUN apk upgrade --no-cache \
    && apk add --no-cache python-3.15 libstdc++ 'libexpat1>=2.10.0' 'sqlite-libs>=3.54.0'
ENV PATH="/opt/venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

FROM base AS pypkgs
RUN apk add --no-cache py3.15-pip
RUN /usr/bin/python3.15 -m venv --without-pip /opt/venv
ADD catenae.tar /src
RUN /usr/bin/python3.15 -m pip --python /opt/venv install --no-compile --only-binary=google-crc32c,pymongo,rocksdict /src \
    && /usr/bin/python3.15 -m pip --python /opt/venv check

FROM base
COPY --from=pypkgs /opt/venv /opt/venv
WORKDIR /opt/stopover
CMD ["python"]
