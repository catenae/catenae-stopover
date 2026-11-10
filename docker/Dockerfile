FROM cgr.dev/chainguard/wolfi-base@sha256:24f549a7fcace88e69b9557941168c74645688ecc4acf7fb6d734a6b74e9019d AS base
COPY docker/apk-base.lock /tmp/apk-base.lock
RUN apk add --no-cache $(cat /tmp/apk-base.lock)
ENV PATH="/opt/venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

FROM base AS pypkgs
COPY docker/apk-pypkgs.lock /tmp/apk-pypkgs.lock
RUN apk add --no-cache $(cat /tmp/apk-pypkgs.lock)
RUN /usr/bin/python3.15 -m venv --without-pip /opt/venv
COPY src /src
COPY docker/requirements-build.lock /tmp/requirements-build.lock
COPY docker/requirements.lock /tmp/requirements.lock
RUN /usr/bin/python3.15 -m pip install --require-hashes --no-compile --target /tmp/build -r /tmp/requirements-build.lock \
    && /usr/bin/python3.15 -m pip --python /opt/venv install --require-hashes --no-compile -r /tmp/requirements.lock \
    && PYTHONPATH=/tmp/build /usr/bin/python3.15 -m pip wheel --no-deps --no-build-isolation --wheel-dir /tmp/wheels /src \
    && /usr/bin/python3.15 -m pip --python /opt/venv install --no-deps --no-compile /tmp/wheels/*.whl \
    && /usr/bin/python3.15 -m pip --python /opt/venv check \
    && rm -rf /tmp/build /tmp/wheels

FROM base
COPY --from=pypkgs /opt/venv /opt/venv
WORKDIR /opt/stopover
CMD ["python"]
