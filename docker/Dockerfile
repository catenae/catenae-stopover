FROM registry.opensuse.org/opensuse/tumbleweed:latest@sha256:c6a30f005bf3444ef1ca02f49a0c34ee539b3299f948f1ecaf633e5b12dd2490 AS base
# Maintainer RPMs carry the TLS/TAR fixes ahead of the stable snapshot.
RUN zypper --non-interactive addrepo --priority 120 https://download.opensuse.org/repositories/devel:/languages:/python:/Factory/openSUSE_Tumbleweed/ python-factory
# Require the maintained Expat amplification fix.
RUN zypper --non-interactive addrepo --priority 120 https://download.opensuse.org/repositories/devel:/libraries:/c_c++/openSUSE_Factory/ libraries-devel
RUN zypper --non-interactive --gpg-auto-import-keys refresh \
    && zypper --non-interactive install --no-recommends ca-certificates 'python314>=3.14.10' 'python314-base>=3.14.10' 'python314-dbm>=3.14.10' libstdc++6 'libexpat1>=2.10.0' \
    && zypper clean --all
ENV PATH="/opt/venv/bin:${PATH}" PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

FROM base AS pypkgs
RUN zypper --non-interactive refresh \
    && zypper --non-interactive install --no-recommends python314-pip \
    && zypper clean --all
RUN python3.14 -m venv --system-site-packages --without-pip /opt/venv
ADD catenae.tar /src
RUN python -m pip install --no-compile --only-binary=google-crc32c,pymongo,rocksdict /src && python -m pip check

FROM base
COPY --from=pypkgs /opt/venv /opt/venv
WORKDIR /opt/stopover
CMD ["python"]
