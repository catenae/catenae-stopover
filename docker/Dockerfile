# Catenae link image for Stopover.
#
# Replaces catenae/foundation-stopover (Ubuntu 20.04 EOL, Python 3.9, and the
# legacy python-rocksdb 0.7.0 / easyrocks 0.0.15a0 stack it dragged in) with a
# digest-pinned Alpine image.
#
# The previous Dockerfile could not build: it ran `python setup.py install`
# from /opt/catenae while the tar unpacked the sources under
# /opt/catenae/src (`can't open file '/opt/catenae/setup.py'`). build.sh now
# tars src/ directly, and `stopover` is pinned instead of `pip install -U`.

FROM python:3.14.10-alpine3.24@sha256:eb2c62658b8d51bff0acc408504b76521d02030de56a8a3c1e830083e89b89d2

# The pinned base includes current Alpine security updates. Re-pin to a new
# image digest when refreshing OS packages instead of an unbounded upgrade.

WORKDIR /opt/catenae
ADD catenae.tar /opt/catenae
RUN \
    pip install --no-cache-dir \
    "stopover==2.2610.0" \
    "binnakle==2.2610.0" \
    "falcon==4.4.0" \
    "setuptools>=84.2.1,<85" \
    "wheel==0.48.0" \
    && pip install --no-cache-dir --no-compile --no-build-isolation . \
    && rm -rf /root/.cache/pip \
    && find /usr/local -type d -name __pycache__ -exec rm -r {} \+ \
    && rm -rf /usr/local/lib/python3.14/site-packages/wheel* \
    /usr/local/lib/python3.14/site-packages/pip \
    /usr/local/lib/python3.14/site-packages/pip-*.dist-info \
    /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.14 /usr/local/bin/wheel
