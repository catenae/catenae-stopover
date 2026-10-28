#!/bin/bash
set -euo pipefail
cd $(dirname $0)
source env.sh
tar -C ../src -cf catenae.tar .
trap 'rm -f catenae.tar' EXIT
docker build --no-cache -t catenae/link-stopover:$VERSION .
docker tag catenae/link-stopover:$VERSION catenae/link-stopover:latest
rm -f catenae.tar
