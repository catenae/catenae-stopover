#!/bin/bash
cd $(dirname $0)
source env.sh
docker push catenae/link-stopover:$VERSION
docker push catenae/link-stopover:latest
