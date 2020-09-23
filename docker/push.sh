#!/bin/bash
source env.sh
docker push catenae/link-stopover:$VERSION
docker push catenae/link-stopover:latest
