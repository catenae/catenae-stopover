#!/bin/bash
cd $(dirname $0)
docker pull catenae/foundation-stopover
source env.sh
docker rmi catenae/link-stopover:$VERSION 2> /dev/null
tar cf ../../catenae.tar ../
mv ../../catenae.tar .
docker build --no-cache -t catenae/link-stopover:$VERSION .
docker tag catenae/link-stopover:$VERSION catenae/link-stopover:latest
rm -f catenae.tar
