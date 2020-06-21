#!/bin/bash
source env.sh
docker rmi catenae/link-stopover:$VERSION 2> /dev/null
tar cf ../../catenae.tar ../
mv ../../catenae.tar .
docker build -t catenae/link-stopover:$VERSION .
rm -f catenae.tar
