#!/bin/bash
VERSION=${1:-develop}
docker rmi catenae/link:$VERSION
tar cf ../../catenae.tar ../
mv ../../catenae.tar .
docker build -t catenae/link:$VERSION .
rm -f catenae.tar
