#!/bin/bash
VERSION=${1:-develop}
tar cf ../../catenae.tar ../
mv ../../catenae.tar .
docker build -t catenae/link:$VERSION .
rm -f catenae.tar
