#!/bin/bash
VERSION=${1:-develop}
docker push catenae/link:$VERSION
