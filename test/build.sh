#!/bin/bash
tar cf ../../catenae.tar ../
mv ../../catenae.tar .
docker build -t catenae/test .
rm -f catenae.tar
