#!/bin/bash
tar cf ../../catenae.tar ../
mv ../../catenae.tar .
docker build -t catenae/link:develop .
rm -f catenae.tar
