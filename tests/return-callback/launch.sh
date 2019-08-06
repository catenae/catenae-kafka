#!/bin/bash
current_dir="$(pwd)"
cd ../../docker && ./build.sh
cd $current_dir
docker-compose up -d
