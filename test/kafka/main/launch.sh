#!/bin/bash
current_dir="$(pwd)"
docker-compose down
cd ../..
./build.sh
cd $current_dir
docker-compose up -d
