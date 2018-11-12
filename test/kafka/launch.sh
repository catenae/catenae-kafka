#!/bin/bash
docker-compose down
cd ..
./build.sh
cd kafka
docker-compose up -d
