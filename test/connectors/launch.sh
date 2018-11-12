#!/bin/bash
cd ..; ./build.sh; cd connectors
docker-compose down
docker-compose up -d
