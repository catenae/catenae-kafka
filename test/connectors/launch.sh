#!/bin/bash
cd ..; ./build.sh; cd connectors
docker-compose up -d
