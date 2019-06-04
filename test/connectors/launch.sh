#!/bin/bash
current_dir="$(pwd)"
cd ../../docker && ./build.sh
cd $current_dir
docker-compose down
docker-compose up -d
sleep 5
echo "Aerospike connector test: "$(docker logs aerospike_test)
echo "MongoDB connector test: "$(docker logs mongodb_test)
docker rm -f aerospike_test mongodb_test 2>&1 > /dev/null
