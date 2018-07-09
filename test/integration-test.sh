#!/bin/bash
docker rm -f kafka-catenae

docker run -d --net=host  \
--ulimit nofile=90000:90000 \
--name kafka-catenae \
catenae/kafka

python source_link.py -o queue1 -b 127.0.0.1:9092 &
python middle_link.py -i queue1 -o queue2 -b 127.0.0.1:9092 &
python leaf_link.py -i queue2 -b 127.0.0.1:9092 &
