#!/bin/bash
docker rm -f kafka-catenae

docker run -d --net=host  \
--ulimit nofile=90000:90000 \
--name kafka-catenae \
catenae/kafka

python source_link.py -o input1 -b 127.0.0.1:9092 > /dev/null &
python source_link.py -o input2 -b 127.0.0.1:9092 > /dev/null &
python leaf_link.py -i input3 -b 127.0.0.1:9092 > /dev/null &
python middle_link.py -i input1 -o input3 -b 127.0.0.1:9092
