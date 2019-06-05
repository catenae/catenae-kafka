# About

Catenae is a Python library for **building distributed Stream Processing applications in minutes**. An application is usually composed of multiple micromodules (Python scripts) that can be interconnected through named streams. Catenae-based systems can scale up horizontally by increasing the number of instances of any micromodule without further configuration. The communication between micromodules is managed by Apache Kafka.

Every module can consume and/or produce messages on one or multiple streams (Kafka topics). Catenae is a Python client library for Kafka that facilitates common tasks and whose main goal is to make you forget about Kafka.

Despite it is focused on Python developments, a Catenae topology can interoperate with any Kafka producer or consumer. This can be trivially achieved when sending messages in string format on the external Kafka producers and analogously on external Kafka consumers.

Among Catenae's main functionalities are:
- RPC invocations among micromodules by instance or class
- Priority support for multiple input streams
- Sequential and synchronous modes for critical systems
- Custom wrappers for MongoDB and Aerospike clients
- Transparent serialization and deserialization for Python objects

# Docs
To-Do

# Example 1: Filter
Try it at [`examples/filter`](https://github.com/catenae/catenae/tree/develop/examples/filter)

## Micromodules

### Streamer
This module streams a line of the defined text every two seconds.
```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time


class Streamer(Link):
    def setup(self):
        self.lines = ("A purely peer-to-peer version of electronic cash would allow online " \
                      "payments to be sent directly from one party to another without going " \
                      "through a financial institution. Digital signatures provide part of the " \
                      "solution, but the main benefits are lost if a trusted third party is still "\
                      "required to prevent double-spending. We propose a solution to the "\
                      "double-spending problem using a peer-to-peer network. The network "\
                      "timestamps transactions by hashing them into an ongoing chain of "\
                      "hash-based proof-of-work, forming a record that cannot be changed without "\
                      "redoing the proof-of-work. The longest chain not only serves as proof of "\
                      "the sequence of events witnessed, but proof that it came from the largest "\
                      "pool of CPU power. As long as a majority of CPU power is controlled by "\
                      "nodes that are not cooperating to attack the network, they'll generate the "\
                      "longest chain and outpace attackers. The network itself requires minimal "\
                      "structure. Messages are broadcast on a best effort basis, and nodes can "\
                      "leave and rejoin the network at will, accepting the longest proof-of-work "\
                      "chain as proof of what happened while they were gone.").split('.')

    def generator(self):
        for line in self.lines:
            self.send(line.strip())
        time.sleep(2)


if __name__ == "__main__":
    Streamer().start()
```

### Filter
```python
from catenae import Link, Electron


class Filter(Link):
    def setup(self):
        self.allowed_words = set(['CPU', 'proof-of-work', 'chain', 'nodes'])

    def transform(self, electron):
        line = electron.value
        words = line.split()
        if set(words).intersection(self.allowed_words):
            self.logger.log(line)


if __name__ == "__main__":
    Filter().start()
```

## Deployment
```yaml
version: '3.4'

x-logging: &default-logging
  options:
    max-size: '50m'
    max-file: '1'
  driver: json-file

services:

  kafka:
    image: catenae/kafka
    logging: *default-logging

  streamer:
    image: catenae/link:develop
    command: streamer.py -o filter_input -k kafka:9092
    working_dir: /opt/catenae/examples/filter
    restart: always
    depends_on:
      - kafka

  filter:
    image: catenae/link:develop
    command: filter.py -i filter_input -k kafka:9092
    working_dir: /opt/catenae/examples/filter
    restart: always
    depends_on:
      - kafka
```

```bash
docker-compose up -d
```

# Example 2: Word Count
Try it at [`examples/wordcount`](https://github.com/catenae/catenae/tree/develop/examples/wordcount)

## Micromodules

### Streamer
> The same script of the Example 1 will be used.

### Mapper
This module can be launched multiple times. It returns a tuple for every received line with the number of occurrences of each word. The streamed lines will be distributed among the existing instances automatically.

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class Mapper(Link):
    def get_counts(self, line):
        counts = dict()
        for word in line.split():
            word_lower = word.lower()
            word_clean = word.replace(',', '')
            if not word_clean in counts:
                counts[word_clean] = 1
            else:
                counts[word_clean] += 1
        for kv in counts.items():
            yield kv

    def transform(self, electron):
        for kv in self.get_counts(electron.value):
            self.logger.log(f'Tuple {kv} sent')
            self.send(kv)


if __name__ == "__main__":
    Mapper().start()
```

### Reducer
The last module of this topology will be instantiated only once and it is in charge of aggregating the stream of local counts emitted from the Mapper instances in real time. For every received tuple it will show the updated result.
```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class Reducer(Link):
    def setup(self):
        self.counts = dict()

    def print_result(self):
        self.logger.log('=== Updated result ===')
        for kv in self.counts.items():
            self.logger.log(kv)
        self.logger.log()

    def reduce(self, wordcount):
        word, count = wordcount
        if not word in self.counts:
            self.counts[word] = count
        else:
            self.counts[word] += count

    def transform(self, electron):
        self.reduce(electron.value)
        self.print_result()


if __name__ == "__main__":
    Reducer().start()
```

## Deployment
```yaml
version: '3.4'

x-logging: &default-logging
  options:
    max-size: '50m'
    max-file: '1'
  driver: json-file

services:

  kafka:
    image: catenae/kafka
    logging: *default-logging

  streamer:
    image: catenae/link:develop
    command: streamer.py -o mapper_input -k kafka:9092
    working_dir: /opt/catenae/examples/wordcount
    restart: always
    depends_on:
      - kafka

  mapper:
    image: catenae/link:develop
    command: mapper.py -i mapper_input -o reducer_input -k kafka:9092
    working_dir: /opt/catenae/examples/wordcount
    restart: always
    depends_on:
      - kafka

  reducer:
    image: catenae/link:develop
    command: reducer.py -i reducer_input -k kafka:9092
    working_dir: /opt/catenae/examples/wordcount
    restart: always
    depends_on:
      - kafka
```

```bash
docker-compose up -d --scale mapper=4
```

# References
> A Micromodule Approach for Building Real-Time Systems with Python-Based Models: Application to Early Risk Detection of Depression on Social Media — R Martínez-Castaño, JC Pichel, DE Losada, F Crestani — European Conference on Information Retrieval, 2018 — [PDF](https://dev.brunneis.com/documents/Papers/ECIR%202018/A%20Micromodule%20Approach%20for%20Building%20Real-Time%20Systems%20with%20Python-Based%20Models:%20Application%20to%20Early%20Risk%20Detection%20of%20Depression%20on%20Social%20Media.pdf)

> Building Python-Based Topologies for Massive Processing of Social Media Data in Real Time — R Martínez-Castaño, JC Pichel, DE Losada — Proceedings of the 5th Spanish Conference on Information Retrieval, 2018
