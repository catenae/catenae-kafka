#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
from random import randint


class SourceLink(Link):
    def generator(self):
        number = randint(0, 1000)
        self.logger.log(f'randint: {number}')
        self.rocksdb.put('test_key', number)
        self.logger.log(f"rocksdb[test_key]: {self.rocksdb.get('test_key')}")
        time.sleep(1)


if __name__ == "__main__":
    SourceLink(rocksdb_path='/tmp/rocksdb').start()
