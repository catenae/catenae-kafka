#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
from random import randint


class SourceLink(Link):
    def setup(self):
        self.loop(self.input1_producer)
        self.loop(self.input2_producer)
        self.loop(self.input3_producer)

    def input1_producer(self):
        self.send(randint(0, 1000), topic='input1')
        time.sleep(1)

    def input2_producer(self):
        self.send(randint(0, 1000), topic='input2')
        time.sleep(2)

    def input3_producer(self):
        self.send(randint(0, 1000), topic='input3')
        time.sleep(3)


if __name__ == "__main__":
    SourceLink().start()
