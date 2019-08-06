#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time


class SourceLink(Link):
    def setup(self):
        self.counter = 0

    def generator(self):
        self.send(self.counter)
        self.counter += 1
        time.sleep(.05)


if __name__ == "__main__":
    SourceLink().start()
