#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time


class SourceLink(Link):
    def setup(self):
        self.finished = False

    def generator(self):
        self.suicide()

    def finish(self):
        self.finished = True
        self.logger.log(f'finished: {self.finished}')


if __name__ == "__main__":
    SourceLink().start()
