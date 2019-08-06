#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class MiddleLink(Link):
    def setup(self):
        self.counter = 0

    def transform(self, electron):
        self.logger.log(f'Received message #{electron.value}')
        assert self.counter == electron.value
        self.counter += 1


if __name__ == "__main__":
    MiddleLink(synchronous=True).start()
