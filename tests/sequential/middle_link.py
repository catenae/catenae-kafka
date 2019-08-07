#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link


class MiddleLink(Link):
    def setup(self):
        self.counter = 0

    def transform(self, electron):
        self.logger.log(f'Received: {electron.value}')
        assert electron.value == self.counter
        self.counter += 1


if __name__ == "__main__":
    MiddleLink().start()
