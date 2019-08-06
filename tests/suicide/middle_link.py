#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
import logging


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0

    def generator(self):
        self.logger.log("Hi from generator()")
        time.sleep(1)

    def transform(self, electron):
        self.logger.log(f'Received: {electron.value}')

        self.suicide()
        assert False


if __name__ == "__main__":
    MiddleLink().start()