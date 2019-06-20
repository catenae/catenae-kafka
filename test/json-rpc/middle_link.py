#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
import logging


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0

    def plus_two(self, number=0):
        return number + 2

    def transform(self, electron):
        self.logger.log(f'Received: {electron.value}')


if __name__ == "__main__":
    MiddleLink().start()
