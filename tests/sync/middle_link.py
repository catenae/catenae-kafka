#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link
import logging
import random


class MiddleLink(Link):
    def transform(self, electron):
        number = electron.value
        self.logger.log(f'Received: {number}')
        if random.randint(0, 20) == 7:
            self.suicide('rebooting...')
        self.logger.log(f'Processed: {number}')


if __name__ == "__main__":
    MiddleLink(synchronous=True).start()
