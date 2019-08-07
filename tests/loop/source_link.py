#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
from random import randint


class SourceLink(Link):
    def setup(self):
        self.loop(self.loop_target_one, interval=1, args='one', wait=True)
        self.loop(self.loop_target_two_three, interval=2, args=['two', 82])
        self.loop(self.loop_target_two_three, interval=4, kwargs={'name': 'three', 'value': 82})
        self.loop(self.loop_target_four, interval=8, args=['four'], kwargs={'value': 83})

    def loop_target_one(self, name, value=None):
        self.logger.log(f'Hello from {name}')

    def loop_target_two_three(self, name, value=None):
        self.logger.log(f'Hello from {name}')
        assert value == 82

    def loop_target_four(self, name, value=None):
        self.logger.log(f'Hello from {name}')
        assert value == 83


if __name__ == "__main__":
    SourceLink().start()
