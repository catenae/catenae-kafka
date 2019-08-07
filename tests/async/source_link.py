#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
from random import randint


class SourceLink(Link):
    def setup(self):
        self.message_count = 0

    def generator(self):
        self.send(self.message_count)
        self.logger.log(f'Message "{self.message_count}" sent')
        self.message_count += 1
        time.sleep(0.01)


if __name__ == "__main__":
    SourceLink().start()
