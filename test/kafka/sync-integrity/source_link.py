#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
import logging


class SourceLink(Link):
    def setup(self):
        self.message_count = 0

    def generator(self):
        while True:
            self.message_count += 1
            time.sleep(1)
            self.send(self.message_count)


if __name__ == "__main__":
    SourceLink(link_mode=Link.CUSTOM_INPUT, sequential=True).start()
