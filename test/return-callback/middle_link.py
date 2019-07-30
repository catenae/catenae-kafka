#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0

    def callback_method(self, value):
        self.logger.log(f'callback_method("{value}") executed')

    def transform(self, electron):
        self.logger.log(f'Received: {electron.value}')
        return "uno", self.callback_method, [electron.value]


if __name__ == "__main__":
    MiddleLink(synchronous=True).start()
