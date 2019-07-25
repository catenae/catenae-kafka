#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0

    def transform(self, electron):
        self.logger.log(f'Received: {electron.value}')
        assert electron.value == 'message 0'
        self.suicide('rebooting link...', relaunch=True)


if __name__ == "__main__":
    MiddleLink(synchronous=True).start()
