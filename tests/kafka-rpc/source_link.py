#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time


class SourceLink(Link):
    def generator(self):
        self.logger.log(f'Invoking plus_two()')
        self.rpc_notify('plus_two', kwargs={'number': 8}, to='MiddleLink')
        time.sleep(1)


if __name__ == "__main__":
    SourceLink().start()