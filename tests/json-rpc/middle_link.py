#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc


class MiddleLink(Link):
    def setup(self):
        self.loop(self.send, 'ping', interval=.5)

    @rpc
    def plus_two(self, number=0):
        self.logger.log(f'method plus_two invoked')
        return number + 2

    @rpc
    def fail(self):
        return -32000, "Custom error"


if __name__ == "__main__":
    MiddleLink().start()