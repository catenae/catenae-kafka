#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc


class MiddleLink(Link):
    @rpc
    def plus_two(self, number=0):
        self.logger.log(f'method plus_two invoked')
        return number + 2


if __name__ == "__main__":
    MiddleLink().start()