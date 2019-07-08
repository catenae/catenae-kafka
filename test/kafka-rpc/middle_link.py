#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, rpc


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0

    @rpc
    def plus_two(self, context, number=0):
        self.logger.log(f"method plus_two invoked by {context['uid']}; result: {number + 2}")


if __name__ == "__main__":
    MiddleLink().start()
