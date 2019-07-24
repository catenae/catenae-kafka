#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time


class SourceLink(Link):
    def setup(self):
        self.init = False

    def generator(self):
        self.logger.log(f'Invoking add_input()')
        self.rpc_call('MiddleLink', 'add_input', args=['topic2'])

        time.sleep(10)

        self.logger.log(f'Invoking remove_input()')
        self.rpc_call('MiddleLink', 'remove_input', args=['topic2'])

        self.suicide("Done!")


if __name__ == "__main__":
    SourceLink().start()