#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import logging


class SourceLink(Link):
    def generator(self):
        message = 'hi'
        self.send(message)
        self.logger.log(f'Message "{message}" sent')

        self.suicide()
        assert False


if __name__ == "__main__":
    SourceLink().start()
