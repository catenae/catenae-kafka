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
            try:
                message = self._p2p_jsonrpc_queue.get()
                self.logger.log(f"{type(message)}: {message}")
            except Exception:
                continue


if __name__ == "__main__":
    SourceLink(synchronous=True).start()
