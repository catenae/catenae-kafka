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
            self.send(self.message_count)
            self.logger.log(f'Message "{self.message_count}" sent')

            try:
                instance_uid = list(self.store['by_group']['catenae_middlelink'].keys())[0]
                self.logger.log(f'Invoking plus_two() from instance {instance_uid}')
                result = self.jsonrpc_call(instance_uid, 'plus_two', kwargs={'number': 8})
                assert result == 10

            except KeyError:
                pass

            time.sleep(1)


if __name__ == "__main__":
    SourceLink().start()