#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, errors
import time


class SourceLink(Link):
    def generator(self):
        try:
            instance_uid = self.instances['by_group']['catenae_middlelink'][0]
            self.logger.log(f'Invoking plus_two() from instance {self.uid}')
            result = self.rpc_call(instance_uid, 'plus_two', kwargs={'number': 18})
            self.logger.log(f'result: {result}')
            assert result == 20

        except (KeyError, IndexError):
            self.logger.log('MiddleLink not yet available')
            time.sleep(1)

        except errors.RPCError:
            self.logger.log(level='exception')

    def transform(self, electron):
        self.logger.log(f'Received message: {electron.value}')


if __name__ == "__main__":
    SourceLink().start()