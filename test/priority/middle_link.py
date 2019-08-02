#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, utils


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0
        self.start_timestamp = utils.get_timestamp()

    def transform(self, electron):
        self.logger.log(f'{electron.previous_topic} -> {electron.value}')


if __name__ == "__main__":
    MiddleLink(input_mode='exp').start()
