#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, utils


class MiddleLink(Link):
    def setup(self):
        self.message_count = 0
        self.start_timestamp = utils.get_timestamp()

    def transform(self, electron):
        current_timestamp = utils.get_timestamp()

        self.logger.log(f'Received message #{electron.value}')

        if (electron.value + 1) % 2534 == 0:
            self.suicide('rebooting...')

        self.message_count += 1
        messages_second = self.message_count / (current_timestamp - self.start_timestamp)
        self.logger.log(f'messages per second: {messages_second}')

        if self.message_count > 500:
            # 20 messages per second for 5 instances
            # => 4 messages per second in average
            assert messages_second > 3.5 and messages_second < 4.5


if __name__ == "__main__":
    MiddleLink().start()
