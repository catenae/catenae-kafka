#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link


class MiddleLink(Link):
    def setup(self):
        self.unseen = set()
        self.seen = set()
        self.counter = 0

    def process_value(self, value):
        self.seen.add(value)
        if value in self.unseen:
            self.unseen.remove(value)

        if self.counter != value:
            for i in range(value):
                if i not in self.seen:
                    self.unseen.add(i)
        self.counter += 1

    def transform(self, electron):
        self.logger.log(f'Received: {electron.value}')
        self.process_value(electron.value)
        self.logger.log(f'missing: {list(self.unseen)}')


if __name__ == "__main__":
    MiddleLink().start()
