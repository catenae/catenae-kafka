#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class Reducer(Link):
    def setup(self):
        self.counts = dict()

    def print_result(self):
        self.logger.log('=== Updated result ===')
        for kv in self.counts.items():
            self.logger.log(kv)
        self.logger.log()

    def reduce(self, wordcount):
        word, count = wordcount
        if not word in self.counts:
            self.counts[word] = count
        else:
            self.counts[word] += count

    def transform(self, electron):
        self.reduce(electron.value)
        self.print_result()


if __name__ == "__main__":
    Reducer().start()