#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron


class Mapper(Link):
    def get_counts(self, line):
        counts = dict()
        for word in line.split():
            word_lower = word.lower()
            word_clean = word.replace(',', '')
            if not word_clean in counts:
                counts[word_clean] = 1
            else:
                counts[word_clean] += 1
        for kv in counts.items():
            yield kv

    def transform(self, electron):
        for kv in self.get_counts(electron.value):
            self.logger.log(f'Tuple {kv} sent')
            self.send(kv)


if __name__ == "__main__":
    Mapper().start()