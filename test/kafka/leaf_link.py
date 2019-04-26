#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link
import logging


class LeafLink(Link):
    def setup(self):
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(f'{self.__class__.__name__} -> setup()')
        logging.debug(f'{self.__class__.__name__} -> input_topics: {self.input_topics}')

    def transform(self, electron):
        logging.debug(f'{self.__class__.__name__} -> transform()')
        logging.debug(f'{self.__class__.__name__} -> received key: {electron.key}, value: {electron.value}')
        logging.debug(f'{self.__class__.__name__} -> previous topic: {electron.previous_topic}')


if __name__ == "__main__":
    LeafLink(log_level='DEBUG', link_mode=Link.CUSTOM_OUTPUT, uid_consumer_group=True).start()
