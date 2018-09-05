#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, util
import time
import logging


class SourceLink(Link):
    def setup(self):
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(f'{self.__class__.__name__} -> setup()')
        logging.debug(f'{self.__class__.__name__} -> output_topics: {self.output_topics}')
        self.message_count = 0

    def custom_input(self):
        logging.debug(f'{self.__class__.__name__} -> custom_input()')
        while True:
            logging.debug(f'{self.__class__.__name__} -> message sent')
            self.queue.put(Electron(
                'source_key',
                f'source_value_{self.message_count}',
                topic=self.output_topics[0]
            ))
            self.message_count += 1
            time.sleep(1)

if __name__ == "__main__":
    SourceLink().start(link_mode=Link.CUSTOM_INPUT)
