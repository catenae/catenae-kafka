#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron
import time
import logging


class SourceLink(Link):
    def setup(self):
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(f'{self.__class__.__name__} -> setup()')
        logging.debug(f'{self.__class__.__name__} -> output_topics: {self.output_topics}')
        self.message_count = 0

    def generator(self):

        # If a key is assigned, all the messages with the same key will
        # be assigned to the same partition and, thus, to the same consumer
        # within a consumer group. If the key is left empty, the messages
        # will be distributed in a round-robin fashion.

        logging.debug(f'{self.__class__.__name__} -> custom_input()')
        while True:
            electron = Electron('source_key_1',
                                f'source_value_{self.message_count}')
            self.send(electron)
            logging.debug(f'{self.__class__.__name__} -> keyed message sent')

            electron = Electron('source_key_2',
                                f'source_value_{self.message_count}')
            self.send(electron)
            logging.debug(f'{self.__class__.__name__} -> keyed message sent')

            electron = Electron(value=f'non_keyed_source_value_{self.message_count}')
            self.send(electron)
            logging.debug(f'{self.__class__.__name__} -> keyed message sent')

            # Send directly a string which won't be encapsulated in an Electron internally by Catenae
            string = 'simple string'
            self.send(string)
            logging.debug(f'{self.__class__.__name__} -> Simple string sent')

            # Send a list of strings in the same way
            self.send([string, string, string])
            logging.debug(f'{self.__class__.__name__} -> Array of simple strings sent')

            # Send directly any object without encapsuling it in an Electron instance
            dict_ = {'dict_key': 'dict_value'}
            self.send(dict_)
            logging.debug(f'{self.__class__.__name__} -> Object sent')

            # Again, but with a custom output topic
            self.send(dict_, topic='input1')
            logging.debug(f'{self.__class__.__name__} -> Object sent with custom topic')

            # Send a list of non-electron objects
            self.send([dict_, dict_, dict_])
            logging.debug(f'{self.__class__.__name__} -> Array of objects sent')

            # Send a list of Electrons
            electron = Electron(value=dict_)
            self.send([electron, electron, electron])
            logging.debug(f'{self.__class__.__name__} -> Array of electrons sent')

            self.message_count += 1
            time.sleep(1)


if __name__ == "__main__":
    SourceLink().start(link_mode=Link.CUSTOM_INPUT)
