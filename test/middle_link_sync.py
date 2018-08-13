#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, util
import logging
import random


def callback_noargs():
    logging.debug(f'Callback called')

def callback_args(arg1=None, arg2=None):
    logging.debug(f'Callback called, args: {arg1}, {arg2}')

class MiddleLinkSync(Link):
    def instance_callback_noargs(self):
        logging.debug(f'{self.__class__.__name__} -> Callback called')

    def instance_callback_args(self, arg1, arg2):
        logging.debug(f'{self.__class__.__name__} -> Callback called, args: {arg1}, {arg2}')

    def setup(self):
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(f'{self.__class__.__name__} -> setup()')
        logging.debug(f'{self.__class__.__name__} -> input_topics: {self.input_topics}')
        logging.debug(f'{self.__class__.__name__} -> output_topics: {self.output_topics}')

    def transform(self, electron):
        logging.debug(f'{self.__class__.__name__} -> transform()')
        logging.debug(f'{self.__class__.__name__} -> received key: {electron.key}, value: {electron.value}')
        electron.key = electron.key + '_transformed_sync'
        electron.value = electron.value + '_transformed_sync'
        logging.debug(f'{self.__class__.__name__} -> previous topic: {electron.previous_topic}')

        if random.randint(0,100) == 84:
            if "input2" not in self.input_topics:
                self.add_input_topic("input2")
                logging.debug(f'{self.__class__.__name__} -> INPUT CHANGED {self.input_topics}')
        # elif random.randint(0,100) == 84:
        #     self.remove_input_topic("input2")
        #     logging.debug(f'{self.__class__.__name__} -> INPUT CHANGED {self.input_topics}')

        option = random.randint(0,6)
        if option == 0:
            return electron
        elif option == 1:
            return electron, callback_noargs
        elif option == 2:
            return electron, callback_args, ['arg_val1', 'arg_val2']
        elif option == 3:
            return electron, callback_args, {'arg1': 'kwarg_val1', 'arg2': 'kwarg_val2'}
        elif option == 4:
            return electron, self.instance_callback_noargs
        elif option == 5:
            return electron, self.instance_callback_args, ['instance_arg_val1', 'instance_arg_val2']
        elif option == 6:
            return electron, self.instance_callback_args, {'arg1': 'instance_kwarg_val1', 'arg2': 'instance_kwarg_val2'}


if __name__ == "__main__":
    MiddleLinkSync().start(consumer_group='custom_group_2', synchronous=True)
