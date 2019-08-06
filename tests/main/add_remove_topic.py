#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link
import logging
import random
import time


class AddRemoveTopic(Link):
    def setup(self):
        self.good_topics = set()
        self.known_topics = ['input1', 'input2', 'input3']
        self.add_input_topic(self.known_topics[0])

        self.step = 0

    def add_one_topic(self):
        self.add_input_topic('input1')

    def transform(self, electron):
        self.logger.log(f'step: {self.step}')
        self.logger.log(f'previous_topic: {electron.previous_topic}')
        self.logger.log(f'input_topics: {self.input_topics}')

        if self.step == 0:
            self.add_input_topic('input2')
            if not hasattr(self, 'step_one_exit_countdown'):
                self.step_one_exit_countdown = 50
            else:
                self.step_one_exit_countdown -= 1
                if self.step_one_exit_countdown == 0:
                    self.step = 1
        elif self.step == 1:
            if not hasattr(self, 'step_two_exit_countdown'):
                self.step_two_exit_countdown = 50
            else:
                self.step_two_exit_countdown -= 1
                if self.step_two_exit_countdown == 0:
                    self.remove_input_topic('input1')
                    self.step = 2
        elif self.step == 2:
            self.remove_input_topic('input2')
            self.step = 3


if __name__ == "__main__":
    AddRemoveTopic().start()
