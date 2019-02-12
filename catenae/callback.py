#!/usr/bin/env python
# -*- coding: utf-8 -*-

from . import utils
import time


class Callback(object):

    COMMIT_KAFKA_MESSAGE = 0

    def __init__(self, target=None, args=None, kwargs=None, type_=None):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.type_ = type_

    def __nonzero__(self):
        if self.target != None:
            return True
        return False

    def execute(self):
        if self.type_ == self.COMMIT_KAFKA_MESSAGE:
            return self._execute_commit_kafka_message_callback()

        if self.kwargs:
            self.target(**self.kwargs)
        elif self.args:
            self.target(*self.args)
        else:
            self.target()

    def _execute_commit_kafka_message_callback(self):
        done = False
        attempts = 0
        while not done:
            if attempts == 15:
                utils.print_error(self, 'Cannot commit a message. Exiting...', fatal=True)
            try:
                if self.kwargs:
                    self.target(**self.kwargs)
                elif self.args:
                    self.target(*self.args)
                else:
                    self.target()
                done = True
            except Exception as e:
                if 'UNKNOWN_MEMBER_ID' in str(e):
                    utils.print_error(self, 'Cannot commit a message (timeout). Exiting...', fatal=True)
                utils.print_exception(f'Trying to commit a message ({attempts})...')
                attempts += 1
                time.sleep(2)