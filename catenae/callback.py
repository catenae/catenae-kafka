#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import logging


class Callback:

    COMMIT_KAFKA_MESSAGE = 0

    def __init__(self, target=None, args=None, kwargs=None, mode=None):
        self.target = target
        self.mode = mode

        if args is None:
            self.args = []
        else:
            self.args = args

        if kwargs is None:
            self.kwargs = {}
        else:
            self.kwargs = kwargs

    def __bool__(self):
        if self.target != None:
            return True
        return False

    def execute(self):
        if not self:
            logging.error('callback without target.')
            return

        if self.mode == self.COMMIT_KAFKA_MESSAGE:
            self._execute_kafka_commit()
            return

        self._execute()

    def _execute(self):
        self.target(*self.args, **self.kwargs)

    def _execute_kafka_commit(self):
        done = False
        attempts = 0
        while not done:
            try:
                self._execute()
                done = True
            except Exception as e:
                if 'UNKNOWN_MEMBER_ID' in str(e):
                    raise Exception('Cannot commit a message (timeout).')
                logging.exception(f'Trying to commit a message ({attempts})...')
                attempts += 1
                time.sleep(1)
