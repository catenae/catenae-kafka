#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threading import Lock
import time
from .utils import get_timestamp


class CustomQueue:
    BLOCK_SECONDS = 0.1

    class EmptyError(Exception):
        def __init__(self, message=None):
            if message == None:
                message = 'The queue is empty'
            super(CustomQueue.EmptyError, self).__init__(message)

    def __init__(self, size=0, circular=False):
        self._size = size
        self._circular = circular
        self._queue = list()
        self._lock = Lock()

    def _truncate(self):
        if self._size > 0 and len(self._queue) > self._size:
            self._queue.pop(0)

    def put(self, item, block=True, timeout=None):
        if self._circular:
            self._lock.acquire()
            self._queue.append(item)
            self._truncate()
            self._lock.release()
            return

        start_timestamp = get_timestamp()
        while timeout == None or get_timestamp() - start_timestamp < timeout:
            self._lock.acquire()
            if self._size <= 0 or len(self._queue) < self._size:
                self._queue.append(item)
                self._lock.release()
                return
            self._lock.release()
            if not block:
                raise CustomQueue.EmptyError
            time.sleep(CustomQueue.BLOCK_SECONDS)

    def get(self, block=True, timeout=None):
        start_timestamp = get_timestamp()
        while timeout == None or get_timestamp() - start_timestamp < timeout:
            self._lock.acquire()
            if len(self._queue) > 0:
                item = self._queue.pop(0)
                self._lock.release()
                return item
            self._lock.release()
            if not block:
                raise CustomQueue.EmptyError
            time.sleep(CustomQueue.BLOCK_SECONDS)


class LinkQueue(CustomQueue):
    def __init__(self, minimum_messages=1, messages_left=None):
        if messages_left is None:
            messages_left = minimum_messages
        self.minimum_messages = minimum_messages
        self.messages_left = messages_left
        super().__init__()