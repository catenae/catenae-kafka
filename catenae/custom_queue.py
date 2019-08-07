#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import multiprocessing
import time
from .utils import get_timestamp


class CustomQueue:
    BLOCKING_SECONDS = 0.1

    def __init__(self, size=0, circular=False):
        self._size = size
        self._circular = circular

    def put(self):
        pass

    def get(self):
        pass

    class EmptyError(Exception):
        def __init__(self, message=None):
            if message is None:
                message = 'The queue is empty'
            super().__init__(message)


class ThreadingQueue(CustomQueue):
    def __init__(self, size=0, circular=False):
        super().__init__(size, circular)
        self._queue = list()
        self._lock = threading.Lock()

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
        while timeout is None or get_timestamp() - start_timestamp < timeout:
            self._lock.acquire()
            if self._size <= 0 or len(self._queue) < self._size:
                self._queue.append(item)
                self._lock.release()
                return
            self._lock.release()
            if not block:
                raise ThreadingQueue.EmptyError
            time.sleep(ThreadingQueue.BLOCKING_SECONDS)

    def get(self, block=True, timeout=None):
        if timeout is not None:
            block = False

        start_timestamp = get_timestamp()
        while timeout is None or get_timestamp() - start_timestamp < timeout:
            self._lock.acquire()
            if len(self._queue) > 0:
                item = self._queue.pop(0)
                self._lock.release()
                return item

            self._lock.release()
            if timeout is None and not block:
                raise ThreadingQueue.EmptyError
            time.sleep(ThreadingQueue.BLOCKING_SECONDS)

        if not block:
            raise ThreadingQueue.EmptyError