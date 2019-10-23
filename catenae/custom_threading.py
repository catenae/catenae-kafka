#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
from .custom_queue import ThreadingQueue
from .errors import EmptyError


def should_stop():
    return threading.current_thread().will_stop


class Thread(threading.Thread):
    def __init__(self, target, args=None, kwargs=None):
        if args is None:
            args = ()
        elif isinstance(args, list):
            args = tuple(args)
        elif not isinstance(args, tuple):
            args = ([args])

        if kwargs is None:
            kwargs = dict()

        super().__init__(target=target, args=args, kwargs=kwargs)
        self._will_stop = False

    def stop(self):
        self._will_stop = True

    @property
    def will_stop(self):
        return self._will_stop


class ThreadPool:
    def __init__(self, link_instance, num_threads=1):
        self.link_instance = link_instance
        self.tasks_queue = ThreadingQueue()
        self.threads = []

        for i in range(num_threads):
            thread = Thread(self._worker_target, i)
            self.threads.append(thread)
            thread.start()

    def submit(self, target, args=None, kwargs=None):
        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        self.tasks_queue.put((target, args, kwargs))

    def _worker_target(self, index):
        while not self.threads[index].will_stop:
            try:
                target, args, kwargs = self.tasks_queue.get(timeout=1, block=False)
                target(*args, **kwargs)
            except EmptyError:
                pass
            except Exception:
                self.link_instance.logger.log(f'exception during the execution of a task', level='exception')
