#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
from queue import Queue


class Thread(threading.Thread):
    def __init__(self, **kwargs):
        super(Thread, self).__init__(**kwargs)
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.is_set()

class ThreadPool:
    def __init__(self, link_instance, num_threads=1):
        self.link_instance = link_instance
        self.tasks_queue = Queue()

        self.threads = []
        for i in range(num_threads):
            thread = Thread(target=self._worker_target, args=[i])
            self.threads.append(thread)
            thread.start()

    def submit(self, target, args=None, kwargs=None):
        self.tasks_queue.put((target, args, kwargs))

    def _worker_target(self, i):
        while not self.threads[i].stopped():
            try:
                target, args, kwargs = self.tasks_queue.get()
                if args:
                    target(*args)
                elif kwargs:
                    target(**kwargs)
                else:
                    target()
            except Exception:
                self.link_instance.logger.log(
                    f'Exception during the execution of "{target.__name__}".',
                    level='exception')