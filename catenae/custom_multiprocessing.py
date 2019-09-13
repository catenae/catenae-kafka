#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing


class Process(multiprocessing.Process):
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
        self._will_stop = multiprocessing.Event()

    def stop(self):
        self._will_stop.set()

    @property
    def will_stop(self):
        return self._will_stop.is_set()


class ProcessPool:
    def __init__(self, link_instance, num_processes=1):
        self.link_instance = link_instance
        self.tasks_queue = multiprocessing.Queue()
        self.processes = []

        for i in range(num_processes):
            process = Process(self._worker_target, i)
            self.processes.append(process)
            process.start()

    def submit(self, target, args=None, kwargs=None):
        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        self.tasks_queue.put((target, args, kwargs))

    def _worker_target(self, index):
        while not self.processes[index].will_stop:
            try:
                target, args, kwargs = self.tasks_queue.get()
                target(*args, **kwargs)
            except Exception:
                self.link_instance.logger.log(f'exception during the execution of a task',
                                              level='exception')