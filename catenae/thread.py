#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading


class Thread(threading.Thread):
    def __init__(self, **kwargs):
        super(Thread, self).__init__(**kwargs)
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.is_set()