#!/usr/bin/env python
# -*- coding: utf-8 -*-

from orderedset import OrderedSet
from collections import OrderedDict


class CircularOrderedDict(OrderedDict):
    def __init__(self, size):
        super(CircularOrderedDict, self).__init__()
        self.size = size
        self._truncate()

    def __setitem__(self, key, value):
        super(CircularOrderedDict, self).__setitem__(key, value)
        self._truncate()

    def _truncate(self):
        # self.size == None => no limit
        if self.size is not None:
            while len(self) > self.size:
                # The first element is removed
                self.popitem(last=False)

class CircularOrderedSet(OrderedSet):
    def __init__(self, size):
        super(CircularOrderedSet, self).__init__()
        self.size = size
        self._truncate()

    def add(self, value):
        super(CircularOrderedSet, self).add(value)
        self._truncate()

    def _truncate(self):
        # self.size == None => no limit
        if self.size is not None:
            while len(self) > self.size:
                # The first element is removed
                self.pop(last=False)
