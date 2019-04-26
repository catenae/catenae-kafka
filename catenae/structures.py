#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict
from orderedset import OrderedSet


class CircularOrderedDict(OrderedDict):
    def __init__(self, size=0):
        super(CircularOrderedDict, self).__init__()
        self.size = size

    def __setitem__(self, key, value):
        super(CircularOrderedDict, self).__setitem__(key, value)
        self._truncate()

    def _truncate(self):
        if len(self) > self.size:
            self.popitem(last=False)


class CircularOrderedSet(OrderedSet):
    def __init__(self, size=0):
        super(CircularOrderedSet, self).__init__()
        self.size = size

    def add(self, value):
        super(CircularOrderedSet, self).add(value)
        self._truncate()

    def _truncate(self):
        if len(self) > self.size:
            self.pop(last=False)
