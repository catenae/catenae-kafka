#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy


class Electron:
    def __init__(self,
                 key=None,
                 value=None,
                 topic=None,
                 previous_topic=None,
                 unpack_if_string=False,
                 callbacks=None,
                 timestamp=None):
        self.key = key
        self.value = value
        self.topic = topic  # Destination topic
        self.previous_topic = previous_topic
        self.unpack_if_string = unpack_if_string
        if callbacks is None:
            self.callbacks = []
        else:
            self.callbacks = callbacks
        self.timestamp = timestamp

    def __bool__(self):
        if self.value != None:
            return True
        return False

    def get_sendable(self):
        copy = self.copy()
        copy.topic = None
        copy.previous_topic = None
        copy.unpack_if_string = False
        copy.callbacks = []
        copy.timestamp = None
        return copy

    def copy(self):
        electron = Electron()
        electron.key = self.key
        electron.value = copy.deepcopy(self.value)
        electron.topic = self.topic
        electron.previous_topic = self.previous_topic
        electron.unpack_if_string = self.unpack_if_string
        electron.callbacks = self.callbacks
        electron.timestamp = self.timestamp
        return electron
