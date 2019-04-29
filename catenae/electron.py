#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy


class Electron:
    def __init__(self, key=None, value=None, topic=None, previous_topic=None, unpack_if_string=False, callbacks=None):
        self.key = key
        self.value = value
        self.topic = topic  # Destination topic
        self.previous_topic = previous_topic
        self.unpack_if_string = unpack_if_string
        if callbacks == None:
            self.callbacks = []
        else:
            self.callbacks = callbacks

    def __bool__(self):
        if self.value != None:
            return True
        return False

    def get_sendable(self):
        copy = self.deepcopy()
        copy.topic = None
        copy.previous_topic = None
        copy.unpack_if_string = False
        copy.callbacks = []
        return copy

    def deepcopy(self):
        electron = Electron()
        electron.key = self.key
        electron.value = copy.deepcopy(self.value)
        electron.topic = self.topic
        electron.previous_topic = self.previous_topic
        electron.unpack_if_string = self.unpack_if_string
        self.callbacks = []
        return electron

    def copy(self):
        return Electron(self.key, self.value, self.topic, self.previous_topic, self.unpack_if_string, self.callbacks)
