#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Electron:
    def __init__(self,
                 key=None,
                 value=None,
                 topic=None,
                 previous_topic=None,
                 unpack_if_string=False,
                 callbacks=None):
        self.key = key
        self.value = value
        self.topic = topic  # Destiny topic
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
        copy = self.copy()
        copy.topic = None
        copy.previous_topic = None
        copy.unpack_if_string = None
        copy.callbacks = None
        return copy

    def copy(self):
        try:
            return Electron(self.key, self.value.copy(), self.topic, self.previous_topic,
                            self.unpack_if_string, self.callbacks)
        except Exception:
            return Electron(self.key, self.value, self.topic, self.previous_topic,
                            self.unpack_if_string, self.callbacks)
