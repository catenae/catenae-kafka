#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Electron(object):

    def __init__(self,
                 key=None,
                 value=None,
                 keep_key=True,
                 topic=None,
                 previous_topic=None,
                 unpack_if_string=False):
        self.key = key
        self.value = value
        self.keep_key = keep_key
        self.topic = topic # Destiny topic
        self.previous_topic = previous_topic
        self.unpack_if_string = unpack_if_string

    def copy(self):
        try:
            return Electron(self.key,
                            self.value.copy(),
                            self.keep_key,
                            self.topic,
                            self.previous_topic,
                            self.unpack_if_string)
        except Exception:
            return Electron(self.key,
                            self.value,
                            self.keep_key,
                            self.topic,
                            self.previous_topic,
                            self.unpack_if_string)