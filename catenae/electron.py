#!/usr/bin/env python
# -*- coding: utf-8 -*-


class Electron(object):

    def __init__(self, key, value, keep_key=True,
                 topic=None, previous_topic=None):
        self.key = key
        self.value = value
        self.keep_key = keep_key
        self.topic = topic # Destiny topic
        self.previous_topic = previous_topic
