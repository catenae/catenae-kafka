#!/usr/bin/env python
# -*- coding: utf-8 -*-

from queue import Queue


class LinkQueue(Queue):
    def __init__(self, minimum_messages=1, messages_left=None):
        if messages_left is None:
            messages_left = minimum_messages
        self.minimum_messages = minimum_messages
        self.messages_left = messages_left
        super().__init__(maxsize=-1)
