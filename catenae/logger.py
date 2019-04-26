#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


class Logger:
    def __init__(self, instance, level='info'):
        self.instance = instance
        logging.getLogger().setLevel(getattr(logging, level, logging.INFO))

    def log(self, message='', level='info'):
        if message:
            message = f'{self.instance.__class__.__name__}: {message}'
        getattr(logging, level.lower(), 'INFO')(message)