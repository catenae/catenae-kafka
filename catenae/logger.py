#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


class Logger:
    def __init__(self, instance, log_level):
        self.instance = instance
        logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

    def log(self, message, level='info'):
        message = f'{self.instance.__class__.__name__}: {message}'
        getattr(logging, level.lower(), 'INFO')(message)