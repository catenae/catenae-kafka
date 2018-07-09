#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import traceback
import logging
import os


def get_current_timestamp():
    return int(datetime.datetime.today().timestamp())

def print_error(instance, message, fatal=False):
    message = 'Error at ' + instance.__class__.__name__ \
        + '\nMessage: ' + message + '\n\n'
    if fatal:
        logging.critical(message)
        os._exit(1)
    logging.error(message)

def print_exception(instance, message, fatal=False):
    traceback.print_exc()
    print_error(instance, message, fatal)
