#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import traceback
import logging


def get_current_timestamp():
    return int(datetime.datetime.today().timestamp())

class format:
    RED_FG = '\033[91m'
    WHITE_BG = '\33[107m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_error(instance, message, fatal=False):
    message = format.RED_FG + format.WHITE_BG + format.BOLD \
        + 'Error at ' + instance.__class__.__name__ \
        + '\nMessage: ' + message + format.END + '\n\n'
    if fatal:
        logging.critical(message)
        raise SystemExit
    logging.error(message)

def print_exception(instance, message, fatal=False):
    traceback.print_exc()
    print_error(instance, message, fatal)
