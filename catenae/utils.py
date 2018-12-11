#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
import logging
import os
from web3 import Web3
import json
import time
from deprecated import deprecated


@deprecated
def get_current_timestamp():
    return get_timestamp()

def get_timestamp():
    return int(round(time.time()))

def get_timestamp_ms():
    return int(round(time.time() * 1000))

def keccak256(item):
    return Web3.sha3(text=str(item)).hex()[2:]

def dump_ordered_dict(dict):
    return json.dumps(dict, separators=(',', ':'))

def load_ordered_dict(str_dict):
    return json.loads(str_dict, object_pairs_hook=OrderedDict)

def get_tuples_from_dict(dictionary):
    for key, value in dictionary.items():
        if type(value) == dict:
            yield from get_tuples_from_dict(value)
        else:
            yield key, value

def print_error(instance, message, fatal=False):
    message = 'Error at ' + instance.__class__.__name__ \
        + '. Message: ' + message
    if fatal:
        logging.critical(message)
        os._exit(1)
    logging.error(message)

def print_exception(instance, message, fatal=False):
    traceback.print_exc()
    print_error(instance, message, fatal)