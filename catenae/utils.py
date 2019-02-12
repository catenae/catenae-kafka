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
from collections import OrderedDict
from orderedset import OrderedSet


class CircularOrderedDict(OrderedDict):
    def __init__(self, size):
        super(CircularOrderedDict, self).__init__()
        self.size = size
        self._truncate()

    def __setitem__(self, key, value):
        super(CircularOrderedDict, self).__setitem__(key, value)
        self._truncate()

    def _truncate(self):
        # self.size == None => no limit
        if self.size is not None:
            while len(self) > self.size:
                # The first element is removed
                self.popitem(last=False)


class CircularOrderedSet(OrderedSet):
    def __init__(self, size):
        super(CircularOrderedSet, self).__init__()
        self.size = size
        self._truncate()

    def add(self, value):
        super(CircularOrderedSet, self).add(value)
        self._truncate()

    def _truncate(self):
        # self.size == None => no limit
        if self.size is not None:
            while len(self) > self.size:
                # The first element is removed
                self.pop(last=False)


def get_timestamp():
    return int(round(time.time()))

def get_timestamp_ms():
    return int(round(time.time() * 1000))

def keccak256(item):
    if type(item) != str:
        raise ValueError
    return Web3.sha3(text=item).hex()[2:]

def dump_dict(dict):
    return json.dumps(dict, separators=(',', ':'), ensure_ascii=False)

def load_dict(str_dict):
    return json.loads(str_dict, object_pairs_hook=OrderedDict)

def get_tuples_from_dict(item):
    for key, value in item.items():
        if type(value) == dict:
            yield from get_tuples_from_dict(value)               
        elif type(value) == list:
            for v in value:
                yield from get_tuples_from_dict(v)
        else:
            yield key, value

def print(instance, message):
    logging.info(f"{instance.__class__.__name__}: {message}")

def print_error(instance, message, fatal=False):
    message = 'Error at ' + instance.__class__.__name__ \
        + '. Message: ' + message
    if fatal:
        logging.critical(message)
        os._exit(1)
    logging.error(message)

def print_exception(instance, message, fatal=False):
    logging.exception('')
    print_error(instance, message, fatal)