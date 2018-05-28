#!/usr/bin/env python
# -*- coding: utf-8 -*-


class LocalConnector(object):

    def __init__(self, path):
        self.path = path

    def get_object(object_name):
        object = None
        with open(path + '/' + object_name, 'rb') as file:
            return joblib.load(file)
