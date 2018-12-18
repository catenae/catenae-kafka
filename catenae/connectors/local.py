#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle


class LocalConnector:

    def __init__(self, path):
        self.path = path

    def get_object(self, object_name):
        with open(self.path + '/' + object_name, 'rb') as file:
            return pickle.load(file)
