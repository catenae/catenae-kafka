#!/usr/bin/env python
# -*- coding: utf-8 -*-

import aerospike
from aerospike import exception as aerospike_exceptions


class AerospikeConnector:

    def __init__(self, bootstrap_server, bootstrap_port, default_namespace=None,
                 default_set=None, connect=False):
        self.config = {'hosts': [(bootstrap_server, bootstrap_port)],
                       'policies': {'timeout': 5000}}
        if default_namespace:
            self.default_namespace = default_namespace
        else:
            self.default_namespace = 'test'
        if default_set:
            self.default_set = default_set
        else:
            self.default_set = 'test'
        self.client = None
        if connect:
            self.open_connection()

    def set_defaults(self, namespace, set_):
        self.default_namespace = namespace
        self.default_set = set_

    def open_connection(self):
        if self.client == None:
            self.client = aerospike.client(self.config).connect()

    def close_connection(self):
        if self.client != None:
            self.client.close()
            self.client = None

    def _connect_get_askey(self, key, namespace, set_):
        self.open_connection()
        namespace, set_ = \
            self._set_namespace_set_names(namespace, set_)
        as_key = (namespace, set_, key)
        return as_key

    def _set_namespace_set_names(self, namespace, set_):
        if not namespace:
            namespace = self.default_namespace
        if not set_:
            set_ = self.default_set
        return namespace, set_

    def get_and_close(self, key, namespace=None, set_=None):
        self.open_connection()
        namespace, set_ = \
            self._set_namespace_set_names(namespace, set_)
        try:
            bins = self.get(key, namespace, set_)
            self.close_connection()
            return bins
        except Exception:
            return

    def exists(self, key, namespace=None, set_=None):
        self.open_connection()
        namespace, set_ = \
            self._set_namespace_set_names(namespace, set_)
        _, meta = self.client.exists((namespace, set_, key))
        if meta == None:
            return False
        else:
            return True

    def get(self, key, namespace=None, set_=None):
        as_key = self._connect_get_askey(key, namespace, set_)
        try:
            (_, _, bins) = self.client.get(as_key)
        except aerospike_exceptions.RecordNotFound as e:
            return None, None
        # Return a single value if there is only one bin
        if len(bins) == 1:
            if 'value' in bins:
                return None, bins['value']
            elif 'key' in bins:
                return bins['key'], None
        # Return a tuple if the record is of type key -> (key, value)
        elif len(bins) == 2:
            if 'key' in bins and 'value' in bins:
                return bins['key'], bins['value']
            elif 'key' in bins and key in bins:
                return bins['key'], bins[key]
        return None, bins

    def put(self, key, bins=None, namespace=None, set_=None, store_key=False):
        if store_key:
            if bins == None:
                bins = {'key': key}
            elif type(bins) == dict:
                bins['key'] = key
            else:
                bins = {'key': key, 'value': bins}
        else:
            # Reduce bin size
            if bins == None:
                bins = 0
            if type(bins) != dict:
                bins = {'value': bins}
        as_key = self._connect_get_askey(key, namespace, set_)
        self.client.put(as_key, bins)

    def remove(self, key, namespace=None, set_=None):
        as_key = self._connect_get_askey(key, namespace, set_)
        self.client.remove(as_key)

    def create_index(self, bin_, set_=None, type_='string', name=None,
                     namespace=None):
        self.open_connection()
        namespace, set_ = \
            self._set_namespace_set_names(namespace, set_)
        if not name:
            name = bin_ + '_index'
        if type_ == 'string':
            self.client.index_string_create(namespace, set_, bin_, name)
        elif type_ == 'integer' or type_ == 'numeric':
            self.client.index_integer_create(namespace, set_, bin_, name)
