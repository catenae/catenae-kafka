#!/usr/bin/env python
# -*- coding: utf-8 -*-

import aerospike


class AerospikeConnector:

    DEFAULT_NAMESPACE = 'test'
    DEFAULT_SET = 'test'

    def __init__(self, bootstrap_server, bootstrap_port, default_namespace=None,
                 default_set=None, connect=False):
        self.config = {'hosts': [(bootstrap_server, bootstrap_port)]}
        if default_namespace:
            AerospikeConnector.DEFAULT_NAMESPACE = default_namespace
        if default_set:
            AerospikeConnector.DEFAULT_SET = default_set
        self.client = None
        if connect:
            self.open_connection()

    def open_connection(self):
        if self.client == None:
            self.client = aerospike.client(self.config).connect()

    def close_connection(self):
        if self.client != None:
            self.client.close()

    @staticmethod
    def _set_namespace_set_names(namespace, set_):
        if not namespace:
            namespace = AerospikeConnector.DEFAULT_NAMESPACE
        if not set_:
            set_ = AerospikeConnector.DEFAULT_SET
        return namespace, set_

    def get_and_close(self, key, namespace=None, set_=None):
        self.open_connection()
        namespace, set_ = \
            AerospikeConnector._set_namespace_set_names(namespace, set_)
        try:
            bins = self.get(key, namespace, set_)
            self.close_connection()
            return bins
        except Exception:
            return

    def exists(self, key, namespace=None, set_=None):
        self.open_connection()
        namespace, set_ = \
            AerospikeConnector._set_namespace_set_names(namespace, set_)
        _, meta = self.client.exists((namespace, set_, key))
        if meta == None:
            return False
        else:
            return True

    def get(self, key, namespace=None, set_=None):
        self.open_connection()
        namespace, set_ = \
            AerospikeConnector._set_namespace_set_names(namespace, set_)
        try:
            as_key = (namespace, set_, key)
            (_, _, bins) = self.client.get(as_key)
            return bins
        except Exception:
            return

    def put(self, key, bins=None, namespace=None, set_=None, store_key=True):
        self.open_connection()
        namespace, set_ = \
            AerospikeConnector._set_namespace_set_names(namespace, set_)
        if not bins:
            bins = {'key': key}
        elif store_key:
            bins['key'] = key
        as_key = (namespace, set_, key)
        self.client.put(as_key, bins)

    def create_index(self, bin_, set_=None, type_='string', name=None,
                     namespace=None):
        self.open_connection()
        namespace, set_ = \
            AerospikeConnector._set_namespace_set_names(namespace, set_)
        if not name:
            name = bin_ + '_index'
        print(namespace)
        if type_ == 'string':
            self.client.index_string_create(namespace, set_, bin_, name)
        elif type_ == 'integer' or type_ == 'numeric':
            self.client.index_integer_create(namespace, set_, bin_, name)
