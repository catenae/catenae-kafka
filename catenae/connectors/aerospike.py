#!/usr/bin/env python
# -*- coding: utf-8 -*-

import aerospike

class AerospikeConnector(object):

    def __init__(self, bootstrap_server, bootstrap_port):
        self.config = {'hosts': [(bootstrap_server, bootstrap_port)]}
        self.client = None

    def open_connection(self):
        if self.client is None:
            try:
                self.client = aerospike.client(self.config).connect()
            except Exception as ex:
                import sys
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)

    def close_connection(self):
        if self.client is not None:
            self.client.close()

    def get_and_close(self, key, as_namespace, as_set):
        """The connection is automatically closed after the call"""
        try:
            value = self.get(key, as_namespace, as_set)
            self.close_connection()
            return value
        except:
            return None

    def exists(self, key, as_namespace, as_set):
        """Check a record. The connection is created (if it is not already) and
        it keeps opened until it is manually closed.
        """
        self.open_connection()
        _, meta = self.client.exists((as_namespace, as_set, key))
        if meta is None:
            return False
        else:
            return True

    def get(self, key, as_namespace, as_set):
        """Get a record. The connection is created (if it is not already) and
        it keeps opened until it is manually closed.
        """
        self.open_connection()
        try:
            (as_key, metadata, record) = \
                self.client.get((as_namespace, as_set, key))
            return record['value']
        except:
            return None

    def put(self, key, value, as_namespace, as_set):
        """Put a record. The connection is created (if it is not already) and
        it keeps opened until it is manually closed.
        """
        self.open_connection()
        as_key = (as_namespace, as_set, key)
        self.client.put(as_key, {
            'key': key,
            'value': value
        })
