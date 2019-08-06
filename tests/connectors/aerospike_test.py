#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae.connectors.aerospike import AerospikeConnector
import time


class AerospikeTest:
    def __init__(self):
        self.aerospike = AerospikeConnector('aerospike', 3000, connect=True)
        self.aerospike.set_defaults('catenae', 'catenae')

        self.sample_key = 'sample_key'
        self.bins = {'bin1': 'value1', 'bin2': 'value2'}

    def open_close_connection(self):
        self.aerospike.open_connection()
        self.aerospike.close_connection()

    def remove_record_if_exists(self):
        if self.aerospike.exists(self.sample_key):
            self.aerospike.remove(self.sample_key)

    def check_non_existing_key(self):
        self.remove_record_if_exists()
        assert (not self.aerospike.exists(self.sample_key))
        key, value = self.aerospike.get(self.sample_key)
        assert (key == None and value == None)

    def check_existing_key_without_bins(self):
        self.remove_record_if_exists()
        self.aerospike.put(self.sample_key)
        assert (self.aerospike.exists(self.sample_key))
        key, value = self.aerospike.get(self.sample_key)
        assert (key == None and value == 0)

    def check_existing_key_with_key_bin(self):
        self.remove_record_if_exists()
        self.aerospike.put(self.sample_key, store_key=True)
        assert (self.aerospike.exists(self.sample_key))
        key, value = self.aerospike.get(self.sample_key)
        assert (key == self.sample_key and value == None)

    def check_existing_key_with_key_value_bins(self):
        self.remove_record_if_exists()
        self.aerospike.put(self.sample_key, bins=self.bins)
        assert (self.aerospike.exists(self.sample_key))
        key, value = self.aerospike.get(self.sample_key)
        assert (key == None and value == self.bins)

    def create_index(self):
        self.remove_record_if_exists()
        self.aerospike.put(self.sample_key, bins=self.bins)
        self.aerospike.create_index('bin1', type_='string', name='bin1_idx')
        self.aerospike.get_and_close('key1')

    def start(self):
        self.open_close_connection()
        self.create_index()

        self.check_existing_key_without_bins()
        self.check_non_existing_key()
        self.check_existing_key_with_key_bin()
        self.check_existing_key_with_key_value_bins()

        print('PASSED')


if __name__ == "__main__":
    AerospikeTest().start()