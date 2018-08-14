#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient


class MongodbConnector:

    def __init__(self, host, port):
        self.config = {'host': host, 'port': port}
        self.client = None

    def open_connection(self):
        if not self.client:
            self.client = MongoClient(**self.config)

    def close_connection(self):
        if self.client:
            self.client.close()

    def _get_collection(self, database_name, collection_name):
        database = getattr(self.client, database_name)
        collection = getattr(database, collection_name)
        return collection

    def get_and_close(self, query, database_name, collection_name):
        self.open_connection()
        result = self.get(query, database_name, collection_name)
        self.close_connection()
        return result

    def exists(self, query, database_name, collection_name):
        self.open_connection()
        collection = self._get_collection(database_name, collection_name)
        if not collection.find_one(query):
            return False
        return True

    def create_index(self, attribute, database_name, collection_name, unique=True):
        collection = self._get_collection(database_name, collection_name)
        collection.create_index(attribute, unique=unique, background=True)

    def get(self, query, database_name, collection_name):
        self.open_connection()
        collection = self._get_collection(database_name, collection_name)
        result = collection.find(query)
        return result

    def update_one(self, query, value, database_name, collection_name):
        self.open_connection()
        collection = self._get_collection(database_name, collection_name)
        collection.update_one(query, {'$set': value}, upsert=True)

    def insert_one(self, value, database_name, collection_name):
        self.open_connection()
        collection = self._get_collection(database_name, collection_name)
        collection.insert_one(value)
