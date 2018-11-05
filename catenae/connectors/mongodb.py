#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import (
    MongoClient,
    ASCENDING,
    DESCENDING,
)


class MongodbConnector:

    def __init__(self, host, port, default_database=None,
                 default_collection=None, connect=False):
        self.config = {'host': host, 'port': port}
        if default_database:
            self.database = default_database
        if default_collection:
            self.collection = default_collection
        self.client = None
        if connect:
            self.open_connection()

    def _get_collection(self, database_name, collection_name):
        self.open_connection()
        database_name, collection_name = \
            self._set_database_collection_names(database_name,
                                                collection_name)
        database = getattr(self.client, database_name)
        collection = getattr(database, collection_name)
        return collection

    def open_connection(self):
        if not self.client:
            self.client = MongoClient(**self.config)

    def close_connection(self):
        if self.client:
            self.client.close()

    def _set_database_collection_names(self, database_name, collection_name):
        if not database_name:
            database_name = self.database
        if not collection_name:
            collection_name = self.collection
        return database_name, collection_name

    def get_and_close(self, query=None, database_name=None,
                      collection_name=None):
        result = self.get(query, database_name, collection_name)
        self.close_connection()
        return result

    def exists(self, query, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        if not collection.find_one(query):
            return False
        return True

    def create_index(self, attribute, database_name=None, collection_name=None,
                     unique=False, type_='asc'):
        if type_ == 'desc':
            type_ = DESCENDING
        else:
            type_ = ASCENDING
        collection = self._get_collection(database_name, collection_name)
        collection.create_index([(attribute, type_)], unique=unique, background=True)

    def get(self, query=None, database_name=None, collection_name=None, sort=None,
            limit=None):
        collection = self._get_collection(database_name, collection_name)
        result = collection.find(query, sort=sort)
        if limit:
            result = result.limit(limit)
        return result

    def get_random(self, query=None, database_name=None, collection_name=None,
                   sort=None, limit=1):
        collection = self._get_collection(database_name, collection_name)
        result = collection.aggregate([{'$sample': {'size': limit}}])
        return result

    def update_one(self, query, value, database_name=None, collection_name=None):
        self.update(query, value, database_name, collection_name)

    def update(self, query, value, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.update_one(query, {'$set': value}, upsert=True)

    def put(self, value, query=None, database_name=None, collection_name=None):
        if query:
            self.update(query, value, database_name, collection_name)
        else:
            self.insert(value, database_name, collection_name)

    def remove(self, query=None, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.remove(query)

    def insert_one(self, value, database_name=None, collection_name=None):
        self.insert(value, database_name, collection_name)

    def insert(self, value, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.insert_one(dict(value))

    def count(self, query=None, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        if not query:
            query = {}
        return collection.count_documents(filter=query)
