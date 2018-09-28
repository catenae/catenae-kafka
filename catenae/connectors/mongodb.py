#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient


class MongodbConnector:

    DEFAULT_DATABASE = 'test'
    DEFAULT_COLLECTION = 'test'

    def __init__(self, host, port, default_database=None,
                 default_collection=None, connect=False):
        self.config = {'host': host, 'port': port}
        if default_database:
            MongodbConnector.DEFAULT_DATABASE = default_database
        if default_collection:
            MongodbConnector.DEFAULT_COLLECTION = default_collection
        self.client = None
        if connect:
            self.open_connection()

    def open_connection(self):
        if not self.client:
            self.client = MongoClient(**self.config)

    def close_connection(self):
        if self.client:
            self.client.close()

    @staticmethod
    def _set_database_collection_names(database_name, collection_name):
        if not database_name:
            database_name = MongodbConnector.DEFAULT_DATABASE
        if not collection_name:
            collection_name = MongodbConnector.DEFAULT_COLLECTION
        return database_name, collection_name

    def _get_collection(self, database_name=None, collection_name=None):
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        database = getattr(self.client, database_name)
        collection = getattr(database, collection_name)
        return collection

    def get_and_close(self, query=None, database_name=None,
                      collection_name=None):
        self.open_connection()
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        result = self.get(query, database_name, collection_name)
        self.close_connection()
        return result

    def exists(self, query, database_name=None, collection_name=None):
        self.open_connection()
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        collection = self._get_collection(database_name, collection_name)
        if not collection.find_one(query):
            return False
        return True

    def create_index(self, attribute, database_name=None, collection_name=None,
                     unique=False):
        self.open_connection()
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        collection = self._get_collection(database_name, collection_name)
        collection.create_index(attribute, unique=unique, background=True)

    def get(self, query=None, database_name=None, collection_name=None, sort=None,
            limit=None):
        self.open_connection()
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        collection = self._get_collection(database_name, collection_name)
        result = collection.find(query, sort=sort)
        if limit:
            result = result.limit(limit)
        return result

    def update_one(self, query, value, database_name=None, collection_name=None):
        self.open_connection()
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        collection = self._get_collection(database_name, collection_name)
        collection.update_one(query, {'$set': value}, upsert=True)

    def put(self, value, query=None, database_name=None, collection_name=None):
        if query:
            self.update_one(query, value, database_name, collection_name)
        else:
            self.insert_one(value, database_name, collection_name)

    def insert_one(self, value, database_name=None, collection_name=None):
        self.open_connection()
        database_name, collection_name = \
            MongodbConnector._set_database_collection_names(database_name,
                                                            collection_name)
        collection = self._get_collection(database_name, collection_name)
        collection.insert_one(value)
