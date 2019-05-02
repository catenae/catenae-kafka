#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import logging
import time


class MongodbConnector:
    def __init__(self, host, port, default_database=None, default_collection=None, connect=False):
        self._config = {'host': host, 'port': port}
        if default_database != None:
            self._default_database = default_database
        if default_collection != None:
            self._default_collection = default_collection
        self._client = None
        if connect:
            self.open_connection()

    @property
    def client(self):
        return self._client

    def _get_collection(self, database_name, collection_name):
        self.open_connection()
        database_name, collection_name = \
            self._get_database_and_collection_names(database_name,
                                                collection_name)
        database = getattr(self._client, database_name)
        collection = getattr(database, collection_name)
        return collection

    def set_defaults(self, database_name, database_collection=None):
        self._default_database = database_name
        if database_collection != None:
            self._default_collection = database_collection

    def open_connection(self, attempts=10):
        if not self._client:
            try:
                self._client = MongoClient(**self._config)
                self._client.server_info()
            except Exception:
                if attempts == 0:
                    logging.exception('')
                else:
                    time.sleep(1)
                    self.open_connection(attempts - 1)

    def close_connection(self):
        if self._client:
            self._client.close()

    def _get_database_and_collection_names(self, database_name, collection_name):
        if not database_name and hasattr(self, '_default_database'):
            database_name = self._default_database
        if not collection_name and hasattr(self, '_default_collection'):
            collection_name = self._default_collection
        return database_name, collection_name

    def get_and_close(self, query=None, database_name=None, collection_name=None):
        result = self.get(query, database_name, collection_name)
        self.close_connection()
        return result

    def exists(self, query, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        if not collection.find_one(query):
            return False
        return True

    def create_index(self,
                     attribute=None,
                     keys=None,
                     database_name=None,
                     collection_name=None,
                     unique=False,
                     type_='asc',
                     background=True):
        if not keys and not attribute:
            raise TypeError
        if not keys:
            if type_ == 'desc':
                type_ = -1
            else:
                type_ = 1
            keys = [(attribute, type_)]
        collection = self._get_collection(database_name, collection_name)
        collection.create_index(keys, unique=unique, background=background)

    def get(self,
            query=None,
            database_name=None,
            collection_name=None,
            sort=None,
            sort_attribute=None,
            sort_type=None,
            limit=None,
            index=None,
            index_attribute=None,
            index_type=None):
        collection = self._get_collection(database_name, collection_name)
        if sort_attribute and sort_type:
            if sort_type == 'desc':
                sort_type = -1
            else:
                sort_type = 1
            sort = [(sort_attribute, sort_type)]
        if limit == 1:
            result = collection.find_one(query, sort=sort)
            if result:
                return iter([result])
            return iter([])
        result = collection.find(query, sort=sort)
        if index or (index_attribute and index_type):
            if index:
                result = result.hint(index)
            else:
                if index_type == 'desc':
                    index_type = -1
                else:
                    index_type = 1
                result = result.hint([(index_attribute, index_type)])
        if limit:
            result = result.limit(limit)
        return result

    def get_random(self,
                   query=None,
                   database_name=None,
                   collection_name=None,
                   sort=None,
                   sort_attribute=None,
                   sort_type=None,
                   limit=1):
        collection = self._get_collection(database_name, collection_name)
        if sort_attribute and sort_type:
            if sort_type == 'desc':
                sort_type = -1
            else:
                sort_type = 1
            sort = [(sort_attribute, sort_type)]
        operation = [{'$sample': {'size': limit}}]
        if query:
            operation = [{'$match': query}] + operation
        if sort:
            operation = operation + [{'$sort': sort}]
        result = collection.aggregate(operation)
        return result

    def update(self, query, value, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.update_one(query, {'$set': value}, upsert=True)

    def push(self, query, key, value, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.update_one(query, {'$push': {key: {'$each': value}}}, upsert=True)

    def put(self, value, query=None, database_name=None, collection_name=None):
        if query:
            self.update(query, value, database_name, collection_name)
        else:
            self.insert(value, database_name, collection_name)

    def remove(self, query=None, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.remove(query)

    def insert(self, value, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        collection.insert_one(dict(value))

    def count(self, query=None, database_name=None, collection_name=None):
        collection = self._get_collection(database_name, collection_name)
        if not query:
            query = {}
        return collection.count_documents(filter=query)
