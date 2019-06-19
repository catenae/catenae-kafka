#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from flask_cors import CORS
from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems
from .custom_queue import ProcessingQueue
import sys
import logging


class JsonRPC:
    def __init__(self, queue):
        self.app = Flask(__name__)
        CORS(self.app)
        api = Api(self.app)
        api.add_resource(JsonRPC.Endpoint, '/', resource_class_kwargs={'queue': queue})

    class Endpoint(Resource):
        def __init__(self, queue):
            self.queue = queue

        def get(self):
            self.queue.put("test")
            return {"result": "Hello JSON-RPC", "error": None, "id": 1}, 200

    class StreamHider:
        def write(self, _):
            pass

        def flush(self):
            pass

    def run(self):
        sys.stdout = JsonRPC.StreamHider()
        sys.stderr = JsonRPC.StreamHider()
        options = {'bind': '0.0.0.0:9494', 'workers': 1, 'timeout': 60, 'log-level': 'error'}
        Server(self.app, options).run()


class Server(BaseApplication):
    def __init__(self, app, options=None):
        self.app = app
        self.options = options or {}
        super(Server, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.app

    def run(self):
        super().run()
