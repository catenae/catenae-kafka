#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from flask_cors import CORS
from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems
from .custom_queue import ProcessingQueue


class Server:
    class Endpoint(Resource):
        def __init__(self, queue):
            self.queue = queue

        def get(self):
            return {"result": "Hello JSON-RPC", "error": None, "id": 1}, 200

    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        server = Api(self.app)
        queue = ProcessingQueue()
        server.add_resource(Server.Endpoint, '/', resource_class_kwargs={'queue': queue})

    def run(self):
        options = {
            'bind': '%s:%s' % ('0.0.0.0', 9494),
            'workers': 1,
        }
        StandaloneApplication(self.app, options).run()


class StandaloneApplication(BaseApplication):
    def __init__(self, app, options=None):
        self.application = app
        self.options = options or {}
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

    def run(self):
        super().run()
