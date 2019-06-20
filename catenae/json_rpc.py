#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask, request, Response
from flask_restful import reqparse, abort, Api, Resource
from flask_cors import CORS
from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems
from .custom_queue import ProcessingQueue
import sys
import logging
from .logger import Logger


class JsonRPC:
    def __init__(self, request_queue, response_queue):
        self.app = Flask(__name__)
        CORS(self.app)
        api = Api(self.app)
        api.add_resource(JsonRPC.Endpoint,
                         '/',
                         resource_class_kwargs={
                             'request_queue': request_queue,
                             'response_queue': response_queue
                         })

    class Endpoint(Resource):
        def __init__(self, request_queue, response_queue):
            self.request_queue = request_queue
            self.response_queue = response_queue

        def check_valid_call(self, rpc_request):
            if 'jsonrpc' not in rpc_request:
                raise AttributeError
            if rpc_request['jsonrpc'] != '2.0':
                raise ValueError

            # if 'method' not in rpc_request:
            #     raise AttributeError
            # if type(rpc_request['method']) != str:
            #     raise ValueError

            # if 'params' in rpc_request:
            #     if type(rpc_request['params']) != dict:
            #         raise ValueError
            #     for key, value in rpc_request['params']:
            #         if type(key) != str:
            #             raise ValueError

            # if 'id' not in rpc_request:
            #     raise AttributeError

        def post(self):
            rpc_request = request.get_json(force=True)

            try:
                self.check_valid_call(rpc_request)
            except (AttributeError, ValueError):
                return Response(status=400)

            if 'params' in rpc_request:
                queue_request = (rpc_request['method'], rpc_request['params'])
            else:
                queue_request = (rpc_request['method'], None)

            self.request_queue.put(queue_request)
            result = self.response_queue.get()

            response = {'jsonrpc': '2.0', 'result': result, 'id': rpc_request['id']}
            return response, 200

    class StreamHider:
        def write(self, _):
            pass

        def flush(self):
            pass

    def run(self):
        sys.stdout = JsonRPC.StreamHider()
        sys.stderr = JsonRPC.StreamHider()

        options = {'bind': '0.0.0.0:9494', 'workers': 1, 'timeout': 60}
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
