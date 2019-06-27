#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask, request, Response
from flask_restful import reqparse, abort, Api, Resource
from flask_cors import CORS
from gunicorn.app.base import BaseApplication
from gunicorn.six import iteritems
import sys
import logging
from .logger import Logger
from os import environ
import json


class JsonRPC:
    def __init__(self, pipe_connection, logger):
        self.logger = logger
        self.app = Flask(__name__)
        CORS(self.app)
        api = Api(self.app)
        api.add_resource(JsonRPC.Endpoint,
                         '/',
                         resource_class_kwargs={
                             'pipe_connection': pipe_connection,
                             'logger': logger
                         })

    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    ERROR_CODES = {
        -32700: 'Parse error',
        -32600: 'Invalid Request',
        -32601: 'Method not found',
        -32602: 'Invalid params',
        -32603: 'Internal error',
    }

    HTTP_CODE = {
        None: 200,
        PARSE_ERROR: 400,
        INVALID_REQUEST: 400,
        METHOD_NOT_FOUND: 404,
        INVALID_PARAMS: 400,
        INTERNAL_ERROR: 500
    }

    class ParseError(Exception):
        def __init__(self, message=None):
            super(JsonRPC.ParseError, self).__init__(message)

    class InvalidRequestError(Exception):
        def __init__(self, message=None):
            super(JsonRPC.InvalidRequestError, self).__init__(message)

    class MethodNotFoundError(Exception):
        def __init__(self, message=None):
            super(JsonRPC.MethodNotFoundError, self).__init__(message)

    class InvalidParamsError(Exception):
        def __init__(self, message=None):
            super(JsonRPC.InvalidParamsError, self).__init__(message)

    class InternalError(Exception):
        def __init__(self, message=None):
            super(JsonRPC.InternalError, self).__init__(message)

    @staticmethod
    def get_response(id, result=None, error_code=None):
        response = {'jsonrpc': '2.0'}

        if error_code is None:
            response.update({'result': result})
        else:
            if error_code in JsonRPC.ERROR_CODES:
                message = JsonRPC.ERROR_CODES[error_code]
            elif error_code >= -32099 and error_code <= -32000:
                message = 'Server error'
            else:
                raise ValueError
            response.update({'error': {'code': error_code, 'message': message}})

        response.update({'id': id})
        return response

    class Endpoint(Resource):
        def __init__(self, pipe_connection, logger):
            self.pipe_connection = pipe_connection
            self.logger = logger

        def check_valid_jsonrpc_request(self, rpc_request):
            if 'jsonrpc' not in rpc_request:
                raise AttributeError
            if rpc_request['jsonrpc'] != '2.0':
                raise JsonRPC.InvalidRequestError

            if 'method' not in rpc_request:
                raise JsonRPC.InvalidRequestError
            if type(rpc_request['method']) is not str:
                raise JsonRPC.InvalidRequestError

            if 'params' in rpc_request:
                if type(rpc_request['params']) is not dict:
                    raise JsonRPC.InvalidParamsError

        def post(self):
            try:
                rpc_request = request.get_json(force=True)
            except Exception:
                response = JsonRPC.get_response(None, error_code=JsonRPC.INVALID_REQUEST)
                return response, 400

            if 'id' in rpc_request:
                request_id = rpc_request['id']
                is_notification = False
            else:
                request_id = None
                is_notification = True

            try:
                self.check_valid_jsonrpc_request(rpc_request)
            except JsonRPC.InvalidRequestError:
                response = JsonRPC.get_response(request_id, error_code=JsonRPC.INVALID_REQUEST)
                return response, 400
            except JsonRPC.InvalidParamsError:
                response = JsonRPC.get_response(request_id, error_code=JsonRPC.INVALID_PARAMS)
                return response, 400

            if 'params' in rpc_request:
                queue_request = (rpc_request['method'], rpc_request['params'])
            else:
                queue_request = (rpc_request['method'], None)

            if is_notification:
                self.pipe_connection.send((True, queue_request))
                return Response(status=200)
            else:
                self.pipe_connection.send((False, queue_request))

            error_code, result = self.pipe_connection.recv()
            response = JsonRPC.get_response(request_id, result=result, error_code=error_code)

            try:
                json.dumps(response)
            except TypeError:
                self.logger.log(level='exception')
                response = JsonRPC.get_response(request_id, error_code=JsonRPC.INTERNAL_ERROR)

            http_code = JsonRPC.HTTP_CODE[error_code]
            return response, http_code

    class StreamToLogger:
        def __init__(self, logger, level='info'):
            self.logger = logger
            self.level = level

        def write(self, text):
            text = text.strip()
            if text:
                self.logger.log(text, level=self.level)

    def run(self):
        sys.stderr = JsonRPC.StreamToLogger(self.logger)
        # In order to support multiple workers, the interprocess communication
        # has to be reimplemented with queues instead of pipes
        options = {'bind': f"0.0.0.0:{environ['JSONRPC_PORT']}", 'workers': 1, 'timeout': 60}
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
