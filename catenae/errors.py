#!/usr/bin/env python
# -*- coding: utf-8 -*-

###################################################################################################


class RPCError(Exception):
    def __init__(self, message=None):
        super().__init__(message)


class Timeout(RPCError):
    def __init__(self, message=None):
        super().__init__(message)


class ParseError(RPCError):
    def __init__(self, message=None):
        super().__init__(message)


class InvalidRequestError(RPCError):
    def __init__(self, message=None):
        super().__init__(message)


class MethodNotFoundError(RPCError):
    def __init__(self, message=None):
        super().__init__(message)


class InvalidParamsError(RPCError):
    def __init__(self, message=None):
        super().__init__(message)


class InternalError(RPCError):
    def __init__(self, message=None):
        super().__init__(message)


###################################################################################################


class EmptyError(Exception):
    def __init__(self, message=None):
        super().__init__(message)


###################################################################################################
