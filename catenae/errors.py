#!/usr/bin/env python
# -*- coding: utf-8 -*-


class EmptyError(Exception):
    pass


class TimeoutError(Exception):
    pass


class RPCError(Exception):
    pass


class ParseError(RPCError):
    pass


class InvalidRequestError(RPCError):
    pass


class MethodNotFoundError(RPCError):
    pass


class InvalidParamsError(RPCError):
    pass


class InternalError(RPCError):
    pass
