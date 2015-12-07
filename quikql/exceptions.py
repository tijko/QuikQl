#!/usr/bin/env python
# -*- coding: utf-8 -*-


class InvalidArg(TypeError):
    '''
    Exception class that is raised to handle bad arguments passed.

    @type arg_type: <type 'type'>
    @param arg_type: The type that was passed and resulted in exeception being
                     raised.
    '''
    def __init__(self, arg_type):
        super(InvalidArg, self).__init__(arg_type)


class InvalidSQLType(ValueError):
    '''
    Exception class that is raised to handle bad sqlite types passed.

    @type values: <type 'str'>
    @param values: A string of the columns type names that were passed and
                   resulted in the exeception being raised.
    '''
    def __init__(self, values):
        super(InvalidSQLType, self).__init__(values)
