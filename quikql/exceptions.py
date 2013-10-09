#!/usr/bin/env python
# -*- coding: utf-8 -*-


class InsufficientArgs(Exception):

    def __init__(self, func, manarg, argnum):
        self.func = func
        self.manarg = manarg
        self.argnum = argnum 

    def __str__(self):
        msg = ("%s expected at least %d arguments, got %d" % 
               (self.func, self.manarg, self.argnum))
        return msg

 
class InvalidArg(Exception):

    def __init__(self, func, argname, argtype):
        self.func = func
        self.argname = argname
        self.argtype = argtype

    def __str__(self):
        msg = "%s: %s must be a %s" % (self.func, argname, argtype)
        return msg
