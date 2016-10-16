# -*- coding: utf-8 -*-

# Copyright (c) 2016 Grant AO. All rights reserved.
import time


def est_perf(func):
   def func_wrapper(*args):
       begin_time = time.time()
       result = func(*args)
       print func.__name__, 'execution time:', time.time() - begin_time
       return result
   return func_wrapper