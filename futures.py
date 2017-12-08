#!/usr/bin/python
# -*- coding: :new utf-8 -*-
import logging

from collections import deque

from tornado import gen

class Delays(object):
	
	def __init__(self):
		super(Delays,self).__init__()
		self._pending = deque()
		self._total = 0

	def add(self,future):
		self._pending.append(future)
		self._total += 1
	
	@gen.coroutine
	def join(self):
		results = []
		while self._total > 0:
			future = self._pending.popleft()
			if future.done():
				results.append((yield future))
				self._total -= 1
				continue

			# chance to switch
			yield gen.moment
	
		raise gen.Return(result)	



