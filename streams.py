#!/usr/bin/python
# -*- coding: : utf-8 -*-

import logging

from collections import deque
from tornado import gen

class ClosableStream(object):
	
	def __init__(self,closable_stream):
		super(ClosableStream,self).__init__()
		self._stream = closable_stream
	
	def close(self):
		if self._stream is not None:
			self._stream.close()
		self.__dict__.clear()
	
	def stream(self):
		return self._stream	

	def switch(self,stream):
		self._stream = stream	
	
class BufferedStream(ClosableStream):
	
	def __init__(self,tornado_stream):
		super(BufferedStream,self).__init__(tornado_stream)
		self._buffer = ''
	
	@gen.coroutine
	def poll(self,hint):
		# localized
		local = self._buffer
		stream = self.stream()
	
		# do read
		while len(local) < hint:
			# reallocate
			local = '%s%s' %(
				''.join(local),
				''.join((yield stream.read_bytes(hint-len(local),partial=False))),
			)

		# back
		self._buffer = local	
	
	@gen.coroutine
	def peek(self,hint):
		yield self.poll(hint)	
		raise gen.Return(self._buffer[:hint])
	
	@gen.coroutine
	def read(self,hint):	
		yield self.poll(hint)
		content = self._buffer[:hint]
		# slice
		self._buffer = self._buffer[hint:]
		raise gen.Return(content)	

	@gen.coroutine
	def write(self,content):
		yield self.stream().write(content)
	
	def readable(self):
		return len(self._buffer)

class SwitchableStream(ClosableStream):
	
	def __init__(self,candidates):
		super(SwitchableStream,self).__init__(None)
		self._candidates = deque(candidates)
		self._readed = 0		
	
	@gen.coroutine
	def _unsafe_read(self,hint):
		current = self.stream()
		if current is None:
			try:
				# pop and initialize
				current = self._candidates.pop()()

				# ugly and tricky
				if isinstance(current,gen.Future):
					current = yield current
			except IndexError:
				# no more
				raise gen.Return(None)
			
			# got new stream
			# now recover offest
			# use seek if avaliable
			if hasattr(current,'seek') and callable(current.seek):
				yield current.seek(self._readed)
			else:	 
				yield current.read(self._readed)
			
			# switch
			self.switch(current)
		
		# a readbale stream
		readed = yield current.read(hint)	

		# update state
		if readed is not None:
			self._readed += len(readed)
		
		raise gen.Return(readed)
	
	@gen.coroutine
	def read(self,hint):
		try:
			raise gen.Return((yield self._unsafe_read(hint)))
		except gen.Return as result:
			raise result
		except:
			# close it if open
			current = self.stream()
			logging.exception('fail of stream:%s,try next one...' % current)
			if current is not None:
				current.close()

			self.switch(None)

			# now next
			raise gen.Return((yield self.read(hint)))



