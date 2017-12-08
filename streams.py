#!/usr/bin/python
# -*- coding: : utf-8 -*-

import logging

from tornado import gen

class ClosableStream(object):
	
	def __init__(self,closable_stream):
		super(ClosableStream,self).__init__()
		self._stream = closable_stream
	
	def close(self):
		if self._stream is not None:
			self._stream.close()
		self.__dict__.clear()
		logging.info(self._stream)
	
	def stream(self):
		return self._stream	

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
		if len(local) < hint:
			# reallocate
			local = '%s%s' %(
				''.join(local),
				''.join((yield stream.read_bytes(hint-len(local),partial=True))),
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

	
