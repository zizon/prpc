#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

import struct

from collections import deque

from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.concurrent import Future
from tornado.iostream import StreamClosedError

from google.protobuf.internal.encoder import _EncodeVarint
from google.protobuf.internal.decoder import _DecodeVarint32

from gen import RpcHeader_pb2
from gen import IpcConnectionContext_pb2
from gen import ProtobufRpcEngine_pb2

class ProtocolBuffer(object):
	def __init__(self):
		super(ProtocolBuffer,self).__init__()
	
	def _varint(self,value):
		encoded = []
		_EncodeVarint(encoded.append,value)
		return ''.join(encoded)
	
	def read_varint32(self,raw,start):
		return _DecodeVarint32(raw,start)
	

	def wrap(self,message):
		message = message.SerializeToString()
		message = '%s%s' % (
			self._varint(len(message)),
			message,
		)
		return message

class HadoopRPC(ProtocolBuffer):
	CLIENT_ID = 'hrpc-client-next'
	
	def __init__(self,host,port):
		super(HadoopRPC,self).__init__()
		self._connect_info = (host,port)
		self._ioloop = None
		self._stream = None
		self._rpc_request_header = None
		self._callbacks = None

	def closed(self):
		return self._stream is None or self._stream.closed()	

	@gen.coroutine
	def start(self,ioloop=None):
		# close all
		yield self.close()
		
		# update io loop
		self._ioloop = ioloop if ioloop is not None else IOLoop.current()
		
		# create new stream
		self._stream = yield TCPClient(io_loop=self._ioloop).connect(*self._connect_info)
		
		# make stream context
		self._callabcks = {}
		
		# connect
		yield self._connect()
		
		# kick reading
		self._ioloop.add_callback(self._read)
	
	@gen.coroutine
	def close(self):
		if self._stream is not None:
			self._stream.close()
		self._stream = None
		self._callbacks = None
	
	@gen.coroutine
	def call(self,clazz,method,request,response):
		if self._stream is None:
			raise StreamClosedError()

		request_header = ProtobufRpcEngine_pb2.RequestHeaderProto()
		request_header.methodName = method
		request_header.declaringClassProtocolName = clazz
		request_header.clientProtocolVersion = 1
		
		# make call context
		call_id = self._next_call(response)
			
		# send request	
		yield self._write(
			[
				self._rpc_request_header,
				request_header,
				request,
			]
		)

		# check future	
		future = self._callbacks[call_id][1]
		yield future		
		
		raise gen.Return(future.result())		

	@gen.coroutine
	def _read(self):
		try:
			if self._stream is None:
				raise gen.Return()			

			# wait for response
			raw = yield self._stream.read_bytes(4)
			length = struct.unpack('!I',raw)[0]
			raw = yield self._stream.read_bytes(length)

			# response header
			response_header = RpcHeader_pb2.RpcResponseHeaderProto()
			response_header_length,offest = self.read_varint32(raw,0)
			response_header.ParseFromString(raw[offest:offest+response_header_length])
			
			call_context = self._callbacks.pop(response_header.callId)
	
			response = call_context[0] 
			future = call_context[1]
			if response_header.status == RpcHeader_pb2.RpcResponseHeaderProto.SUCCESS:
				# response
				message_header_length,offest = self.read_varint32(raw,offest+response_header_length)
				response.ParseFromString(raw[offest:offest+message_header_length])
				future.set_result((True,response))
			else:
				future.set_result((False,response_header))
		
			self._ioloop.add_callback(self._read)
		except StreamClosedError:
			pass

	@gen.coroutine		
	def _connect(self):
		self._callbacks = {}

		# hello
		yield self._stream.write(''.join([
			'hrpc',
			struct.pack('!B',9), # version
			struct.pack('!B',0), # service class
			struct.pack('!B',0), # simple auth
		]))	
		
		# handshake	
		self._rpc_request_header = self._init_rpc_request_header()
		context = IpcConnectionContext_pb2.IpcConnectionContextProto()
			
		yield self._write(
			[
				self._rpc_request_header,
				IpcConnectionContext_pb2.IpcConnectionContextProto(),
			]
		)

	def _init_rpc_request_header(self):
		header = RpcHeader_pb2.RpcRequestHeaderProto()
		header.callId = -3
		header.rpcKind = RpcHeader_pb2.RPC_PROTOCOL_BUFFER
		header.rpcOp = RpcHeader_pb2.RpcRequestHeaderProto.RPC_FINAL_PACKET
		# clientid should be 16 bytes long ...
		# see org.apache.hadoop.ipc.RetryCache
		# at least namenode rpc enforce this.
		# or simply set it to empty
		header.clientId = HadoopRPC.CLIENT_ID
		return header
	
	@gen.coroutine	
	def _write(self,payloads):
		out = ''.join(
			map(lambda x:self.wrap(x),payloads)
		)	
		yield self._stream.write(
			''.join([
				struct.pack('!I',len(out)),
				out,
		]))
	
	def _next_call(self,response):
		if self._rpc_request_header.callId == -3:
			self._rpc_request_header.callId = 0
		self._rpc_request_header.callId += 1 	
		self._callbacks[self._rpc_request_header.callId] = (
			response,
			Future(),	
			IOLoop.current().time(),
		)
		return self._rpc_request_header.callId

class MultiRPC(object):
	
	def __init__(self,hosts):
		self._clients = deque(hosts)
		self._active = None

	@gen.coroutine
	def _active_client(self):
		active = self._active
		if active is not None and not active.closed():
			raise gen.Return(active)
		
		# find one
		host = self._clients.popleft()
		self._clients.append(host)
	 	active = HadoopRPC(*host)
			
		yield active.start()
		logging.info('using host:%s ...' % str(host))
		raise gen.Return(active)

	@gen.coroutine
	def call(self,*args,**kwargs):
		response = None
		active  = None

		while True:
			switching = False
			try:
				active = yield self._active_client()
				response = yield active.call(*args,**kwargs)
				switching = self.ha_switching(response)
				if not switching:	
					break
			except StreamClosedError:
				logging.warn('stream closed,retry...')
				continue
			except:
				yield active.close()
				logging.exception('why')
			
			# either ha switch or exception,
			# try next
			yield gen.sleep(1)
			continue

		# update connection
		if self._active is None:
			self._active = active
		elif self._active != active:
			yield active.close()
	
		raise gen.Return(response)			
	
	@gen.coroutine
	def close(self):
		active = self._active
		if active is not None:
			yield active.close()
		self._active = None

	def ha_switching(self,response):
		return False


