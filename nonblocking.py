#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

import struct

from collections import deque
from urlparse import urlparse

from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient
from tornado import gen
from tornado.concurrent import Future
from tornado.iostream import StreamClosedError

from google.protobuf.internal.encoder import _EncodeVarint
from google.protobuf.internal.decoder import _DecodeVarint32

from gen import ClientNamenodeProtocol_pb2
from gen import RpcHeader_pb2
from gen import IpcConnectionContext_pb2
from gen import ProtobufRpcEngine_pb2
from gen import yarn_service_protos_pb2
from gen import yarn_protos_pb2
from gen import mr_service_protos_pb2
from gen import ClientNamenodeProtocol_pb2

class HadoopRPC(object):
	
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
			response_header_length,offest = self._read_varint32(raw,0)
			response_header.ParseFromString(raw[offest:offest+response_header_length])
			
			call_context = self._callbacks.pop(response_header.callId)
	
			response = call_context[0] 
			future = call_context[1]
			if response_header.status == RpcHeader_pb2.RpcResponseHeaderProto.SUCCESS:
				# response
				message_header_length,offest = self._read_varint32(raw,offest+response_header_length)
				response.ParseFromString(raw[offest:offest+message_header_length])
				future.set_result((True,response))
			else:
				future.set_result((False,response_header))
		
			self._ioloop.add_callback(self._read)
		except StreamClosedError:
			pass

	def _read_varint32(self,raw,start):
		return _DecodeVarint32(raw,start)
	
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
		header.clientId = 'hrpc-client-next'
		return header
	
	def _varint(self,value):
		encoded = []
		_EncodeVarint(encoded.append,value)
		return ''.join(encoded)

	def _wrap(self,message):
		message = message.SerializeToString()
		message = '%s%s' % (
			self._varint(len(message)),
			message,
		)
		return message

	@gen.coroutine	
	def _write(self,payloads):
		out = ''.join(
			map(lambda x:self._wrap(x),payloads)
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
				logging.info('stream closed,retry...')
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

class Yarn(MultiRPC):
	def __init__(self,hosts):
		super(Yarn,self).__init__(hosts)

	@gen.coroutine	
	def running_applications(self):
		request = yarn_service_protos_pb2.GetApplicationsRequestProto()
		request.application_states.extend([
			yarn_protos_pb2.RUNNING,		
		]) 

		ok,response = yield self.call(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'getApplications',
			request,
			yarn_service_protos_pb2.GetApplicationsResponseProto(),
		)
		raise gen.Return(response if ok else [])
	
	@gen.coroutine	
	def move_to_queue(self,application_id,queue):
		request = yarn_service_protos_pb2.MoveApplicationAcrossQueuesRequestProto()
		request.application_id.id = application_id['id']
		request.application_id.cluster_timestamp = application_id['cluster_timestamp']
		request.target_queue = queue
	
		response = yarn_service_protos_pb2.MoveApplicationAcrossQueuesResponseProto()
		ok,response = yield self.call(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'moveApplicationAcrossQueues',
			request,
			response,
		)
		raise gen.Return(ok)

	@gen.coroutine
	def kill(self,application_id):
		request = yarn_service_protos_pb2.KillApplicationRequestProto()	
		request.application_id.id = application_id['id']
		request.application_id.cluster_timestamp = application_id['cluster_timestamp']
		
		response = yarn_service_protos_pb2.KillApplicationResponseProto()
		ok,response = yield self.call(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'forceKillApplication',
			request,
			response,
		)

	@gen.coroutine
	def application(self,application_id):	
		request = yarn_service_protos_pb2.GetApplicationReportRequestProto()	
		request.application_id.id = application_id['id']
		request.application_id.cluster_timestamp = application_id['cluster_timestamp']
		
		response = yarn_service_protos_pb2.GetApplicationReportResponseProto()
		ok,response = yield self.call(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'getApplicationReport',
			request,
			response,
		)
			
		raise gen.Return(response if ok else None)
	
	@gen.coroutine
	def application_attempt(self,application_id,attempt):	
		request = yarn_service_protos_pb2.GetApplicationAttemptReportRequestProto()	
		request.application_attempt_id.application_id.id = application_id['id']
		request.application_attempt_id.application_id.cluster_timestamp = application_id['cluster_timestamp']
		request.application_attempt_id.attemptId = attempt
		
		response = yarn_service_protos_pb2.GetApplicationAttemptReportResponseProto()
		ok,response = yield self.call(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'getApplicationAttemptReport',
			request,
			response,
		)
		
		raise gen.Return(response if ok else None)
		
class MRClient(MultiRPC):
	
	def __init__(self,hosts,history=False):
		super(MRClient,self).__init__(hosts)
		self._protocol = None
		if history:
			self._protocol = 'org.apache.hadoop.mapreduce.v2.api.HSClientProtocolPB'
		else:
			self._protocol = 'org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB'
	
	@gen.coroutine
	def jobreport(self,application_id,attempt):
		request = mr_service_protos_pb2.GetJobReportRequestProto()
		request.job_id.app_id.id = application_id['id'] 
		request.job_id.app_id.cluster_timestamp = application_id['cluster_timestamp'] 
		request.job_id.id = attempt
	
		response = mr_service_protos_pb2.GetJobReportResponseProto()	
		ok,response = yield self.call(
			self._protocol,
			'getJobReport',
			request,
			response,
		)
		
		raise gen.Return(response if ok else None)

class Namenode(MultiRPC):

	def __init__(self,hosts):
		super(Namenode,self).__init__(hosts)		
		self._protocol = 'org.apache.hadoop.hdfs.protocol.ClientProtocol'	

	def ha_switching(self,response):
		if super(Namenode,self).ha_switching(response):
			return True
		
		ok,response = response
		if not ok and response.exceptionClassName == 'org.apache.hadoop.ipc.StandbyException':
			logging.info('namenode ha switch...')
			return True
		return False
	
	@gen.coroutine	
	def file_info(self,path):
		request = ClientNamenodeProtocol_pb2.GetFileInfoRequestProto()
		request.src = path
		
		response = ClientNamenodeProtocol_pb2.GetFileInfoResponseProto()
		ok,response = yield self.call(
			self._protocol,
			'getFileInfo',
			request,
			response,
		)
		
		if ok and response.fs.fileId != 0:
			raise gen.Return(response)	
		raise gen.Return(None)
	
	@gen.coroutine
	def blocks(self,path,offset,length):
		request = ClientNamenodeProtocol_pb2.GetBlockLocationsRequestProto()
		request.src = path
		request.offset = offset
		request.length = length
	
		response = ClientNamenodeProtocol_pb2.GetBlockLocationsResponseProto()
		ok,response = yield self.call(
			self._protocol,
			'getBlockLocations',
			request,
			response,
		)
		
		raise gen.Return(response if ok else None)

	@gen.coroutine
	def list_dirs(self,path,after_child='',location=False):
		request = ClientNamenodeProtocol_pb2.GetListingRequestProto()
		request.src = path
		request.startAfter = after_child
		request.needLocation = location
	
		response = ClientNamenodeProtocol_pb2.GetListingResponseProto()
		ok,response = yield self.call(
			self._protocol,
			'getListing',
			request,
			response,
		)
		
		raise gen.Return(response if ok else None)	
	
	@gen.coroutine	
	def mkdirs(self,path,permission=0x777):
		request = ClientNamenodeProtocol_pb2.MkdirsRequestProto()
		request.src = path
		request.createParent = True
		request.masked.perm = permission

		response = ClientNamenodeProtocol_pb2.MkdirsResponseProto()
		ok,response = yield self.call(
			self._protocol,
			'mkdirs',
			request,
			response,
		)
		
		raise gen.Return(response if ok else None)
	
	@gen.coroutine
	def move(self,source,destination,force=False):
		request = ClientNamenodeProtocol_pb2.Rename2RequestProto()
		request.src = source
		request.dst = destination
		request.moveToTrash = True
		request.overwriteDest = force
	
		response = ClientNamenodeProtocol_pb2.Rename2ResponseProto()
		ok,response = yield self.call(
			self._protocol,
			'rename2',
			request,
			response,
		)
		if not ok:
			logging.warn(response)
		raise gen.Return(response if ok else None)	

class Namenodes(object):
	
	def __init__(self,namenodes):
		super(Namenodes,self).__init__()
		self._namenodes = namenodes	
	
	def resolve(self,path):
		path = urlparse(path)
		if path.scheme != 'hdfs':
			return None
		
		name_service = path.netloc.split(':')[0]	
		namenode = self._namenodes.get(name_service,None)
		if namenode is None:
			return None
		
		return namenode














