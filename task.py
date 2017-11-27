#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

import struct
import socket
import time
import json
import collections

from google.protobuf.internal.encoder import _EncodeVarint
from google.protobuf.internal.decoder import _DecodeVarint32

from gen import ClientNamenodeProtocol_pb2
from gen import RpcHeader_pb2
from gen import IpcConnectionContext_pb2
from gen import ProtobufRpcEngine_pb2
from gen import yarn_service_protos_pb2
from gen import yarn_protos_pb2

logging.basicConfig(level=logging.INFO,format=u'%(asctime)s [%(levelname)s] (%(name)s) {%(pathname)s:%(lineno)d@%(funcName)s} -  %(message)s')

class HadoopRPC(object):
	
	def __init__(self,host,port):
		super(HadoopRPC,self).__init__()
		self._sock = socket.create_connection((host,port))
		self._rpc_request_header =None
		self._connect()
		return

	def _connect(self):
		self._sock.send('hrpc')
		self._sock.send(struct.pack('!B',9)) # version	
		self._sock.send(struct.pack('!B',0)) # service class
		self._sock.send(struct.pack('!B',0)) # simple auth

		self._rpc_request_header = self._init_rpc_request_header()
		context = IpcConnectionContext_pb2.IpcConnectionContextProto()
		
		self._write(self._sock,[self._rpc_request_header,context])

		return

	def _next_callid(self):
		if self._rpc_request_header.callId == -3:
			self._rpc_request_header.callId = 0
		self._rpc_request_header.callId += 1 	

	def _write(self,socket,payloads):
		out = ''.join(
			map(lambda x:self._wrap(x),payloads)
		)	
		socket.send(struct.pack('!I',len(out)))
		socket.send(out)
		
		return
	
	def _read_varint32(self,raw,start):
		return _DecodeVarint32(raw,start)
	
	def _varint(self,value):
		encoded = []
		_EncodeVarint(encoded.append,value)
		return ''.join(encoded)

	def _init_rpc_request_header(self):
		header = RpcHeader_pb2.RpcRequestHeaderProto()
		header.callId = -3
		header.rpcKind = RpcHeader_pb2.RPC_PROTOCOL_BUFFER
		header.rpcOp = RpcHeader_pb2.RpcRequestHeaderProto.RPC_FINAL_PACKET
		header.clientId = 'sonm-python-client'
		return header
	
	def _wrap(self,message):
		message = message.SerializeToString()
		message = '%s%s' % (
			self._varint(len(message)),
			message,
		)
		return message
	
	def call(self,clazz,method,request,response,callback=None):
		request_header = ProtobufRpcEngine_pb2.RequestHeaderProto()
		request_header.methodName = method
		request_header.declaringClassProtocolName = clazz
		request_header.clientProtocolVersion = 1

		self._next_callid()
		self._write(
			self._sock,
			[
				self._rpc_request_header,
				request_header,
				request,
			]
		)
		
		# wait for response
		length = struct.unpack('!I',self._sock.recv(4))[0]
		more = length
		raws = []
		while more > 0:
			raw = self._sock.recv(length)
			more -= len(raw)
			raws.append(raw)
		raw = ''.join(raws)
		
		# response header
		response_header = RpcHeader_pb2.RpcResponseHeaderProto()
		response_header_length,offest = self._read_varint32(raw,0)
		response_header.ParseFromString(raw[offest:offest+response_header_length])
			
		if response_header.status == RpcHeader_pb2.RpcResponseHeaderProto.SUCCESS:
			# response
			message_header_length,offest = self._read_varint32(raw,offest+response_header_length)
			response.ParseFromString(raw[offest:offest+message_header_length])
		
		if callback is not None:
			callback(response_header,response)
		else:
			return response_header,response

class MultiRPC(object):
	
	def __init__(self,rpcs):
		super(MultiRPC,self).__init__()
		self._rpcs = rpcs
		self._client = None
	
	def applications(self):
		request = yarn_service_protos_pb2.GetApplicationsRequestProto()
		request.application_states.extend([
			yarn_protos_pb2.ACCEPTED,		
			yarn_protos_pb2.RUNNING,		
		]) 	

		response = yarn_service_protos_pb2.GetApplicationsResponseProto()
		_,response = self.rpc(request,response)		
	
	def _find_client(self):
		if self._client is None:
			for rpc in self._rpcs:
				try:
					self._client = HadoopRPC(rpc[0],rpc[1])	
				except:
					pass
		if self._client is None:
			return self._find_client()
		else:
			return self._client	

	def rpc(self,protocol,method,request,response):
		while True:
			try:
				return self._find_client().call(
					protocol,
					method,
					request,
					response,
				)
			except:
				self._client = None

class Scheduler(object):
	
	def __init__(self,hosts):
		self._yarn = MultiRPC(hosts)
	
	def _is_ok(self,response_header):
		return response_header is not None and response_header.status == RpcHeader_pb2.RpcResponseHeaderProto.SUCCESS	
 
	def applications(self):
		request = yarn_service_protos_pb2.GetApplicationsRequestProto()
		request.application_states.extend([
			yarn_protos_pb2.RUNNING,		
		]) 
		#request.application_types.append('MAPREDUCE')
		request.application_types.append('SPARK')

		response = yarn_service_protos_pb2.GetApplicationsResponseProto()
		header,response = self._yarn.rpc(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'getApplications',
			request,
			response,
		)
		
		if self._is_ok(header):
			return map(
				lambda x:{
					'id': {
						'cluster_timestamp' : x.applicationId.cluster_timestamp,
						'id' : x.applicationId.id,
					},
					'name' : x.name,
					'queue' : x.queue,
					'resouces' : {
						'cpu' : x.app_resource_Usage.used_resources.virtual_cores,
						'memory' : x.app_resource_Usage.used_resources.memory,
					},
					'times' : {
						'cpu' : x.app_resource_Usage.vcore_seconds,
						'memory' : x.app_resource_Usage.memory_seconds,
					},
					'greedy' : {
						'cpu' : x.app_resource_Usage.vcore_seconds / x.app_resource_Usage.used_resources.virtual_cores,
						'memory' :  x.app_resource_Usage.memory_seconds / x.app_resource_Usage.used_resources.memory  
					}
				},
				response.applications,
			)
		else:
			return []
	
	def _queue_info(self,name):
		request = yarn_service_protos_pb2.GetQueueInfoRequestProto()
		request.queueName = name
		request.recursive = True
	
		response = yarn_service_protos_pb2.GetQueueInfoResponseProto()
		header,response = self._yarn.rpc(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'getQueueInfo',
			request,
			response,
		)
		
		if not self._is_ok(header):
			return None
				
		return {
			'name' : response.queueInfo.queueName ,
			'busy' : response.queueInfo.currentCapacity,
			'capacity' : response.queueInfo.capacity,
			'usage' : response.queueInfo.currentCapacity,
		}
	
	def queues(self):
		request = yarn_service_protos_pb2.GetQueueUserAclsInfoRequestProto()
		
		response = yarn_service_protos_pb2.GetQueueUserAclsInfoResponseProto()
		header,response = self._yarn.rpc(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'getQueueUserAcls',
			request,
			response,
		)
		
		if not self._is_ok(header):
			return []
		
		# collect leaf queues	
		queues = {}
		for name in map(lambda x:x.queueName,response.queueUserAcls):
			part = name.split('.')
			container = queues
			for part in name.split('.'):
				child = container.get(part,None)
				if child is None:
					container[part] = child = {}
				container = child
		# flatten back
		flatten = set()
		def drill(container,context=[]):
			for key,value in container.iteritems():
				context.append(key)
				if len(value) > 0:
					drill(value,context)
				else:
					# leaf
					flatten.add('.'.join(context))
				context = context[:-1]
		drill(queues)	
		
		# find queue info
		queue_infos = []
		for queue in flatten:
			info = self._queue_info(queue)
			if info is None:
				continue
			logging.info('fetch queue:%s' % info)
			queue_infos.append(info)
		return queue_infos
	
	def _move(self,task,queue):
		request = yarn_service_protos_pb2.MoveApplicationAcrossQueuesRequestProto()
		request.application_id.id = task['id']['id']
		request.application_id.cluster_timestamp = task['id']['cluster_timestamp']
		request.target_queue = queue['name']
	
		response = yarn_service_protos_pb2.MoveApplicationAcrossQueuesResponseProto()
		header,response = self._yarn.rpc(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'moveApplicationAcrossQueues',
			request,
			response,
		)
		
		if not self._is_ok(header):
			logging.warn('fail to move task:%s to queue:%s' % (task,queue))
			return False

		logging.info('move ok')		
		return True
	
	def _kill(self,task):
		request = yarn_service_protos_pb2.KillApplicationRequestProto()	
		request.application_id.id = task['id']['id']
		request.application_id.cluster_timestamp = task['id']['cluster_timestamp']
		
		response = yarn_service_protos_pb2.KillApplicationResponseProto()
		header,response = self._yarn.rpc(
			'org.apache.hadoop.yarn.api.ApplicationClientProtocolPB',
			'forceKillApplication',
			request,
			response,
		)
		if not self._is_ok(header):
			logging.warn('fail to kill task:%s' % task)
			return False
		return True

	def do(self):
		applications = self.applications()
		if len(applications) <= 0:
			return		
		
		for application in filter(lambda x:x['queue'] == 'root.spark',applications):
			if application['resouces']['cpu'] >= 500:
				logging.info('killing for used too much cpu, application:%s' % application)
				self._kill(application)
		
		applications = filter(lambda x:x['queue'] == 'root.default',applications)		
		
		logging.info(json.dumps(applications))
		queue = {'name':'root.spark'}
		for application in filter(lambda x:x['queue'] != queue['name'] ,applications):
			logging.info('moving application:%s to root.spark' % application)
			if not self._move(application,queue):
				self._kill(application)
		return

if __name__ == '__main__':
	scheduler = Scheduler([
		('10.116.100.10',23140),
		('10.116.100.11',23140),
	])
	
	while True:
		scheduler.do()
		logging.info('sleep...')	
		time.sleep(5)
