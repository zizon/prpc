#!/usr/bin/python
# -*- coding: : utf-8 -*-

import logging

import struct

from collections import deque
from urlparse import urlparse

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient

from nonblocking import HadoopRPC
from nonblocking import MultiRPC
from nonblocking import ProtocolBuffer

from streams import BufferedStream
from streams import ClosableStream

from gen import ClientNamenodeProtocol_pb2
from gen import datatransfer_pb2

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
	def list_dirs_all(self,path,each,location=False):
		after_child = ''
		while True:
			response = yield self.list_dirs(path,after_child,False)
			if response is None:
				break

			# gen child
			for entry in response.dirList.partialListing:
				# update last
				after_child = entry.path
				yield each(entry)
			
			# more?
			if response.dirList.remainingEntries <= 0:
				break
			
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
	
	@gen.coroutine
	def stream(self,path):
		file_info = yield self.file_info(path)
		blocks = deque((yield self.blocks(path,0,file_info.fs.length)).locations.blocks)
		
		def next_stream(this,current):
			logging.info('close one')
			if current is not None:
				current.close()
			
			# pop one
			block = blocks.pop()
			
			# build datanode
			hosts = map(
				lambda x:(x.id.ipAddr,x.id.xferPort),
				block.locs,
			)
			return Datanode(hosts[0]).stream({
					'block' : block.b.blockId,
					'pool' : block.b.poolId,
					'timestamp' : block.b.generationStamp,
					'length' : block.b.numBytes,
				})
		
		raise gen.Return(_BoundedMultiStream(
			None,
			file_info.fs.length,
			next_stream,
		))

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

class Datanode(ProtocolBuffer):
	
	def __init__(self,host):
		super(Datanode,self).__init__()
		self._host = host
		self._ioloop = None

	def _op(self,op):
		return ''.join([
			struct.pack('!H',28), # transfer protocol version
			struct.pack('!B',op), 
		])

	@gen.coroutine
	def stream(self,block):
		logging.info('connect to:%s fetch blcok:%s' % (self._host,block))
		
		# send op
		op = self._op(81) # org.apache.hadoop.hdfs.protocol.datatransfer.Op#READ_BLOCK
		
		# then payload
		payload = datatransfer_pb2.OpReadBlockProto()
		payload.offset = 0
		payload.len = block['length']
		payload.sendChecksums = False
		payload.header.clientName = HadoopRPC.CLIENT_ID 
		payload.header.baseHeader.block.poolId = block['pool']
		payload.header.baseHeader.block.blockId = block['block']
		payload.header.baseHeader.block.generationStamp = block['timestamp']
		
		# send request
		content = ''.join([
			op,
			self.wrap(payload)
		])
		
		stream = None
		try:

			# establish connection
			ioloop = self._ioloop if self._ioloop is not None else IOLoop.current() 
			stream = BufferedStream((yield TCPClient(io_loop=ioloop).connect(*self._host)))
			
			# send reqeust
			yield stream.write(content)
			
			# read response length
			partial = yield stream.peek(4)
			response_length,offset = self.read_varint32(partial,0)
			yield stream.read(offset)
			
			# parse response	
			response = datatransfer_pb2.BlockOpResponseProto()
			response.ParseFromString((yield stream.read(response_length)))
			
			# check status
			if response.status != datatransfer_pb2.SUCCESS:
				raise IOError('read block %s fail: %s' % (
						block,
						response,
				))
			
			raise gen.Return(
				_BoundedMultiStream(
					stream,
					block['length'],
					lambda self,_:_PacketStream(self.stream()),
				)
			)
		except not gen.Return:
			logging.exception('unexpected io excetion,close stream for%s'%(
				str(self._host),
			))
			stream.close()
		
class _PacketStream(ClosableStream):
	
	def __init__(self,buffered_stream):
		super(_PacketStream,self).__init__(buffered_stream)
		self._inited = False
		self._remainds = 0
			
	@gen.coroutine
	def read(self,hint):		
		if not self._inited:
			yield self._init()
		if self._remainds == 0:
			raise gen.Return(None)
		
		# correct hint
		if self._remainds < hint:
			hint = self._remainds 
		
		content = yield self.stream().read(hint)
		self._remainds -= len(content)
		raise gen.Return(content) 
		
	@gen.coroutine
	def _init(self):
		stream = self.stream()

		partial = yield stream.read(6)
		payload_length = struct.unpack('!I',partial[:4])[0]
		header_length = struct.unpack('!H',partial[4:])[0]
		
		# parse header
		header = datatransfer_pb2.PacketHeaderProto()
		header.ParseFromString((yield stream.read(header_length)))
		
		# calculate checksum length
		checksum_length = payload_length - 4 - header.dataLen 	
		
		# skip checksum
		yield stream.read(checksum_length)
		self._remainds = header.dataLen		
		
		# flag it
		self._inited = True
	
class _BoundedMultiStream(ClosableStream):
	
	def __init__(self,buffered_stream,logical_length,next_stream=None):
		super(_BoundedMultiStream,self).__init__(buffered_stream)
		self._remainds = logical_length
		self._current = None
		
		# with argument (self,current_stream)
		self._next_stream = next_stream

	@gen.coroutine
	def read(self,hint):
		# no more
		if self._remainds == 0:
			raise gen.Return(None)
		
		# any packet?
		if self._current is None:
			current = self._next_stream(self,self._current) 			
			
			# check if is future
			# a bit ugly and tricky
			if isinstance(current,gen.Future):
				current = yield current
			self._current = current

		# do read
		content = yield self._current.read(hint)
		
		# end of packet?
		if content is None:
			self._current = None
			# drill down
			raise gen.Return((yield self.read(hint)))
		
		# did read something,update trackers
		self._remainds -= len(content)
		raise gen.Return(content)
		




