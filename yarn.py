#!/usr/bin/python
# -*- coding: : utf-8 -*-

import logging

from tornado import gen

from nonblocking import MultiRPC

from gen import yarn_service_protos_pb2
from gen import yarn_protos_pb2
from gen import mr_service_protos_pb2

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


