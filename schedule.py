#!/usr/bin/python
# -*- coding: :new utf-8 -*-

import logging

from urlparse import urlparse

from tornado import gen
from tornado.ioloop import IOLoop

from nonblocking import Yarn
from nonblocking import MRClient
from nonblocking import Namenode

from futures import Delays

logging.basicConfig(level=logging.INFO,format=u'%(asctime)s [%(levelname)s] (%(name)s) {%(pathname)s:%(lineno)d@%(funcName)s} -  %(message)s')

@gen.coroutine
def move_spark_task(yarn):
	logging.info('trigger move_spark_task...')
	for application in filter(
			lambda x: x.queue == 'root.default' and x.applicationType == 'SPARK', 
			(yield yarn.running_applications()).applications
		):
		logging.info('moving spark application:%s from root.default to root.spark' % application)
		ok = yield yarn.move_to_queue(
			{
				'id': application.applicationId.id,
				'cluster_timestamp' : application.applicationId.cluster_timestamp,
			},
			'root.spark'
		)
		if not ok:
			logging.info('fail to move spark application_%s_%s to queue:root.spark,killing it' % (
				application.applicationId.id,
				application.applicationId.cluster_timestamp,
			))
			yield yarn.kill({
				'id': application.applicationId.id,
				'cluster_timestamp' : application.applicationId.cluster_timestamp,
			})
	# requeue
	IOLoop.current().call_later(5,move_spark_task,yarn)

@gen.coroutine
def evict_large_spark_task(yarn):
	logging.info('triger evict_large_spark_task...')
	for application in filter(
			lambda x: x.applicationType == 'SPARK',
			(yield yarn.running_applications()).applications,
		):
		"""
		app_resource_Usage {
		  num_used_containers: 17
		  num_reserved_containers: 0
		  used_resources {
		    memory: 69632
		    virtual_cores: 33
		  }
		  reserved_resources {
		    memory: 0
		    virtual_cores: 0
		  }
		  needed_resources {
		    memory: 69632
		    virtual_cores: 33
		  }
		  memory_seconds: 17216444230
		  vcore_seconds: 8159203
		}
		"""
		requested = application.app_resource_Usage.needed_resources
		if requested.memory > 250*10240:
			logging.info('spark application request too much memory(2,500G), killing :%s' % application)
			yield yarn.kill({
				'id': application.applicationId.id,
				'cluster_timestamp' : application.applicationId.cluster_timestamp,
			})
	
	# requeue
	IOLoop.current().call_later(5,evict_large_spark_task,yarn)

@gen.coroutine
def audit_mr_jobs(mr_history,yarn,namenodes):
	for application in filter(
			lambda x:x.applicationType == 'MAPREDUCE', 
			(yield yarn.running_applications()).applications
		):
		
		# prepare callback	
		@gen.coroutine	
		def attempt_callback(future):
			attempt = future.result()
			# no attempt
			if attempt is None:
				raise gen.Return()
	
			mr = MRClient([
				(attempt.application_attempt_report.host,attempt.application_attempt_report.rpc_port)
			])

			# fetch job report
			job = yield mr.jobreport({
					'id' : attempt.application_attempt_report.application_attempt_id.application_id.id,
					'cluster_timestamp' : attempt.application_attempt_report.application_attempt_id.application_id.cluster_timestamp,
				},
				attempt.application_attempt_report.application_attempt_id.application_id.id,
			)
			yield mr.close()
			
			job_config = job.job_report.jobFile	
			job_config = urlparse(job_config)
			if job_config.scheme != 'hdfs':	
				raise gen.Return()

			name_service = job_config.netloc.split(':')[0]
			namenode = namenodes.get(name_service,None)
			if namenode is None:
				logging.warn('no namenode for job file:%s' % job_config)
				raise gen.Return()
			
			#TODO			
			info = yield namenode.file_info(job_config.path)
			if info is None:
				#logging.info(job_config)
				pass
			else:
				#logging.info(info)
				pass
			raise gen.Return()
	
		# fetch attempt
		yarn.application_attempt({
				'id': application.applicationId.id,
				'cluster_timestamp' : application.applicationId.cluster_timestamp,
			},
			application.currentApplicationAttemptId.attemptId
		).add_done_callback(attempt_callback)

	# end loop
	raise gen.Return()

if __name__ == '__main__':
	yarn = Yarn([
		('10.116.100.10',23140),
		('10.116.100.11',23140),
	])
	
	mr_history = MRClient([
		('10.116.100.12',10020),
	],True)
	
	namenodes = {
		'sfbd': Namenode([
			('10.116.100.2',8020),
			('10.116.100.1',8020),
		]),
		'sfbdp1': Namenode([
			('10.116.100.3',8020),
			('10.116.100.4',8020),
		])
	}
	
	IOLoop.current().add_callback(lambda :move_spark_task(yarn))
	IOLoop.current().add_callback(lambda :evict_large_spark_task(yarn))
	#IOLoop.current().add_callback(lambda :audit_mr_jobs(mr_history,yarn,namenodes))
	IOLoop.current().start()



