#!/usr/bin/python
# -*- coding: :new utf-8 -*-

import logging

import os

from urlparse import urlparse
from datetime import datetime

from tornado import gen
from tornado.ioloop import IOLoop

from nonblocking import Yarn
from nonblocking import MRClient
from nonblocking import Namenode
from nonblocking import Namenodes

from gen import hdfs_pb2

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
			namenode = namenodes.resolve(job.job_report.jobFile)	
			if namenode is None:
				logging.warn('no namenode for job_file:%s' % job)
				raise gen.Return()

			#TODO			
			info = yield namenode.file_info(urlparse(job_config).path)
			if info is None:
				logging.warn('no job file for application:%s' % attempt)
				yield gen.Return()
			
			blocks = yield namenode.blocks(job_config.path,0,info.fs.length)	
			logging.info(blocks)
			exit(0)
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

@gen.coroutine
def clean_hive_staging_dir(namenodes,dirs):
	now = int((datetime.now() - datetime(1970,1,1)).total_seconds() * 1000)
	logging.info('trigger clean_hive_staging_dir... %s' % datetime.fromtimestamp(now/1000))
	for candidate in dirs:
		namenode = namenodes.resolve(candidate)
		if namenode is None:
			logging.warn('no namenode for file:%s' % candidate)
			raise gen.Return()
		url = urlparse(candidate)
		
		@gen.coroutine	
		def each(entry):
			if not os.path.split(entry.path)[-1].startswith('.hive-staging_hive'):
				raise gen.Return()

			full_path = '%s/%s' % (
				url.path,
				entry.path,
			)
			
			# process older than a day
			if now - entry.modification_time > (7 * 3600 * 24 * 1000):
				trash(namenode,full_path)
		
		# do work
		yield namenode.list_dirs_all(urlparse(candidate).path,each)
			
	IOLoop.current().call_later(5,clean_hive_staging_dir,namenodes,dirs)

@gen.coroutine
def trash(namenode,candidate):
	parent,file_name = os.path.split(candidate)		
	
	trash_root = '/user/hdfs/.Trash/%s%s' % (
		'%s0000' % datetime.now().strftime('%y%m%d%H'),
		parent,
	)
	yield namenode.mkdirs(trash_root)
	
	# move last modify 24 hours ago
	file_info = yield namenode.file_info(candidate)
		
	target = '%s/%s' % (
		trash_root,
		file_name,
	)
	response = yield namenode.move(candidate,target,True)
	logging.info('move %s to %s, done:%s modify:%s' % (
		candidate,
		target,
		response,
		datetime.fromtimestamp(file_info.fs.modification_time/1000),
	))

@gen.coroutine
def clean_hive_scratch_dir(self,dirs):
	now = int((datetime.now() - datetime(1970,1,1)).total_seconds() * 1000)
	logging.info('trigger clean_hive_scratch_dir... %s' % datetime.fromtimestamp(now/1000))	
	for candidate in dirs:
		namenode = namenodes.resolve(candidate)
		if namenode is None:
			logging.warn('no namenode for file:%s' % candidate)
			raise gen.Return()
		url = urlparse(candidate)
		
		@gen.coroutine
		def each(entry):
			if entry.path.startswith('.hive-staging'):
				raise gen.Return()

			full_path = '%s/%s' % (
				url.path,
				entry.path,
			)
				
			# remove file
			if entry.fileType != 1:
				trash(namenode,full_path)
				raise gen.Return()

			@gen.coroutine
			def uuid(child):
				sample = '0034a069-f4f7-4cdf-855a-c7513a2e8e3f'
				path = child.path
				if len(path) != len(sample) and \
					len(path.split('-')) !=  len(sample.split('-')):
					raise gen.Return()
				full = '%s/%s' % (
					full_path,
					child.path,
				)
				if now - child.modification_time > (7 * 3600 * 24 * 1000):
					# remove
					trash(namenode,full)
			# do work
			yield namenode.list_dirs_all(full_path,uuid)
		# do work
		yield namenode.list_dirs_all(urlparse(candidate).path,each)
	# end for

	IOLoop.current().call_later(5,clean_hive_scratch_dir,namenodes,dirs)
	pass

if __name__ == '__main__':
	yarn = Yarn([
		('10.116.100.10',23140),
		('10.116.100.11',23140),
	])
	
	mr_history = MRClient([
		('10.116.100.12',10020),
	],True)
	
	namenodes = Namenodes({
		'sfbd': Namenode([
			('10.116.100.2',8020),
			('10.116.100.1',8020),
		]),
		'sfbdp1': Namenode([
			('10.116.100.3',8020),
			('10.116.100.4',8020),
		])
	})
	
	hive_stagings = [
		'hdfs://sfbdp1/tmp/hive',
	]

	hive_scratch = [
		'hdfs://sfbdp1/tmp/hive',
	]	
			
	#IOLoop.current().add_callback(lambda :move_spark_task(yarn))
	#IOLoop.current().add_callback(lambda :evict_large_spark_task(yarn))
	IOLoop.current().add_callback(lambda :clean_hive_staging_dir(namenodes,hive_stagings))
	IOLoop.current().add_callback(lambda :clean_hive_scratch_dir(namenodes,hive_scratch))
	#IOLoop.current().add_callback(lambda :audit_mr_jobs(mr_history,yarn,namenodes))
	
	IOLoop.current().start()



