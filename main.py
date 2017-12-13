#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

import os

from urlparse import urlparse
from datetime import datetime

from tornado import gen
from tornado.ioloop import IOLoop

from yarn import Yarn
from yarn import MRClient
from hdfs import Namenode
from hdfs import Namenodes
from hdfs import Datanode

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

			job_config_url = urlparse(job_config)			
			info = yield namenode.file_info(job_config_url.path)
			if info is None:
				logging.warn('no job file for application:%s' % attempt)
				yield gen.Return()
			
			#TODO
			logging.info('%s %s' % (job_config_url.path,info.fs.length))		
			blocks = yield namenode.blocks(job_config_url.path,0,info.fs.length)	
			for block in blocks.locations.blocks:
				logging.info(block)
				hosts = map(
					lambda x:(x.id.ipAddr,x.id.xferPort),
					block.locs,
				)
				for host in hosts:
					datanode = Datanode(host)
					stream = yield datanode.stream({
						'block' : block.b.blockId,
						'pool' : block.b.poolId,
						'timestamp' : block.b.generationStamp,
						'length' : block.b.numBytes,
					})
					#print (yield stream.read(204800))
					break
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
	def exclude(entry):
		return os.path.split(entry)[-1].startswith('.hive-staging_hive')
	
	yield clean_dir(namenodes,dirs,exclude,7 * 3600 * 24 * 1000)
			
	IOLoop.current().call_later(5,clean_hive_staging_dir,namenodes,dirs)

@gen.coroutine
def clean_dir(namenodes,dirs,exclude,expire):
	now = int((datetime.now() - datetime(1970,1,1)).total_seconds() * 1000)
	logging.info('trigger clean_dir...%s at %s' % (
		dirs,
		datetime.fromtimestamp(now/1000)
	))
	for candidate in dirs:
		namenode = namenodes.resolve(candidate)
		if namenode is None:
			logging.warn('no namenode for file:%s' % candidate)
			raise gen.Return()
		url = urlparse(candidate)
		
		@gen.coroutine	
		def each(entry):
			if exclude(entry.path):
				raise gen.Return()

			full_path = '%s/%s' % (
				url.path,
				entry.path,
			)
			
			# process older than a day
			if now - entry.modification_time > expire:
				logging.info('remove %s' % full_path)
				trash(namenode,full_path)
		
		# do work
		yield namenode.list_dirs_all(urlparse(candidate).path,each)

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
def clean_hive_scratch_dir(namenodes,dirs):
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
			def exclude(entry):	
				sample = '0034a069-f4f7-4cdf-855a-c7513a2e8e3f'
				return len(sample) != len(entry.path) and len(sample.split('-')) != len(entry.path.split('-'))
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
	#IOLoop.current().add_callback(lambda :clean_hive_staging_dir(namenodes,hive_stagings))
	#IOLoop.current().add_callback(lambda :clean_hive_scratch_dir(namenodes,hive_scratch))
	#IOLoop.current().add_callback(lambda :audit_mr_jobs(mr_history,yarn,namenodes))
	
	@gen.coroutine
	def callback():
		"""
		datanode = Datanode(('10.116.101.44', 50010))
		stream = yield datanode.stream({'timestamp': 180700178L, 'length': 120834L, 'pool': u'BP-690617512-10.116.100.1-1498291718527', 'block': 1132614535L})
		"""
		# /user/ETL_SPMS_CORE/.staging/job_1512123906043_191087/job.xml 188704
		job_config_url = 'hdfs://sfbdp1/user/ETL_SPMS_CORE/.staging/job_1512123906043_92315/job_1512123906043_92315_1_conf.xml'
		job_config_url = 'hdfs://sfbdp1/user/hive/warehouse/tmp_o2o.db/tt_order_state/000000_0'
		namenode = namenodes.resolve(job_config_url)
		
		job_config_url = urlparse(job_config_url)	
		info = yield namenode.file_info(job_config_url.path)
		logging.info(info)	
		blocks = yield namenode.blocks(job_config_url.path,0,info.fs.length)
		stream = yield namenode.stream(job_config_url.path)
		with open('out','w') as f:
			while True:
				content = yield stream.read(1024)
				if content is None:
					break;
				f.write(content)
		exit()

	@gen.coroutine
	def callback2():
		job_config_url = 'hdfs://sfbdp1/user/hive/warehouse/tmp_o2o.db/tt_order_state/000000_0'
		namenode = namenodes.resolve(job_config_url)
		
		job_config_url = urlparse(job_config_url)	
		info = yield namenode.file_info(job_config_url.path)
		logging.info(info)	
		blocks = yield namenode.blocks(job_config_url.path,0,info.fs.length)
		
		block = blocks.locations.blocks[0]
		#logging.info(block)
		datanode = Datanode((block.locs[0].id.ipAddr,block.locs[0].id.xferPort))
		stream = yield datanode.stream({
			'block' : block.b.blockId,
			'pool' : block.b.poolId,
			'timestamp' : block.b.generationStamp,
			'length' : block.b.numBytes,
			'offset' : 0,
		})
			
		logging.info('try seek...')
		yield stream.seek(100)

		logging.info('try read....')
		yield stream.read(100)
		
		logging.info('done')
		
		exit(0)	
	def exclude(path):
		return 'inpregress' in path
	#IOLoop.current().add_callback(lambda :clean_dir(namenodes,['hdfs://sfbdp1/user/spark/applicationHistory'],exclude))	
	#IOLoop.current().add_callback(callback)
	#IOLoop.current().add_callback(callback2)
	IOLoop.current().start()



