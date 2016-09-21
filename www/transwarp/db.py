#!/usr/bin/env python
# coding: utf-8

import logging
import threading

engine = None

class _Engine(object):
	def __init__(self, connect):
		self._connect = connect
	
	def connect(self):
		return self._connect()

def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
	import mysql.connector
	global engine
	if engine is not None:
		raise DBError('Engine is already initialized.')

	params = dict(user=user, password=password, database=database, host=host, port=port)
	defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
	for k, v in defaults.iteritems():
		params[k] = kw.pop(k, v)
	params.update(kw)
	params['buffered'] = True
	conn = mysql.connector.connect(**params)
	engine = _Engine(lambda: conn)
	logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


class  _LasyConnection(object):    
	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			connection = engine.connect()
			logging.info('open connection <%s>' % hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()

	def rollback(self):
		self.connection.rollback()

	def cleanup(self):
		if self.connection:
			connection = self.connection()
			self.connection = None
			logging.info('close connection <%s>' % hex(id(connection)))
			connection.close()
			

class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None
		
	def init(self):
		logging.info('open lazy connection...')
		self.connection = _LasyConnection()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		self.connection = None

	def cursor(self):
		return self.connection.cursor()


_db_ctx = _DbCtx()


class  _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()	

def connection():
	return _ConnectionCtx()		


def DBError(Exception):
	pass

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('user', 'www-data', 'blogdb')
	with connection():
		pass
	
