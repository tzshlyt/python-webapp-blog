#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading, logging

ctx = threading.local()

class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


class HttpError(Exception):
	pass

class Request(object):
	pass

class Response(object):
	pass

def get(path):
	pass
		
		
def post(path):
	pass

def view(path):
	pass


class WSGIApplication(object):

	def __init__(self, document_root=None, **kw):
		self._document_root = document_root

	def get_wsgi_application(self):

		def wsig(env, start_response):
			status = '200 OK'
			response_headers = [
            	('Content-Type', 'text/plain')
        	]
			start_response(status, response_headers)
			return ['Hello world from wsgia!\n']
		return wsig
		
	def run(self, port=9000, host='127.0.0.1'):
		from wsgiref.simple_server import make_server
		print ('application (%s) will start at %s:%s...' % (self._document_root, host, port))
		server = make_server(host, port, self.get_wsgi_application())
		server.serve_forever()

wsgi = WSGIApplication()
if __name__ == '__main__':
	wsgi.run()
else:
	application = wsgi.get_wsgi_application()

		