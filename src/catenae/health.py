#!/usr/bin/env python
# -*- coding: utf-8 -*-

from wsgiref.simple_server import WSGIServer, WSGIRequestHandler
import falcon


class NoLoggingWSGIRequestHandler(WSGIRequestHandler):
    def log_message(self, *_):
        pass


class HealthCheck:
    def __init__(self, port):
        self.server = WSGIServer(
            ('0.0.0.0', port), NoLoggingWSGIRequestHandler)
        api = falcon.App()
        api.add_route('/health', self)
        self.server.set_app(api)

    def on_get(self, _, response):
        response.media = {'status': 'available'}

    def start(self):
        self.server.serve_forever()
