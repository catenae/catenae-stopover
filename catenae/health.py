#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cherrybone import Server
import falcon


class HealthCheck:
    def __init__(self, port):
        self.port = port
        self.api = falcon.App()
        self.api.add_route('/health', self)

    def on_get(self, _, response):
        response.media = {'status': 'available'}

    def start(self):
        Server(self.api, port=self.port).start()
