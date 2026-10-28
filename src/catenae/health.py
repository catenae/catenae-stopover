#!/usr/bin/env python
# -*- coding: utf-8 -*-

import falcon
from waitress.server import create_server
from threading import Event, Lock, Thread
import time


class HealthCheck:
    def __init__(self, port, readiness=None, liveness=None):
        self._closed = Event()
        self._started = Event()
        self._close_lock = Lock()
        self._readiness = readiness
        self._liveness = liveness
        self._checked_at = 0
        self._checks = {'setup': False}
        api = falcon.App()
        api.add_route('/health', self)
        api.add_route('/ready', self)
        api.add_route('/live', self)
        self.server = create_server(
            api, host='0.0.0.0', port=port, threads=4,
            channel_timeout=5, cleanup_interval=1,
        )

    def on_get(self, request, response):
        live = self._liveness is None or self._liveness()
        available = live
        if request.path != '/live' and self._readiness is not None:
            available = live and time.monotonic() - self._checked_at < 10 and all(self._checks.values())
        if not available:
            response.status = falcon.HTTP_503
        response.media = {'status': 'available' if available else 'unavailable'}
        if request.path != '/live' and self._readiness is not None:
            response.media['checks'] = dict(self._checks)

    def _monitor(self):
        while not self._closed.is_set():
            try:
                checks = self._readiness()
                self._checks = dict(checks)
            except Exception:
                self._checks = {'dependencies': False}
            self._checked_at = time.monotonic()
            self._closed.wait(1)

    def start(self):
        with self._close_lock:
            if self._closed.is_set():
                return
            self._started.set()
        if self._readiness is not None:
            Thread(target=self._monitor, daemon=True).start()
        try:
            self.server.run()
        finally:
            self._closed.set()

    def close(self):
        with self._close_lock:
            if self._closed.is_set():
                return

            def close_on_owner():
                # Close sockets on the select-loop owner, not another thread.
                for channel in list(self.server.active_channels.values()):
                    channel.close()
                self.server.close()

            self.server.task_dispatcher.shutdown()
            if not self._started.is_set():
                close_on_owner()
                self._closed.set()
                return
            self.server.trigger.pull_trigger(close_on_owner)
            if not self._closed.wait(5):
                raise TimeoutError('Health server did not stop')
