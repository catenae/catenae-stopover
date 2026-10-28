#!/usr/bin/env python
# -*- coding: utf-8 -*-

from queue import Empty, Full, Queue
from threading import Lock
from .errors import EmptyError, TimeoutError


class CustomQueue:
    BLOCKING_SECONDS = 0.1

    def __init__(self, size=0, circular=False):
        self._size = size
        self._circular = circular

    def put(self):
        pass

    def get(self):
        pass


class ThreadingQueue(CustomQueue):
    def __init__(self, size=0, circular=False):
        super().__init__(size, circular)
        self._queue = Queue(maxsize=size)
        self._lock = Lock()

    def put(self, item, block=True, timeout=None):
        if self._circular:
            with self._lock:
                if self._size > 0 and self._queue.full():
                    try:
                        self._queue.get_nowait()
                    except Empty:
                        pass
                self._queue.put_nowait(item)
            return

        try:
            self._queue.put(item, block=block,
                            timeout=timeout if block else None)
        except Full as error:
            raise TimeoutError('queue is full') from error

    def get(self, block=True, timeout=None):
        try:
            return self._queue.get(block=block,
                                   timeout=timeout if block else None)
        except Empty as error:
            raise EmptyError('queue is empty') from error
