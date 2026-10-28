#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
from .queue import ThreadingQueue
from .errors import EmptyError


def should_stop():
    return getattr(threading.current_thread(), 'will_stop', False)


class Thread(threading.Thread):
    def __init__(
        self,
        target,
        args=None,
        kwargs=None,
    ):
        if args is None:
            args = tuple()
        elif isinstance(args, list):
            args = tuple(args)
        elif not isinstance(args, tuple):
            args = tuple([args])

        if kwargs is None:
            kwargs = dict()

        super().__init__(target=target, args=args, kwargs=kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def wait(self, timeout):
        return self._stop_event.wait(timeout)

    @property
    def will_stop(self):
        return self._stop_event.is_set()


class ThreadPool:
    def __init__(
        self,
        link_instance,
        num_threads=1,
    ):
        if num_threads < 1:
            raise ValueError('num_threads must be positive')
        self.link_instance = link_instance
        self.tasks_queue = ThreadingQueue()
        self.threads = []

        for i in range(num_threads):
            thread = Thread(self._worker_target, i)
            self.threads.append(thread)
            thread.start()

    def stop(self):
        for thread in self.threads:
            thread.stop()
        for thread in self.threads:
            if thread is not threading.current_thread():
                thread.join()

    def submit(
        self,
        target,
        args=None,
        kwargs=None,
    ):
        if args is None:
            args = []

        if not isinstance(args, (list, tuple)):
            args = [args]

        if kwargs is None:
            kwargs = {}

        self.tasks_queue.put((target, args, kwargs))

    def _worker_target(self, index):
        while not self.threads[index].will_stop:
            try:
                target, args, kwargs = self.tasks_queue.get(
                    timeout=1,
                )
                target(*args, **kwargs)

            except EmptyError:
                pass

            except Exception:
                self.link_instance.logger.log(
                    'exception during the execution of a task',
                    level='exception',
                )
