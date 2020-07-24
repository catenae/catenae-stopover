#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#          ◼◼◼            ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼     ◼◼◼         ◼◼◼       ◼◼◼           ◼◼                ◼◼◼
#        ◼◼◼             ◼◼◼◼            ◼◼           ◼◼◼           ◼◼◼◼      ◼◼◼          ◼◼◼◼             ◼◼◼
#      ◼◼◼             ◼◼◼  ◼◼◼          ◼◼         ◼◼◼             ◼◼◼◼◼     ◼◼◼        ◼◼◼  ◼◼◼         ◼◼◼
#    ◼◼◼              ◼◼◼    ◼◼◼         ◼◼       ◼◼◼               ◼◼◼ ◼◼◼   ◼◼◼       ◼◼◼    ◼◼◼      ◼◼◼
#  ◼◼◼               ◼◼◼      ◼◼◼        ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼  ◼◼◼  ◼◼◼  ◼◼◼      ◼◼◼      ◼◼◼   ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼
#    ◼◼◼            ◼◼◼        ◼◼◼       ◼◼       ◼◼◼               ◼◼◼    ◼◼ ◼◼◼     ◼◼◼        ◼◼◼    ◼◼◼
#      ◼◼◼         ◼◼◼          ◼◼◼      ◼◼         ◼◼◼             ◼◼◼     ◼◼◼◼◼    ◼◼◼          ◼◼◼     ◼◼◼
#        ◼◼◼      ◼◼◼            ◼◼◼     ◼◼           ◼◼◼           ◼◼◼      ◼◼◼◼   ◼◼◼            ◼◼◼      ◼◼◼
#          ◼◼◼   ◼◼◼              ◼◼◼    ◼◼             ◼◼◼         ◼◼◼       ◼◼◼  ◼◼◼              ◼◼◼       ◼◼◼
#
# Catenae 3.0.0 Graphene
# Copyright (C) 2017-2020 Rodrigo Martínez Castaño
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from stopover import Stopover, Message
import time
import catenae
from os import environ
import signal
import traceback
from .logger import Logger
from .threading import Thread, Lock, current_thread
from . import utils


def suicide_on_error(method):
    def suicide_on_error_(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            traceback.print_exc(e)
            self.suicide(f'error when executing {method}')

    return suicide_on_error_


class Link:
    DEFAULT_CONFIG = {
        'no_messages_sleep_interval': .5,
        'intervals': {
            'loop_check_stop': 1,
            'loop_check_start': 1
        }
    }

    def __init__(self,
                 endpoint: str = None,
                 input_stream: str = None,
                 input_streams: list = None,
                 default_output_stream: str = None,
                 receiver_group: str = None,
                 log_level: str = None,
                 **ignored_kwargs):
        self.stopover = Stopover('http://localhost:8080')

        self.logger = Logger(self, level=log_level)

        if input_stream:
            input_streams = [input_stream]

        if ignored_kwargs:
            self.logger.log(
                f'the following kwargs were ignored: {ignored_kwargs}')

        self._config = dict(Link.DEFAULT_CONFIG)
        self._config.update({
            'input_streams':
            input_streams,
            'default_output_stream':
            default_output_stream,
            'receiver_group':
            receiver_group if receiver_group else self.__class__.__name__,
        })

        self._set_uid()

        self._threads = []
        self._locks = {'_threads': Lock(), 'start_stop': Lock()}

        self._started = False
        self._stopped = False

    @property
    def uid(self):
        return self._uid

    @property
    def config(self):
        return self._config

    def setup(self):
        pass

    def start(self,
              startup_text: str = None,
              setup_kwargs: dict = None,
              **ignored_kwargs):
        if not startup_text:
            self.logger.log(catenae.text_logo)
            self.logger.log(
                f'Catenae v{catenae.__version__} {catenae.__version_name__}')

        with self._locks['start_stop']:
            self._started = True

        if setup_kwargs is None:
            setup_kwargs = {}
        self.setup(**setup_kwargs)

        if hasattr(self, 'generator'):
            self._threads.append(self.loop(self.generator, interval=0))

        if hasattr(self, 'transform'):
            self._threads.append(self.launch_thread(self._transform_loop))

        for thread in self._threads:
            thread.join()

    def stop(self):
        pass

    @suicide_on_error
    def send(self, message, stream: str = None):
        stream = self.config[
            'default_output_stream'] if stream is None else stream
        if stream is None:
            raise ValueError('stream not provided')
        self.stopover.put(message, stream)

    @suicide_on_error
    def launch_thread(self, target, args=None, kwargs=None, safe_stop=False):
        thread = Thread(target, args=args, kwargs=kwargs)
        if safe_stop:
            with self._locks['_threads']:
                self._threads.append(thread)
        thread.daemon = True
        thread.start()
        return thread

    @suicide_on_error
    def loop(self,
             target,
             args=None,
             kwargs=None,
             interval=0,
             wait=False,
             level='debug',
             safe_stop=True):
        loop_task_kwargs = {
            'target': target,
            'args': args,
            'kwargs': kwargs,
            'interval': interval,
            'wait': wait,
            'level': level
        }
        thread = Thread(self._loop_task, kwargs=loop_task_kwargs)
        if safe_stop:
            with self._locks['_threads']:
                self._threads.append(thread)
        thread.daemon = True
        thread.start()
        return thread

    def suicide(self, message=None, exception=False):
        with self._locks['start_stop']:
            if self._stopped:
                return
            self._stopped = True

        try:
            self.stop()
        except Exception:
            self.logger.log('error when executing stop()', level='exception')

        if message is None:
            message = '[SUICIDE]'
        else:
            message = f'[SUICIDE] {message}'

        if exception:
            self.logger.log(message, level='exception')
        else:
            self.logger.log(message, level='warn')

        while not self._started:
            time.sleep(self.config['intervals']['loop_check_start'])

        with self._locks['_threads']:
            for thread in self._threads:
                thread.stop()

    def _set_uid(self):
        if 'CATENAE_DOCKER' in environ and bool(environ['CATENAE_DOCKER']):
            self._uid = environ['HOSTNAME']
        else:
            self._uid = utils.get_uid()

    @suicide_on_error
    def _transform_loop(self):
        while not current_thread().will_stop:
            for input_stream in self.config['input_streams']:
                message = self.stopover.get(input_stream,
                                            self.config['receiver_group'])

                if not message:
                    time.sleep(self.config['no_messages_sleep_interval'])
                    continue

                result = self.transform(message)
                if isinstance(result, Message):
                    value = result.value
                else:
                    value = result
                if value and self.config['default_output_stream']:
                    self.stopover.put(value,
                                      self.config['default_output_stream'])

                self.stopover.commit(message, self.config['receiver_group'])

    @suicide_on_error
    def _loop_task(self, target, args, kwargs, interval, wait, level):
        if wait:
            time.sleep(interval)

        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        while not current_thread().will_stop:
            try:
                self.logger.log(f'new loop iteration ({target.__name__})',
                                level=level)
                start_timestamp = utils.get_timestamp()

                target(*args, **kwargs)

                while not current_thread().will_stop:
                    continue_sleeping = (utils.get_timestamp() -
                                         start_timestamp) < interval
                    if not continue_sleeping:
                        break
                    time.sleep(self.config['intervals']['loop_check_stop'])

            except Exception:
                self.logger.log(
                    f'exception raised when executing the loop: {target.__name__}',
                    level='exception')

    def _setup_signals_handler(self):
        for signal_name in ['SIGINT', 'SIGTERM', 'SIGQUIT']:
            signal.signal(getattr(signal, signal_name), self._signal_handler)

    def _signal_handler(self, sig, frame):
        if sig == signal.SIGINT:
            self.suicide('SIGINT')
        elif sig == signal.SIGTERM:
            self.suicide('SIGTERM')
        elif sig == signal.SIGQUIT:
            self.suicide('SIGQUIT')
