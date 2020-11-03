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
# Catenae 3.x Graphene
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

from stopover import Stopover, MessageResponse
import time
import catenae
from os import environ
import signal
import traceback
import argparse
from .logger import Logger
from .threading import Thread, Lock, current_thread
from . import utils


def suicide_on_error(method):
    def suicide_on_error_(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception:
            traceback.print_exc()
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
                 endpoints: list = None,
                 input_stream: str = None,
                 input_streams: list = None,
                 default_output_stream: str = None,
                 receiver_group: str = None,
                 log_level: str = None,
                 **ignored_kwargs):
        self.logger = Logger(self, level=log_level)

        if endpoints is None:
            endpoints = ['http://localhost:5704']

        if input_stream:
            input_streams = [input_stream]
        if input_streams is None:
            input_streams = []

        receiver_group = receiver_group if receiver_group else self.__class__.__name__
        self._config = dict(Link.DEFAULT_CONFIG)
        self._config.update({
            'endpoints': endpoints,
            'input_streams': input_streams,
            'default_output_stream': default_output_stream,
            'receiver_group': receiver_group,
        })
        self._set_uid()
        self._load_args()

        if ignored_kwargs:
            self.logger.log(f'the following kwargs were ignored: {ignored_kwargs}')

        if self._config['endpoints']:
            self.stopover = Stopover(endpoint=self._config['endpoints'][0], uid=self._config['uid'])
        else:
            self.stopover = None

        self._threads = []
        self._locks = {'_threads': Lock(), 'start_stop': Lock()}

        self._started = False
        self._stopped = False

    @property
    def env(self):
        return dict(environ)

    @property
    def args(self):
        return list(self._args)

    def _load_args(self):
        parser = argparse.ArgumentParser()

        parser.add_argument('-e',
                            '--endpoint',
                            action='store',
                            dest='endpoint',
                            help='Message broker endpoint. \
                            E.g., http://localhost:9092',
                            required=False)

        parser.add_argument('-i',
                            '--input',
                            action='store',
                            dest='input_streams',
                            help='Input streams. Several streams ' +
                            'can be specified separated by commas',
                            required=False)

        parser.add_argument('-o',
                            '--output',
                            action='store',
                            dest='default_output_stream',
                            help='Default output stream.',
                            required=False)

        parser.add_argument('-g',
                            '--receiver-group',
                            action='store',
                            dest='receiver_group',
                            help='Receiver group.',
                            required=False)

        parser.add_argument('-u',
                            '--uid',
                            action='store',
                            dest='uid',
                            help='Link\'s Unique ID.',
                            required=False)

        parsed_args = parser.parse_known_args()
        link_args = parsed_args[0]
        self._args = parsed_args[1]

        if link_args.endpoint:
            self._config['endpoints'] = link_args.endpoint.split(',')

        if link_args.input_streams:
            self._config['input_streams'] = link_args.input_streams.split(',')

        if link_args.default_output_stream:
            self._config['default_output_stream'] = link_args.default_output_stream

        if link_args.receiver_group:
            self._config['receiver_group'] = link_args.receiver_group

        if link_args.uid:
            self._config['uid'] = link_args.uid

    @property
    def uid(self):
        return self._config['uid']

    @property
    def config(self):
        return self._config

    def setup(self):
        pass

    def start(self, startup_text: str = None, setup_kwargs: dict = None, **ignored_kwargs):
        if not startup_text:
            self.logger.log(catenae.text_logo)
            self.logger.log(f'Catenae v{catenae.__version__} {catenae.__version_name__}')

        with self._locks['start_stop']:
            self._started = True

        if setup_kwargs is None:
            setup_kwargs = {}
        self.setup(**setup_kwargs)

        if hasattr(self, 'generator'):
            self._threads.append(self.loop(self.generator, interval=0))

        if self.stopover is not None:
            if hasattr(self, 'transform'):
                self._threads.append(self.loop(self._transform))

            self._threads.append(
                self.loop(self.stopover.knock,
                          kwargs={'receiver_group': self.config['receiver_group']},
                          interval=5))

        for thread in self._threads:
            thread.join()

    def stop(self):
        pass

    @suicide_on_error
    def send(self, message, stream: str = None):
        stream = self.config['default_output_stream'] if stream is None else stream
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
    def loop(self, target, args=None, kwargs=None, interval=0, wait=False, safe_stop=True):
        loop_task_kwargs = {
            'target': target,
            'args': args,
            'kwargs': kwargs,
            'interval': interval,
            'wait': wait,
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
            self._config['uid'] = environ['HOSTNAME']
        else:
            self._config['uid'] = utils.get_uid()

    @suicide_on_error
    def _transform(self):
        for input_stream in self.config['input_streams']:
            message = self.stopover.get(input_stream, self.config['receiver_group'])

            if not message:
                time.sleep(self.config['no_messages_sleep_interval'])
                continue

            result = self.transform(message)
            output = result.value if isinstance(result, MessageResponse) else result

            if output:
                if self.config['default_output_stream']:
                    self.stopover.put(output, self.config['default_output_stream'])
                else:
                    raise ValueError('default stream is missing')

            self.stopover.commit(message, self.config['receiver_group'])

    @suicide_on_error
    def _loop_task(self, target, args, kwargs, interval, wait):
        if wait:
            time.sleep(interval)

        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        while not current_thread().will_stop:
            start_timestamp = utils.get_timestamp()

            target(*args, **kwargs)

            while not current_thread().will_stop:
                continue_sleeping = (utils.get_timestamp() - start_timestamp) < interval
                if not continue_sleeping:
                    break
                time.sleep(self.config['intervals']['loop_check_stop'])

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