#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#                                                                                                                         # noqa: E501
#          ◼◼◼            ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼     ◼◼◼         ◼◼◼       ◼◼◼           ◼◼                ◼◼◼         # noqa: E501
#        ◼◼◼             ◼◼◼◼            ◼◼           ◼◼◼           ◼◼◼◼      ◼◼◼          ◼◼◼◼             ◼◼◼           # noqa: E501
#      ◼◼◼             ◼◼◼  ◼◼◼          ◼◼         ◼◼◼             ◼◼◼◼◼     ◼◼◼        ◼◼◼  ◼◼◼         ◼◼◼             # noqa: E501
#    ◼◼◼              ◼◼◼    ◼◼◼         ◼◼       ◼◼◼               ◼◼◼ ◼◼◼   ◼◼◼       ◼◼◼    ◼◼◼      ◼◼◼               # noqa: E501
#  ◼◼◼               ◼◼◼      ◼◼◼        ◼◼     ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼  ◼◼◼  ◼◼◼  ◼◼◼      ◼◼◼      ◼◼◼   ◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼◼  # noqa: E501
#    ◼◼◼            ◼◼◼        ◼◼◼       ◼◼       ◼◼◼               ◼◼◼    ◼◼ ◼◼◼     ◼◼◼        ◼◼◼    ◼◼◼               # noqa: E501
#      ◼◼◼         ◼◼◼          ◼◼◼      ◼◼         ◼◼◼             ◼◼◼     ◼◼◼◼◼    ◼◼◼          ◼◼◼     ◼◼◼             # noqa: E501
#        ◼◼◼      ◼◼◼            ◼◼◼     ◼◼           ◼◼◼           ◼◼◼      ◼◼◼◼   ◼◼◼            ◼◼◼      ◼◼◼           # noqa: E501
#          ◼◼◼   ◼◼◼              ◼◼◼    ◼◼             ◼◼◼         ◼◼◼       ◼◼◼  ◼◼◼              ◼◼◼       ◼◼◼         # noqa: E501
#                                                                                                                         # noqa: E501
#
# Catenae for Stopover
# Copyright (C) 2017-2021 Rodrigo Martínez Castaño
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

import stopover
from typing import List, Dict
import time
import catenae
from os import environ
import signal
import traceback
import argparse
from .logger import Logger
from .threading import Thread
from threading import Lock, current_thread
from .health import HealthCheck
from . import utils

_rpc_enabled_methods = set()


def rpc(method):
    if (method.__name__ not in _rpc_enabled_methods
            and method.__name__ != 'suicide_on_error'):
        _rpc_enabled_methods.add(method.__name__)
    return method


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
                 endpoint: str = None,
                 endpoints: List = None,
                 input_stream: str = None,
                 input_streams: List = None,
                 default_output_stream: str = None,
                 receiver_group: str = None,
                 rpc_enabled: bool = None,
                 rpc_by_uid: bool = None,
                 enable_health: bool = None,
                 health_port: int = None,
                 log_level: str = None,
                 progress_without_commit: bool = None,
                 **ignored_kwargs):
        self.logger = Logger(self, level=log_level)

        if endpoint is not None:
            endpoints = [endpoint]
        if not endpoints:
            endpoints = []

        if input_stream is not None:
            input_streams = [input_stream]
        if not input_streams:
            input_streams = []

        self._uncommitted_messages = []

        self._config = dict(Link.DEFAULT_CONFIG)
        self._set_uid()
        self._config.update({
            'endpoints': endpoints,
            'input_streams': input_streams,
            'default_output_stream': default_output_stream,
            'receiver_group': (receiver_group if receiver_group
                            else self.__class__.__name__),
            'rpc_enabled': True if rpc_enabled else False,
            'enable_health': False if enable_health is False else True,
            'health_port': health_port if health_port else 2094,
            'rpc_topics': [f'catenae_rpc_{self.__class__.__name__.lower()}',
                            'catenae_rpc_broadcast'],
            'progress_without_commit': (True if progress_without_commit
                                        else False),
        })
        if rpc_by_uid:
            self._config['rpc_topics'].append(f'catenae_rpc_{self.uid}')

        self._load_args()

        if ignored_kwargs:
            self.logger.log(
                f'the following kwargs were ignored: {ignored_kwargs}')

        if self._config['endpoints']:
            self.stopover = stopover.Stopover(
                endpoint=self._config['endpoints'][0],
                uid=self._config['uid']
            )
        else:
            self.stopover = None

        self._threads = []
        self._locks = {
            'threads': Lock(),
            'start_stop': Lock(),
            'rpc_lock': Lock()
        }

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
                            '--endpoints',
                            action='store',
                            dest='endpoint',
                            help='Message broker endpoint. \
                            E.g., http://localhost:5704',
                            required=False)

        parser.add_argument('-i',
                            '--input',
                            action='store',
                            dest='input_streams',
                            help='Input streams. Several streams '
                            + 'can be specified separated by commas',
                            required=False)

        parser.add_argument('-o',
                            '--default-output',
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

        parser.add_argument('-r',
                            '--rpc',
                            action='store_true',
                            dest='rpc_enabled',
                            help='Enable RPC.',
                            required=False)

        parsed_args = parser.parse_known_args()
        link_args = parsed_args[0]
        self._args = parsed_args[1]

        if link_args.endpoint:
            self._config['endpoints'] = link_args.endpoint.split(',')

        if link_args.input_streams:
            self._config['input_streams'] = link_args.input_streams.split(',')

        if link_args.default_output_stream:
            self._config[
                'default_output_stream'] = link_args.default_output_stream

        if link_args.receiver_group:
            self._config['receiver_group'] = link_args.receiver_group

        if link_args.uid:
            self._config['uid'] = link_args.uid

        if link_args.rpc_enabled:
            self._config['rpc_enabled'] = link_args.rpc_enabled

    @property
    def uid(self):
        return self._config['uid']

    @property
    def config(self):
        return dict(self._config)

    def setup(self):
        pass

    def start(self,
              startup_text: str = None,
              setup_kwargs: Dict = None,
              embedded: bool = False,
              **_):
        with self._locks['start_stop']:
            if self._started:
                return

        if not startup_text:
            self.logger.log(catenae.text_logo)
        self.logger.log(f'catenae  v{catenae.__version__}')
        self.logger.log(f'stopover v{stopover.__version__}')

        self.logger.log(
            f'configuration:\n{utils.dump_dict_pretty(self._config)}')

        if startup_text:
            self.logger.log(startup_text)

        with self._locks['start_stop']:
            self._started = True

        if setup_kwargs is None:
            setup_kwargs = {}
        self.setup(**setup_kwargs)

        if hasattr(self, 'generator'):
            self._threads.append(self.loop(self.generator))

        if self.stopover is not None:
            if self._config['rpc_enabled']:
                self._threads.append(self.loop(self._rpc_notify_handler))

            if hasattr(self, 'transform'):
                self._threads.append(self.loop(self._transform))

            self._threads.append(
                self.loop(
                    self.stopover.knock,
                    kwargs={'receiver_group': self.config['receiver_group']},
                    interval=5))

        if self.config['enable_health']:
            health_server = HealthCheck(self.config['health_port'])
            self.launch_thread(health_server.start)

        if not embedded:
            self._setup_signals_handler()
            for thread in self._threads:
                thread.join()

    def stop(self):
        pass

    def send(self, message, stream: str = None):
        stream = self.config[
            'default_output_stream'] if stream is None else stream
        if stream is None:
            raise ValueError('stream not provided')
        self.stopover.put(message, stream)

    def launch_thread(
        self,
        target,
        args=None,
        kwargs=None,
        safe_stop=False,
    ):
        thread = Thread(target, args=args, kwargs=kwargs)
        if safe_stop:
            with self._locks['threads']:
                self._threads.append(thread)
        thread.daemon = True
        thread.start()
        return thread

    def loop(
        self,
        target,
        args=None,
        kwargs=None,
        interval=0,
        wait=False,
        safe_stop=True,
    ):
        loop_task_kwargs = {
            'target': target,
            'args': args,
            'kwargs': kwargs,
            'interval': interval,
            'wait': wait,
        }
        thread = self.launch_thread(self._loop_task,
                                    kwargs=loop_task_kwargs,
                                    safe_stop=safe_stop)
        return thread

    def rpc_notify(
        self,
        method=None,
        args=None,
        kwargs=None,
        to='broadcast',
    ):
        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        if not method:
            raise ValueError
        topic = f'catenae_rpc_{to.lower()}'
        call = {
            'method': method,
            'context': {
                'group': self.config['receiver_group'],
                'uid': self.uid
            },
            'args': args,
            'kwargs': kwargs
        }

        self.send(call, topic)

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

        with self._locks['threads']:
            for thread in self._threads:
                thread.stop()

        # Kill the thread that invoked the suicide method
        raise SystemExit

    def commit(self):
        for message in self._uncommitted_messages:
            self.stopover.commit(message, self.config['receiver_group'])
        self._uncommitted_messages.clear()

    def _set_uid(self):
        if 'CATENAE_DOCKER' in environ and bool(environ['CATENAE_DOCKER']):
            self._config['uid'] = environ['HOSTNAME']
        else:
            self._config['uid'] = utils.get_uid()

    def _transform(self):
        no_messages = True
        for input_stream in self.config['input_streams']:
            message = None
            try:
                message = self.stopover.get(
                    input_stream,
                    self.config['receiver_group'],
                    progress_without_commit=self.config[
                        'progress_without_commit'],
                )
            except Exception:
                pass

            if not message:
                continue
            no_messages = False

            if self.config['progress_without_commit']:
                self._uncommitted_messages.append(message)

            result = self.transform(message)
            output = result.value if isinstance(result,
                                                stopover.Response) else result

            if output:
                if self.config['default_output_stream']:
                    self.stopover.put(output,
                                      self.config['default_output_stream'])
                else:
                    raise ValueError('default stream is missing')

            if not self.config['progress_without_commit']:
                self.stopover.commit(message, self.config['receiver_group'])

        if no_messages:
            time.sleep(self.config['no_messages_sleep_interval'])

    def _rpc_notify_handler(self):
        no_messages = True
        for input_stream in self.config['rpc_topics']:
            message = self.stopover.get(input_stream, self.uid)
            if not message:
                continue

            call = message.value
            no_messages = False
            try:
                if 'context' not in call or call['context']['uid'] != self.uid:
                    self._rpc_notify(call)
            except (KeyError, TypeError):
                pass
            finally:
                self.stopover.commit(message, self.uid)

        if no_messages:
            time.sleep(self.config['no_messages_sleep_interval'])

    def _rpc_notify(self, call):
        method = call['method']

        if 'args' not in call:
            call['args'] = []

        if 'kwargs' not in call:
            call['kwargs'] = {}

        if 'context' not in call:
            call['context'] = {'uid': None, 'group': None}

        if method not in _rpc_enabled_methods:
            self.logger.log(f'method {method} cannot be called', level='error')
            return

        if 'method' not in call:
            self.logger.log(f'invalid RPC invocation: {call}', level='error')
            return

        try:
            context = call['context']
            args = [context] + call['args']
            kwargs = call['kwargs']
            self.logger.log(
                f"RPC invocation from {context['uid']} ({context['group']})",
                level='debug')
            with self._locks['rpc_lock']:
                getattr(self, call['method'])(*args, **kwargs)

        except Exception:
            self.logger.log(f'error when invoking {method} remotely',
                            level='exception')

    @suicide_on_error
    def _loop_task(
        self,
        target,
        args,
        kwargs,
        interval,
        wait
    ):
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
                continue_sleeping = (
                    utils.get_timestamp() - start_timestamp) < interval
                if not continue_sleeping:
                    break
                time.sleep(self.config['intervals']['loop_check_stop'])

    def _setup_signals_handler(self):
        for signal_name in ['SIGINT', 'SIGTERM', 'SIGQUIT']:
            signal.signal(getattr(signal, signal_name), self._signal_handler)

    def _signal_handler(self, sig, _):
        if sig == signal.SIGINT:
            self.suicide('SIGINT')
        elif sig == signal.SIGTERM:
            self.suicide('SIGTERM')
        elif sig == signal.SIGQUIT:
            self.suicide('SIGQUIT')
