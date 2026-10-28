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
from copy import deepcopy
from typing import List, Dict
import time
import catenae
from os import environ
import signal
import traceback
import argparse
from inspect import signature
from .logger import Logger
from .threading import Thread
from threading import Lock, current_thread
from .health import HealthCheck
from . import utils


def rpc(method):
    if method.__name__ != 'suicide_on_error':
        method._catenae_rpc_enabled = True
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
    DEFAULT_PICKLE_CLASSES = ()
    DEFAULT_CONFIG = {
        'no_messages_sleep_interval': .5,
        'intervals': {
            'loop_check_stop': 1,
            'loop_check_start': 1
        }
    }

    def __init__(
        self,
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
        binnakle_config: Dict = None,
        client_id: str = None,
        client_secret: str = None,
        allowed_pickle_classes=None,
        **ignored_kwargs,
    ):
        self.logger = Logger(
            self,
            level=log_level,
            config=binnakle_config,
        )

        if endpoint is not None:
            endpoints = [endpoint]
        if not endpoints:
            endpoints = []

        if input_stream is not None:
            input_streams = [input_stream]
        if not input_streams:
            input_streams = []

        self._uncommitted_messages = []

        self._config = deepcopy(Link.DEFAULT_CONFIG)
        self._set_uid()
        self._config.update(
            {
                'endpoints': endpoints,
                'input_streams': input_streams,
                'default_output_stream': default_output_stream,
                'receiver_group': (
                    receiver_group
                    if receiver_group else self.__class__.__name__
                ),
                'rpc_enabled': True if rpc_enabled else False,
                'enable_health': False if enable_health is False else True,
                'health_port': health_port if health_port else 2094,
                'rpc_topics': [
                    f'catenae_rpc_{self.__class__.__name__.lower()}',
                    'catenae_rpc_broadcast'
                ],
                'progress_without_commit': (
                    True if progress_without_commit else False
                ),
            }
        )
        if rpc_by_uid:
            self._config['rpc_topics'].append(f'catenae_rpc_{self.uid}')

        self._load_args()

        if ignored_kwargs:
            self.logger.log(
                f'the following kwargs were ignored: {sorted(ignored_kwargs)}'
            )

        if self._config['endpoints']:
            self.stopover = stopover.Stopover(
                endpoint=self._config['endpoints'][0],
                uid=self._config['uid'],
                client_id=client_id,
                client_secret=client_secret,
                allowed_pickle_classes=(
                    self.DEFAULT_PICKLE_CLASSES
                    if allowed_pickle_classes is None
                    else allowed_pickle_classes
                ),
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
        self._health_server = None
        self._setup_complete = False

    @property
    def env(self):
        return dict(environ)

    @property
    def args(self):
        return list(self._args)

    def _load_args(self):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            '-e',
            '--endpoint',
            '--endpoints',
            action='store',
            dest='endpoint',
            help='Message broker endpoint. E.g., http://localhost:5704',
            required=False,
        )

        parser.add_argument(
            '-i',
            '--input',
            action='store',
            dest='input_streams',
            help='Input streams. Several streams '
            + 'can be specified separated by commas',
            required=False,
        )

        parser.add_argument(
            '-o',
            '--default-output',
            action='store',
            dest='default_output_stream',
            help='Default output stream.',
            required=False,
        )

        parser.add_argument(
            '-g',
            '--receiver-group',
            action='store',
            dest='receiver_group',
            help='Receiver group.',
            required=False,
        )

        parser.add_argument(
            '-u',
            '--uid',
            action='store',
            dest='uid',
            help='Link\'s Unique ID.',
            required=False,
        )

        parser.add_argument(
            '-r',
            '--rpc',
            action='store_true',
            dest='rpc_enabled',
            help='Enable RPC.',
            required=False,
        )

        parsed_args = parser.parse_known_args()
        link_args = parsed_args[0]
        self._args = parsed_args[1]

        if link_args.endpoint:
            self._config['endpoints'] = link_args.endpoint.split(',')

        if link_args.input_streams:
            self._config['input_streams'] = link_args.input_streams.split(',')

        if link_args.default_output_stream:
            self._config['default_output_stream'
                         ] = link_args.default_output_stream

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

    def _health_liveness(self):
        with self._locks['threads']:
            return self._started and not self._stopped and all(thread.is_alive() for thread in self._threads)

    def health_readiness(self):
        checks = {'setup': self._setup_complete}
        if not self._setup_complete:
            return checks
        if self.stopover is not None:
            try:
                checks['stopover'] = self.stopover.health()
            except Exception:
                checks['stopover'] = False
        # Idle loops refresh this heartbeat too; absence of input is healthy.
        with self._locks['threads']:
            checks['loops'] = all(
                thread.is_alive() and (not getattr(thread, 'iteration_started', None)
                                      or time.monotonic() - thread.iteration_started < 300)
                for thread in self._threads)
        return checks

    @staticmethod
    def health_progress():
        """Long-running consumers call this after a completed unit of work."""
        thread = current_thread()
        if getattr(thread, 'iteration_started', None) is not None:
            thread.iteration_started = time.monotonic()

    def start(
        self,
        startup_text: str = None,
        setup_kwargs: Dict = None,
        embedded: bool = False,
        **_,
    ):
        with self._locks['start_stop']:
            if self._started or self._stopped:
                return
            self._started = True

        if not startup_text:
            print(catenae.text_logo)
        else:
            print(startup_text)

        self.logger.log(f'catenae  v{catenae.__version__}')
        self.logger.log(f'stopover v{stopover.__version__}')
        self.logger.log(self._config)

        if setup_kwargs is None:
            setup_kwargs = {}
        health_thread = None
        if self.config['enable_health']:
            self._health_server = HealthCheck(self.config['health_port'], self.health_readiness, self._health_liveness)
            health_thread = self.launch_thread(self._health_server.start)
        try:
            self.setup(**setup_kwargs)
        except BaseException:
            if self._health_server is not None:
                self._health_server.close()
            raise

        if self._stopped:
            return

        if hasattr(self, 'generator'):
            self.loop(self.generator)

        if self.stopover is not None:
            if self._config['rpc_enabled']:
                self.loop(self._rpc_notify_handler)

            if hasattr(self, 'transform'):
                self.loop(self._transform)

            self.loop(
                self.stopover.knock,
                kwargs={'receiver_group': self.config['receiver_group']},
                interval=5
            )

        self._setup_complete = True

        if not embedded:
            self._setup_signals_handler()
            for thread in self._threads:
                thread.join()
            if health_thread is not None:
                health_thread.join()

    def stop(self):
        pass

    def send(self, message, stream: str = None):
        stream = self.config['default_output_stream'
                             ] if stream is None else stream
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
        if interval < 0:
            raise ValueError('interval must be nonnegative')
        loop_task_kwargs = {
            'target': target,
            'args': args,
            'kwargs': kwargs,
            'interval': interval,
            'wait': wait,
        }
        thread = self.launch_thread(
            self._loop_task, kwargs=loop_task_kwargs, safe_stop=safe_stop
        )
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
            'kwargs': kwargs,
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

        if self._health_server is not None:
            self._health_server.close()

        if message is None:
            message = '[SUICIDE]'
        else:
            message = f'[SUICIDE] {message}'

        if exception:
            self.logger.log(message, level='exception')
        else:
            self.logger.log(message, level='warn')

        with self._locks['threads']:
            for thread in self._threads:
                thread.stop()

        # Kill the thread that invoked the suicide method
        raise SystemExit

    def commit(self):
        while self._uncommitted_messages:
            message = self._uncommitted_messages[0]
            self.stopover.commit(message, self.config['receiver_group'])
            self._uncommitted_messages.pop(0)

    def _set_uid(self):
        if environ.get('CATENAE_DOCKER', '').lower() in ('1', 'true', 'yes'):
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
                    progress_without_commit=self.
                    config['progress_without_commit'],
                )
            except Exception:
                pass

            if not message:
                continue
            no_messages = False

            if self.config['progress_without_commit']:
                self._uncommitted_messages.append(message)

            result = self.transform(message)
            output = result.value if isinstance(
                result, stopover.Response
            ) else result

            if output is not None:
                if self.config['default_output_stream']:
                    self.stopover.put(
                        output, self.config['default_output_stream']
                    )
                else:
                    raise ValueError('default stream is missing')

            if not self.config['progress_without_commit']:
                self.stopover.commit(message, self.config['receiver_group'])

        if no_messages:
            time.sleep(self.config['no_messages_sleep_interval'])

    def _rpc_notify_handler(self):
        no_messages = True
        retry = False
        for input_stream in self.config['rpc_topics']:
            message = None
            try:
                message = self.stopover.get(input_stream, self.uid)
            except Exception:
                pass

            if not message:
                continue

            call = message.value
            no_messages = False
            context = call.get('context') if isinstance(call, dict) else None
            own_message = isinstance(context, dict) and context.get('uid') == self.uid
            if not own_message and self._rpc_notify(call) is False:
                retry = True
                continue
            try:
                self.stopover.commit(message, self.uid)
            except Exception:
                retry = True
                self.logger.log('RPC acknowledgement failed; retaining message', level='exception')

        if no_messages or retry:
            time.sleep(self.config['no_messages_sleep_interval'])

    def _rpc_notify(self, call):
        method = call.get('method') if isinstance(call, dict) else None

        if not isinstance(method, str):
            self.logger.log('invalid RPC invocation', level='error')
            return True

        allowed = getattr(type(self), method, None)
        if not getattr(allowed, '_catenae_rpc_enabled', False):
            self.logger.log(f'method {method} cannot be called', level='error')
            return True

        context = call.get('context', {'uid': None, 'group': None})
        args, kwargs = call.get('args', []), call.get('kwargs', {})
        if not isinstance(context, dict) or not isinstance(args, list) or not isinstance(kwargs, dict):
            self.logger.log('invalid RPC arguments', level='error')
            return True
        target = getattr(self, method)
        try:
            signature(target).bind(context, *args, **kwargs)
        except TypeError:
            self.logger.log(f'invalid arguments for RPC {method}', level='error')
            return True

        try:
            self.logger.log(
                f"RPC invocation from {context.get('uid')} ({context.get('group')})",
                level='debug'
            )
            with self._locks['rpc_lock']:
                target(context, *args, **kwargs)

        except Exception:
            self.logger.log(
                f'error when invoking {method} remotely', level='exception'
            )
            return False
        return True

    @suicide_on_error
    def _loop_task(self, target, args, kwargs, interval, wait):
        thread = current_thread()
        if wait and thread.wait(interval):
            return

        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        while not thread.will_stop:
            start_timestamp = time.monotonic()
            thread.iteration_started = start_timestamp
            target(*args, **kwargs)
            thread.iteration_started = None
            remaining = interval - (time.monotonic() - start_timestamp)
            if remaining > 0:
                thread.wait(remaining)

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
