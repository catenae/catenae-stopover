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

# TODO -> ELIMINAR SYNC MODE (SIEMPRE SYNC)

import catenae
import math
from threading import Lock, current_thread
import pickle
import time
import argparse
from os import environ
import signal
import json
from . import utils
from . import errors
from .electron import Electron
from .callback import Callback
from .logger import Logger
from .custom_queue import ThreadingQueue
from .custom_threading import Thread, ThreadPool
from .custom_multiprocessing import Process
from .structures import CircularOrderedSet
from stopover import Sender, Receiver, Message

_rpc_enabled_methods = set()


def rpc(method):
    """ RPC decorator """
    if method.__name__ not in _rpc_enabled_methods and method.__name__ != '_try_except':
        _rpc_enabled_methods.add(method.__name__)
    return method


def suicide_on_error(method):
    """ Try-Except decorator for Link instance methods """

    def _try_except(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception:
            import traceback
            traceback.print_exc()
            self.suicide()

    return _try_except


class Link:

    RECEIVER_POLL_TIMEOUT = 0.5
    QUEUE_GET_TIMEOUT = 0.5
    INSTANCE_TIMEOUT = 3
    SUICIDE_TIMEOUT = 10

    WAIT_INTERVAL = 0.5
    RPC_REQUEST_MONITOR_INTERVAL = 0.5
    CHECK_INSTANCES_INTERVAL = 5
    REPORT_EXISTENCE_INTERVAL = 60
    COMMIT_MESSAGE_INTERVAL = 5
    LOOP_CHECK_STOP_INTERVAL = 1

    MAX_COMMIT_ATTEMPTS = 5

    def __init__(self,
                 log_level='INFO',
                 input_mode='parity',
                 exp_window_size=900,
                 synchronous=False,
                 sequential=False,
                 uid_receiver=False,
                 num_rpc_threads=1,
                 num_main_threads=1,
                 input_streams=None,
                 output_streams=None,
                 stopover_endpoint=None,
                 receiver=None,
                 receiver_timeout=300):

        # Preserve the id if the container restarts
        if 'CATENAE_DOCKER' in environ \
        and bool(environ['CATENAE_DOCKER']):
            self._uid = environ['HOSTNAME']
        else:
            self._uid = utils.get_uid()
        self._class_id = self.__class__.__name__.lower()

        self._set_log_level(log_level)
        self.logger = Logger(self, self._log_level)
        self.logger.log(f'log level: {self._log_level}')

        self.logger.log(f'RPC-enabled methods: {_rpc_enabled_methods}')

        self._started = False
        self._stopped = False
        self._input_streams_lock = Lock()
        self._rpc_lock = Lock()
        self._start_stop_lock = Lock()
        self._instances_lock = Lock()

        # RPC streams
        self._rpc_instance_stream = f'catenae_rpc_{self._uid}'
        self._rpc_group_stream = f'catenae_rpc_{self._class_id}'
        self._rpc_broadcast_stream = 'catenae_rpc_broadcast'
        self._rpc_streams = [self._rpc_instance_stream, self._rpc_group_stream, self._rpc_broadcast_stream]
        self._known_message_ids = CircularOrderedSet(50)

        self._load_args()
        self._set_execution_opts(input_mode, exp_window_size, synchronous, sequential, num_rpc_threads,
                                 num_main_threads, input_streams, output_streams, stopover_endpoint, receiver_timeout)
        self._set_receiver(receiver, uid_receiver)

        self._input_messages = ThreadingQueue()
        self._output_messages = ThreadingQueue()
        self._changed_input_streams = False

        self._instances = {'by_uid': dict(), 'by_group': dict()}
        self._known_instances = dict()
        self._safe_stop_threads = list()

    @suicide_on_error
    def _set_execution_opts(self, input_mode, exp_window_size, synchronous, sequential, num_rpc_threads,
                            num_main_threads, input_streams, output_streams, stopover_endpoint, receiver_timeout):

        if not hasattr(self, '_input_mode'):
            self._input_mode = input_mode
        self.logger.log(f'input_mode: {self._input_mode}')

        if not hasattr(self, '_exp_window_size'):
            self._exp_window_size = exp_window_size
            if self._input_mode == 'exp':
                self.logger.log(f'exp_window_size: {self._exp_window_size}')

        if hasattr(self, '_synchronous'):
            synchronous = self._synchronous

        if hasattr(self, '_sequential'):
            sequential = self._sequential

        if synchronous:
            self._synchronous = True
            self._sequential = True
        else:
            self._synchronous = False
            self._sequential = sequential

        if self._synchronous:
            self.logger.log('execution mode: sync + seq')
        else:
            if self._sequential:
                self.logger.log('execution mode: async + seq')
            else:
                self.logger.log('execution mode: async')

        if hasattr(self, '_num_rpc_threads'):
            num_rpc_threads = self._num_rpc_threads

        if hasattr(self, '_num_main_threads'):
            num_main_threads = self._num_main_threads

        if synchronous or sequential:
            self._num_main_threads = 1
            self._num_rpc_threads = 1
        else:
            self._num_rpc_threads = num_rpc_threads
            self._num_main_threads = num_main_threads
        self.logger.log(f'num_rpc_threads: {self._num_rpc_threads}')
        self.logger.log(f'num_main_threads: {self._num_main_threads}')

        if not self._input_streams:
            self._input_streams = input_streams
        self.logger.log(f'input_streams: {self._input_streams}')

        if not self._output_streams:
            self._output_streams = output_streams
        self.logger.log(f'output_streams: {self._output_streams}')

        if not self._stopover_endpoint:
            self._stopover_endpoint = stopover_endpoint
        self.logger.log(f'stopover_endpoint: {self._stopover_endpoint}')

        if not hasattr(self, '_receiver_timeout'):
            self._receiver_timeout = receiver_timeout
        self.logger.log(f'receiver_timeout: {self._receiver_timeout}')
        self._receiver_timeout = self._receiver_timeout * 1000

    @property
    def input_streams(self):
        return list(self._input_streams)

    @property
    def output_streams(self):
        return list(self._output_streams)

    @property
    def receiver(self):
        return self._receiver

    @property
    def args(self):
        return list(self._args)

    @property
    def uid(self):
        return self._uid

    @suicide_on_error
    def _loop_task(self, target, args=None, kwargs=None, interval=0, wait=False, level='debug'):
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
                self.logger.log(f'new loop iteration ({target.__name__})', level=level)
                start_timestamp = utils.get_timestamp()

                target(*args, **kwargs)

                while not current_thread().will_stop:
                    continue_sleeping = (utils.get_timestamp() - start_timestamp) < interval
                    if not continue_sleeping:
                        break
                    time.sleep(Link.LOOP_CHECK_STOP_INTERVAL)

            except Exception:
                self.logger.log(f'exception raised when executing the loop: {target.__name__}', level='exception')

    @suicide_on_error
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

    @suicide_on_error
    def _delete_from_known_instances(self, uid, group):
        with self._instances_lock:
            if uid in self._instances['by_uid']:
                del self._instances['by_uid'][uid]
                self._instances['by_group'][group].remove(uid)

    @suicide_on_error
    def _add_to_known_instances(self, uid, group, host, port, scheme):
        with self._instances_lock:
            self._instances['by_uid'][uid] = {'host': host, 'port': port, 'scheme': scheme, 'group': group}
            if not group in self._instances['by_group']:
                self._instances['by_group'][group] = list()

            if uid not in self._instances['by_group'][group]:
                self._instances['by_group'][group].append(uid)

    @suicide_on_error
    def _is_method_rpc_enabled(self, method):
        if method in _rpc_enabled_methods:
            return True
        return False

    def _rpc_call(self, method, kwargs=None):
        if not self._is_method_rpc_enabled(method):
            self.logger.log(f'method {method} cannot be called', level='error')
            raise errors.MethodNotFoundError

        if kwargs is None:
            kwargs = dict()

        try:
            output = getattr(self, method)(**kwargs)
            return output

        except TypeError:
            raise errors.InvalidParamsError

        except Exception:
            self.logger.log(level='exception')
            raise errors.InternalError


    @suicide_on_error
    def rpc_notify(self, method=None, args=None, kwargs=None, to='broadcast'):
        """ 
        Send a stopover message which will be interpreted as a RPC call by the receiver module.
        """
        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        if not method:
            raise ValueError
        stream = f'catenae_rpc_{to.lower()}'
        electron = Electron(value={
            'method': method,
            'context': {
                'group': self._receiver_group,
                'uid': self._uid
            },
            'args': args,
            'kwargs': kwargs
        },
                            stream=stream)
        self.send(electron, synchronous=True)

    @suicide_on_error
    def _rpc_notify(self, electron, commit_callback):
        method = electron.value['method']
        if not self._is_method_rpc_enabled(method):
            self.logger.log(f'method {method} cannot be called', level='error')
            return

        if not 'method' in electron.value:
            self.logger.log(f'invalid RPC invocation: {electron.value}', level='error')
            return

        try:
            context = electron.value['context']
            args = [context] + electron.value['args']
            kwargs = electron.value['kwargs']
            self.logger.log(f"RPC invocation from {context['uid']} ({context['group']})", level='debug')
            with self._rpc_lock:
                getattr(self, electron.value['method'])(*args, **kwargs)

        except Exception:
            self.logger.log(f'error when invoking {method} remotely', level='exception')

        commit_callback.execute()

    def suicide(self, message=None, exception=False):
        with self._start_stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self.finish()

        if message is None:
            message = '[SUICIDE]'
        else:
            message = f'[SUICIDE] {message}'

        if exception:
            self.logger.log(message, level='exception')
        else:
            self.logger.log(message, level='warn')

        while not self._started:
            time.sleep(Link.WAIT_INTERVAL)

        self.logger.log('stopping threads...')

        for thread in self._safe_stop_threads:
            thread.stop()

        if self._stopover_endpoint:
            if hasattr(self, '_sender_thread'):
                self._sender_thread.stop()
            if hasattr(self, '_input_handler_thread'):
                self._input_handler_thread.stop()
            if hasattr(self, '_receiver_rpc_thread'):
                self._receiver_rpc_thread.stop()
            if hasattr(self, '_receiver_main_thread'):
                self._receiver_main_thread.stop()

            if hasattr(self, '_transform_rpc_executor'):
                for thread in self._transform_rpc_executor.threads:
                    thread.stop()
            if hasattr(self, '_transform_main_executor'):
                for thread in self._transform_main_executor.threads:
                    thread.stop()

        # Kill the thread that invoked the suicide method
        raise SystemExit

    @suicide_on_error
    def loop(self, target, args=None, kwargs=None, interval=0, wait=False, level='debug', safe_stop=True):
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
            self._safe_stop_threads.append(thread)
        thread.daemon = True
        thread.start()
        return thread

    @suicide_on_error
    def launch_thread(self, target, args=None, kwargs=None, safe_stop=False):
        thread = Thread(target, args=args, kwargs=kwargs)
        if safe_stop:
            self._safe_stop_threads.append(thread)
        thread.daemon = True
        thread.start()
        return thread

    @suicide_on_error
    def launch_process(self, target, args=None, kwargs=None):
        process = Process(target, args=args, kwargs=kwargs)
        process.start()
        return process

    @suicide_on_error
    def _stopover_sender(self):
        while not current_thread().will_stop:
            try:
                electron = self._output_messages.get(timeout=Link.QUEUE_GET_TIMEOUT, block=False)
            except errors.EmptyError:
                continue

            self._produce(electron)

    @suicide_on_error
    def _produce(self, electron, synchronous=None):
        # All the queue items of the _output_messages must be individual
        # instances of Electron
        if not isinstance(electron, Electron):
            raise ValueError

        # The key is enconded for its use as partition key
        partition_key = None
        if electron.key:
            if isinstance(electron.key, str):
                partition_key = electron.key.encode('utf-8')
            else:
                partition_key = pickle.dumps(electron.key, protocol=pickle.HIGHEST_PROTOCOL)
        # Same partition key for the current instance if sequential mode
        # is enabled so receiver can get messages in order
        elif self._sequential:
            partition_key = b'0'

        # If the destiny stream is not specified, the first is used
        if not electron.stream:
            if not self._output_streams:
                self.suicide('electron / default output stream unset')
            electron.stream = self._output_streams[0]

        # Electrons are serialized
        if electron.unpack_if_string and isinstance(electron.value, str):
            serialized_electron = electron.value
        else:
            serialized_electron = pickle.dumps(electron.get_sendable(), protocol=pickle.HIGHEST_PROTOCOL)

        if synchronous is None:
            synchronous = self._synchronous

        if synchronous:
            sender = self._sync_sender
        else:
            sender = self._async_sender

        try:
            # If partition_key == None, the partition.assignment.strategy
            # is used to distribute the messages
            sender.produce(stream=electron.stream, key=partition_key, value=serialized_electron)

            if synchronous:
                # Wait for all messages in the sender queue to be delivered.
                sender.flush()
            else:
                sender.poll(0)

            self.logger.log('electron produced', level='debug')

            for callback in electron.callbacks:
                callback.execute()

        except Exception:
            self.suicide('stopover sender error', exception=True)

    @suicide_on_error
    def _transform(self, electron, commit_callback):
        try:
            with self._rpc_lock:
                transform_result = self.transform(electron)
            self.logger.log('electron transformed', level='debug')
        except Exception:
            self.suicide('exception during the execution of "transform"', exception=True)

        transform_callback = Callback()

        if not isinstance(transform_result, tuple):
            electrons = transform_result
        else:
            electrons = transform_result[0]
            if len(transform_result) > 1:
                transform_callback.target = transform_result[1]
                if len(transform_result) > 2:
                    if isinstance(transform_result[2], dict):
                        transform_callback.kwargs = transform_result[2]
                    else:
                        transform_callback.args = transform_result[2]

        # Transform returns None
        if electrons is None:
            if transform_callback:
                transform_callback.execute()
            if commit_callback:
                commit_callback.execute()
            return

        # Already a list
        if isinstance(electrons, list):
            real_electrons = []
            for electron in electrons:
                if isinstance(electron, Electron):
                    real_electrons.append(electron)
                else:
                    real_electrons.append(Electron(value=electron, unpack_if_string=True))
            electrons = real_electrons
        else:  # If there is only one item, convert it to a list
            if isinstance(electrons, Electron):
                electrons = [electrons]
            else:
                electrons = [Electron(value=electrons)]

        # Execute transform_callback only for the last electron
        if transform_callback:
            electrons[-1].callbacks.append(transform_callback)
        if commit_callback:
            electrons[-1].callbacks.append(commit_callback)

        for electron in electrons:
            if self._synchronous:
                self._produce(electron)
            else:
                self._output_messages.put(electron)

    @suicide_on_error
    def _input_handler(self):
        while not current_thread().will_stop:
            self.logger.log('waiting for a new electron to transform...', level='debug')

            try:
                queue_item = self._input_messages.get(timeout=Link.QUEUE_GET_TIMEOUT, block=False)
            except errors.EmptyError:
                continue

            commit_callback = Callback(mode=Callback.COMMIT_STOPOVER_MESSAGE)

            # Tuple
            if isinstance(queue_item, tuple):
                commit_callback.target = queue_item[1]
                if len(queue_item) > 2:
                    if isinstance(queue_item[2], list):
                        commit_callback.args = queue_item[2]
                    elif isinstance(queue_item[2], dict):
                        commit_callback.kwargs = queue_item[2]
                message = queue_item[0]
            else:
                message = queue_item

            self.logger.log('electron received', level='debug')

            try:
                electron = Electron(value=message)
            except Exception:
                electron = pickle.loads(message.value())

            # Add the message timestamp
            # message_timestamp = message.timestamp()[1]
            # electron.timestamp = message_timestamp

            # Clean the previous stream
            # electron.previous_stream = message.stream()
            # electron.stream = None

            # The destiny stream will be overwritten if desired in the
            # transform method (default, first output stream)
            if electron.previous_stream in self._rpc_streams:
                # Avoid own RPC calls
                if electron.value['context']['uid'] == self._uid:
                    commit_callback.execute()
                else:
                    self._transform_rpc_executor.submit(self._rpc_notify, [electron, commit_callback])
            else:
                self._transform_main_executor.submit(self._transform, [electron, commit_callback])

    @suicide_on_error
    def _commit_stopover_message(self, receiver, message):
        commited = False
        attempts = 1
        self.logger.log(f'trying to commit a message', level='debug')
        while not commited:
            if attempts > Link.MAX_COMMIT_ATTEMPTS:
                self.suicide('the maximum number of attempts to commit the message has been reached', exception=True)

            if attempts > 2:
                self.logger.log(f'trying to commit a message ({attempts}/{Link.MAX_COMMIT_ATTEMPTS})', level='warn')

            try:
                receiver.commit(message=message)
                commited = True
            except Exception:
                self.logger.log('could not commit a message', level='exception')
                time.sleep(Link.COMMIT_MESSAGE_INTERVAL)
            finally:
                attempts += 1

        self.logger.log(f'message commited', level='debug')

    @suicide_on_error
    def _stopover_rpc_receiver(self):
        return

        properties = dict(self._stopover_receiver_synchronous_properties)
        receiver = receiver(properties)
        self.logger.log(f'[RPC] receiver properties: {utils.dump_dict_pretty(properties)}', level='debug')
        subscription = list(self._rpc_streams)
        receiver.subscribe(subscription)
        self.logger.log(f'[RPC] listening on: {subscription}')

        while not current_thread().will_stop:
            message = receiver.poll(Link.RECEIVER_POLL_TIMEOUT)

            if not message or (not message.key() and not message.value()):
                if not self._break_receiver_loop(subscription):
                    continue
                # New stream / restart if there are more streams or
                # there aren't assigned partitions
                break

            # Commit when the transformation is commited
            self._input_messages.put((message, self._commit_stopover_message, [message]))

    @suicide_on_error
    def _stopover_main_receiver(self):
        while not current_thread().will_stop:
            if not self._input_streams:
                self.logger.log('No input streams, waiting...', level='info')
                time.sleep(Link.WAIT_INTERVAL)
                continue

            with self._input_streams_lock:
                streams = list(self._input_streams)

            receivers = []
            for stream in streams:
                receiver = Receiver(endpoint=self._stopover_endpoint,
                                    stream=stream,
                                    receiver_group=self._receiver_group,
                                    instance=self._uid)
                receivers.append(receiver)

            for receiver in receivers:
                with self._input_streams_lock:
                    if self._changed_input_streams:
                        self._changed_input_streams = False
                        break

                self.logger.log(f'[MAIN] listening on: {streams}')

                while not current_thread().will_stop:
                    with self._input_streams_lock:

                        # Subscribe to the streams again if input streams have changed
                        if self._changed_input_streams:
                            # _changed_input_streams is set to False in the
                            # outer loop so both loops are broken
                            break


                    ##################
                    # TODO NO LLEGA MENSAJE A TRANSFORM
                    message = receiver.get()
                    # print(message.value)
                    time.sleep(1)
                    ####################################

                    if not message:
                        continue

                    self._input_messages.put((message, self._commit_stopover_message, [receiver, message]))

    @suicide_on_error
    def setup(self):
        pass

    @suicide_on_error
    def transform(self, _):
        for thread in self._transform_main_executor.threads:
            thread.stop()

    @suicide_on_error
    def finish(self):
        pass

    @suicide_on_error
    def send(self,
             output_content,
             stream=None,
             callback=None,
             callback_args=None,
             callback_kwargs=None,
             synchronous=None):
        if isinstance(output_content, Electron):
            if stream:
                output_content.stream = stream
            electron = output_content.copy()
        elif not isinstance(output_content, list):
            electron = Electron(value=output_content, stream=stream, unpack_if_string=True)
        else:
            for i, item in enumerate(output_content):
                # Last item includes the callback
                if i == len(output_content) - 1:
                    self.send(item, stream=stream, callback=callback, synchronous=synchronous)
                else:
                    self.send(item, stream=stream, synchronous=synchronous)
            return

        if callback is not None:
            electron.callbacks.append(Callback(callback, callback_args, callback_kwargs))

        if synchronous is None:
            synchronous = self._synchronous

        # Electrons can be sent asynchronously / synchronously individually
        if synchronous:
            self._produce(electron, synchronous=synchronous)
        else:
            self._output_messages.put(electron)

    def generator(self):
        self.logger.log('Generator method undefined. Disabled.', level='debug')
        # Kill the generator thread
        raise SystemExit

    @suicide_on_error
    def _thread_target(self, target, args=None, kwargs=None):
        if args is None:
            args = []

        if not isinstance(args, list):
            args = [args]

        if kwargs is None:
            kwargs = {}

        target(*args, **kwargs)

    @suicide_on_error
    def add_input_stream(self, input_stream):
        with self._input_streams_lock:
            if input_stream not in self._input_streams:
                self._input_streams.append(input_stream)
                self._changed_input_streams = True
                self.logger.log(f'added input {input_stream}')

    @suicide_on_error
    def remove_input_stream(self, input_stream):
        with self._input_streams_lock:
            if input_stream in self._input_streams:
                self._input_streams.remove(input_stream)
                self._changed_input_streams = True
                self.logger.log(f'removed input {input_stream}')

    def start(self, embedded=False, startup_text=None, setup_kwargs=None):
        if setup_kwargs is None:
            setup_kwargs = {}

        with self._start_stop_lock:
            if self._started:
                return

        if startup_text is None:
            startup_text = catenae.text_logo
        self.logger.log(startup_text)
        self.logger.log(f'Catenae v{catenae.__version__} {catenae.__version_name__}')

        try:
            self.logger.log(f'link {self._uid} is starting...')
            self.setup(**setup_kwargs)
            self._launch_tasks()
            self._started = True

        except Exception:
            try:
                self.suicide('Exception during the execution of setup()', exception=True)
            except SystemExit:
                pass
        finally:
            self.logger.log(f'link {self._uid} is running')
            if embedded:
                Thread(self._join_tasks).start()
            else:
                self._setup_signals_handler()
                while not self._stopped:
                    time.sleep(Link.WAIT_INTERVAL)
                self._join_tasks()

    def _join_tasks(self):
        for thread in self._safe_stop_threads:
            self._join_if_not_current_thread(thread)

        if self._stopover_endpoint:
            if hasattr(self, '_sender_thread'):
                self._sender_thread.join(Link.SUICIDE_TIMEOUT)

            if hasattr(self, '_input_handler_thread'):
                self._input_handler_thread.join(Link.SUICIDE_TIMEOUT)

            if hasattr(self, '_receiver_rpc_thread'):
                self._receiver_rpc_thread.join(Link.SUICIDE_TIMEOUT)

            if hasattr(self, '_receiver_main_thread'):
                self._receiver_main_thread.join(Link.SUICIDE_TIMEOUT)

            if hasattr(self, '_transform_rpc_executor'):
                for thread in self._transform_rpc_executor.threads:
                    self._join_if_not_current_thread(thread)

            if hasattr(self, '_transform_main_executor'):
                for thread in self._transform_main_executor.threads:
                    self._join_if_not_current_thread(thread)

        self.logger.log(f'link {self._uid} stopped')

    @suicide_on_error
    def _join_if_not_current_thread(self, thread):
        if thread is not current_thread():
            thread.join(Link.SUICIDE_TIMEOUT)

    @suicide_on_error
    def _launch_tasks(self):

        if self._stopover_endpoint:
            # stopover RPC receiver
            receiver_kwargs = {'target': self._stopover_rpc_receiver}
            self._receiver_rpc_thread = Thread(self._thread_target, kwargs=receiver_kwargs)
            self._receiver_rpc_thread.start()

            # stopover main receiver
            self._receiver_main_thread = Thread(self._thread_target, kwargs={'target': self._stopover_main_receiver})
            self._receiver_main_thread.start()

            # stopover sender
            sender_kwargs = {'target': self._stopover_sender}
            self._sender_thread = Thread(self._thread_target, kwargs=sender_kwargs)
            self._sender_thread.start()

            # Transform
            self._transform_rpc_executor = ThreadPool(self, self._num_rpc_threads)
            self._transform_main_executor = ThreadPool(self, self._num_main_threads)
            transform_kwargs = {'target': self._input_handler}
            self._input_handler_thread = Thread(self._thread_target, kwargs=transform_kwargs)
            self._input_handler_thread.start()

        # Generator
        self.loop(self.generator, interval=0, safe_stop=True)

    def _set_log_level(self, log_level):
        if not hasattr(self, '_log_level'):
            self._log_level = log_level.upper()

    @suicide_on_error
    def _set_receiver(self, receiver, uid_receiver):
        if hasattr(self, 'receiver'):
            receiver = self._receiver_group
        if hasattr(self, 'uid_receiver'):
            uid_receiver = self._uid_receiver_group

        if uid_receiver:
            self._receiver_group = f'catenae_{self._uid}'
        elif receiver:
            self._receiver_group = receiver
        else:
            self._receiver_group = f'catenae_{self._class_id}'

        self.logger.log(f'receiver: {self._receiver_group}')

    @suicide_on_error
    def _on_time(self, start_time, assigned_time):
        return (utils.get_timestamp_ms() - start_time) < 1000 * assigned_time

    @suicide_on_error
    def _parse_stopover_args(self, parser):
        parser.add_argument('-i',
                            '--input',
                            action="store",
                            dest="input_streams",
                            help='Stopover input streams. Several streams ' + 'can be specified separated by commas',
                            required=False)
        parser.add_argument('-o',
                            '--output',
                            action="store",
                            dest="output_streams",
                            help='Stopover output streams. Several streams ' + 'can be specified separated by commas',
                            required=False)
        parser.add_argument('-s',
                            '--stopover',
                            action="store",
                            dest="stopover_endpoint",
                            help='Stopover server. \
                            E.g., "localhost:9092"',
                            required=False)
        parser.add_argument('-r',
                            '--receiver',
                            action="store",
                            dest="receiver",
                            help='Stopover receiver name.',
                            required=False)
        parser.add_argument('--receiver-timeout',
                            action="store",
                            dest="receiver_timeout",
                            help='Stopover receiver timeout in seconds.',
                            required=False)

    @suicide_on_error
    def _set_stopover_properties_from_args(self, args):
        if args.input_streams:
            self._input_streams = args.input_streams.split(',')
        else:
            self._input_streams = []

        if args.output_streams:
            self._output_streams = args.output_streams.split(',')
        else:
            self._output_streams = []

        self._stopover_endpoint = args.stopover_endpoint

        if args.receiver:
            self._receiver_group = args.receiver

        if args.receiver_timeout:
            self._receiver_timeout = args.receiver_timeout

    @suicide_on_error
    def _parse_catenae_args(self, parser):
        parser.add_argument('--log-level',
                            action="store",
                            dest="log_level",
                            help='Catenae log level [debug|info|warning|error|critical].',
                            required=False)
        parser.add_argument('--input-mode',
                            action="store",
                            dest="input_mode",
                            help='Link input mode [parity|exp].',
                            required=False)
        parser.add_argument('--exp-window-size',
                            action="store",
                            dest="exp_window_size",
                            help='Consumption window size in seconds for exp mode.',
                            required=False)
        parser.add_argument('--sync',
                            action="store_true",
                            dest="synchronous",
                            help='Synchronous mode is enabled.',
                            required=False)
        parser.add_argument('--seq',
                            action="store_true",
                            dest="sequential",
                            help='Sequential mode is enabled.',
                            required=False)
        parser.add_argument('--random-receiver-group',
                            action="store_true",
                            dest="uid_receiver",
                            help='Synchronous mode is disabled.',
                            required=False)
        parser.add_argument('--rpc-threads',
                            action="store",
                            dest="num_rpc_threads",
                            help='Number of RPC threads.',
                            required=False)
        parser.add_argument('--main-threads',
                            action="store",
                            dest="num_main_threads",
                            help='Number of main threads.',
                            required=False)

    @suicide_on_error
    def _set_catenae_properties_from_args(self, args):
        if args.log_level:
            self._log_level = args.log_level
        if args.input_mode:
            self._input_mode = args.input_mode
        if args.exp_window_size:
            self._exp_window_size = args.exp_window_size
        if args.synchronous:
            self._synchronous = True
        if args.sequential:
            self._sequential = True
        if args.uid_receiver:
            self._uid_receiver_group = True
        if args.num_rpc_threads:
            self._num_rpc_threads = args.num_rpc_threads
        if args.num_main_threads:
            self._num_main_threads = args.num_main_threads

    @suicide_on_error
    def _load_args(self):
        parser = argparse.ArgumentParser()

        self._parse_catenae_args(parser)
        self._parse_stopover_args(parser)

        parsed_args = parser.parse_known_args()
        link_args = parsed_args[0]
        self._args = parsed_args[1]

        self._set_catenae_properties_from_args(link_args)
        self._set_stopover_properties_from_args(link_args)
