"""Regression checks for Catenae's local scheduling and RPC contracts."""

import os
from threading import Event, Thread
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from catenae import Link, rpc
from catenae.errors import EmptyError, TimeoutError
from catenae.queue import ThreadingQueue
from catenae.threading import ThreadPool, should_stop
from stopover import Response


class CoreTests(unittest.TestCase):
    def test_rpc_permission_belongs_to_the_method_not_its_name(self):
        class Allowed(Link):
            @rpc
            def trigger(self, context, value):
                self.received = (context, value)

        class Unmarked(Link):
            def trigger(self, context, value):
                self.received = (context, value)

        call = {'method': 'trigger', 'args': [7],
                'context': {'uid': 'sender', 'group': 'workers'}}
        allowed = Allowed(enable_health=False)
        unmarked = Unmarked(enable_health=False)
        allowed._rpc_notify(dict(call))
        unmarked._rpc_notify(dict(call))
        unmarked._rpc_notify({})
        self.assertEqual(allowed.received, (call['context'], 7))
        self.assertFalse(hasattr(unmarked, 'received'))

    def test_queue_full_is_reported_without_losing_the_first_item(self):
        queue = ThreadingQueue(size=1)
        queue.put('first')
        with self.assertRaises(TimeoutError):
            queue.put('second', timeout=0)
        self.assertEqual(queue.get(block=False), 'first')
        with self.assertRaises(EmptyError):
            queue.get(block=False, timeout=1)
        queue.put('third')
        with self.assertRaises(TimeoutError):
            queue.put('fourth', block=False)
        self.assertEqual(queue.get(), 'third')

    def test_circular_queue_keeps_the_newest_items(self):
        queue = ThreadingQueue(size=2, circular=True)
        for item in ('first', 'second', 'third'):
            queue.put(item)
        self.assertEqual(queue.get(), 'second')
        self.assertEqual(queue.get(), 'third')

    def test_thread_pool_expands_tuple_arguments(self):
        done = Event()
        pool = ThreadPool(SimpleNamespace(logger=SimpleNamespace(log=Mock())))
        try:
            pool.submit(lambda left, right: done.set() if left + right == 3
                        else None, args=(1, 2))
            self.assertTrue(done.wait(2))
        finally:
            pool.stop()
        self.assertFalse(any(thread.is_alive() for thread in pool.threads))

    def test_thread_pool_requires_workers_and_stop_check_is_safe(self):
        self.assertFalse(should_stop())
        with self.assertRaises(ValueError):
            ThreadPool(SimpleNamespace(), num_threads=0)

    def test_subsecond_loop_uses_elapsed_time_not_rounded_clock(self):
        link = Link(enable_health=False)
        calls, done = [], Event()

        def target():
            calls.append(time.monotonic())
            if len(calls) == 2:
                done.set()

        thread = link.loop(target, interval=0.05)
        try:
            self.assertTrue(done.wait(0.5))
        finally:
            thread.stop()
            thread.join(2)

    def test_waiting_loop_stops_before_its_first_iteration(self):
        link = Link(enable_health=False)
        with self.assertRaises(ValueError):
            link.loop(Mock(), interval=-1, wait=True)
        target = Mock()
        thread = link.loop(target, interval=10, wait=True)
        thread.stop()
        thread.join(0.5)
        self.assertFalse(thread.is_alive())
        target.assert_not_called()

    def test_long_interval_does_not_delay_stop(self):
        link = Link(enable_health=False)
        entered = Event()
        thread = link.loop(entered.set, interval=10)
        try:
            self.assertTrue(entered.wait(1))
        finally:
            thread.stop()
            thread.join(0.5)
        self.assertFalse(thread.is_alive())

    def test_start_tracks_each_loop_once(self):
        class Worker(Link):
            def generator(self):
                time.sleep(0.01)

        worker = Worker(enable_health=False)
        try:
            worker.start(embedded=True, startup_text='local test')
            self.assertEqual(len(worker._threads), 1)
        finally:
            for thread in set(worker._threads):
                thread.stop()
                thread.join(2)

    def test_concurrent_start_runs_setup_once(self):
        entered, release = Event(), Event()

        class Worker(Link):
            calls = 0

            def setup(self):
                self.calls += 1
                entered.set()
                release.wait(2)

        worker = Worker(enable_health=False)
        first = Thread(target=worker.start,
                       kwargs={'embedded': True, 'startup_text': 'local test'})
        first.start()
        try:
            self.assertTrue(entered.wait(1))
            worker.start(embedded=True, startup_text='local test')
        finally:
            release.set()
            first.join(2)
        self.assertEqual(worker.calls, 1)

    def test_suicide_before_start_does_not_wait_forever_or_restart(self):
        worker = Link(enable_health=False)
        stopped = Event()

        def suicide():
            try:
                worker.suicide()
            except SystemExit:
                stopped.set()

        thread = Thread(target=suicide, daemon=True)
        thread.start()
        self.assertTrue(stopped.wait(1))
        worker.start(embedded=True, startup_text='stopped test')
        self.assertFalse(worker._started)

    def test_transform_publishes_falsy_values_before_commit(self):
        class Worker(Link):
            def transform(self, message):
                return message.value

        for value in (0, False, '', [], {}):
            with self.subTest(value=value):
                worker = Worker(input_stream='in', default_output_stream='out',
                                enable_health=False)
                worker.stopover = SimpleNamespace(
                    get=Mock(return_value=Response(status=20, value=value)),
                    put=Mock(), commit=Mock())
                worker._transform()
                worker.stopover.put.assert_called_once_with(value, 'out')
                worker.stopover.commit.assert_called_once()

    def test_commit_retains_only_the_unacknowledged_messages(self):
        link = Link(enable_health=False)
        messages = [object() for _ in range(3)]
        link._uncommitted_messages.extend(messages)
        link.stopover = SimpleNamespace(commit=Mock(
            side_effect=[None, RuntimeError('connection lost')]))
        with self.assertRaises(RuntimeError):
            link.commit()
        self.assertEqual(link._uncommitted_messages, messages[1:])
        link.stopover.commit.side_effect = None
        link.commit()
        self.assertEqual(link._uncommitted_messages, [])
        self.assertEqual([call.args[0] for call in
                          link.stopover.commit.call_args_list],
                         [messages[0], messages[1], messages[1], messages[2]])

    def test_default_intervals_do_not_leak_between_links(self):
        first = Link(enable_health=False)
        second = Link(enable_health=False)
        first._config['intervals']['loop_check_stop'] = 0.01
        self.assertEqual(second.config['intervals']['loop_check_stop'], 1)

    def test_false_docker_flag_keeps_the_generated_uid(self):
        with patch.dict(os.environ, CATENAE_DOCKER='false',
                        HOSTNAME='unrelated-host'), \
                patch('catenae.catenae.utils.get_uid',
                      return_value='generated'):
            link = Link(enable_health=False)
        self.assertEqual(link.uid, 'generated')


if __name__ == '__main__':
    unittest.main()
