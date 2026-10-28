"""Exercise the package's own health server over loopback sockets."""

from http.client import HTTPConnection
import json
import socket
from threading import Thread
import subprocess
import sys
import time
import unittest

from catenae.health import HealthCheck


class HealthTests(unittest.TestCase):
    def test_close_before_start_is_idempotent(self):
        health = HealthCheck(0)
        health.close()
        health.close()
        health.start()
        self.assertIsNone(health.server.socket)

    def test_health_only_link_lives_until_sigterm(self):
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        code = ('from catenae import Link; import sys; '
                'Link(health_port=int(sys.argv[1])).start('
                'startup_text="health-only")')
        process = subprocess.Popen(
            [sys.executable, '-B', '-c', code, str(port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while True:
                self.assertIsNone(process.poll(), 'health-only link exited')
                probe = HTTPConnection('127.0.0.1', port, timeout=0.5)
                try:
                    probe.request('GET', '/health')
                    response = probe.getresponse()
                    response.read()
                    if response.status == 503 and time.monotonic() < deadline:
                        time.sleep(0.02)
                        continue
                    self.assertEqual(response.status, 200)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        self.fail('health-only link did not start')
                    time.sleep(0.02)
                finally:
                    probe.close()
            process.terminate()
            self.assertEqual(process.wait(2), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(2)
            process.stderr.close()

    def test_partial_clients_do_not_block_probe_or_shutdown(self):
        health = HealthCheck(0)
        thread = Thread(target=health.start, daemon=True)
        thread.start()
        address = ('127.0.0.1', health.server.effective_port)
        clients = []
        try:
            for _ in range(8):
                client = socket.create_connection(address, timeout=7)
                client.sendall(b'GET /health HTTP/1.1\r\nHost: localhost\r\n')
                clients.append(client)
            probe = HTTPConnection(*address, timeout=1)
            try:
                probe.request('GET', '/health')
                response = probe.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read()),
                                 {'status': 'available'})
            finally:
                probe.close()
            self.assertEqual(clients[0].recv(1), b'')
        finally:
            for client in clients:
                client.close()
            health.close()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            health.close()


if __name__ == '__main__':
    unittest.main()
