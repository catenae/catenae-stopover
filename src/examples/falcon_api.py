import falcon
from catenae import Link
import logging
import cherrypy
from threading import Lock
import time

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)-15s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class CustomLink(Link):
    def setup(self, cherrypy_engine):
        self.cherrypy_engine = cherrypy_engine
        self.lock = Lock()
        self.current_message = None

    def stop(self):
        self.cherrypy_engine.exit()

    def generator(self):
        with self.lock:
            if self.current_message:
                self.send(self.current_message)
                self.current_message = None
            else:
                time.sleep(.05)


class API:
    def __init__(self, link):
        self.link = link

    def on_get(self, request, response, message):
        slot_available = False
        while not slot_available:
            with self.link.lock:
                if not self.link.current_message:
                    slot_available = True
                    self.link.current_message = message
                else:
                    time.sleep(.05)

        response.media = {'message': message, 'status': 'ok'}


link = CustomLink(default_output_stream='stream0')

api = falcon.App()
api.add_route('/{message}', API(link))

cherrypy.config.update({
    'server.socket_host': '0.0.0.0',
    'server.socket_port': 7474,
    'server.thread_pool': 8,
    'engine.autoreload.on': False,
    'checker.on': False,
    'tools.log_headers.on': False,
    'request.show_tracebacks': False,
    'request.show_mismatched_params': False,
    'log.screen': False,
    'engine.SIGHUP': None,
    'engine.SIGTERM': None
})
cherrypy.tree.graft(api, '/')
cherrypy.engine.start()
link.start(embedded=True, setup_kwargs={'cherrypy_engine': cherrypy.engine})
