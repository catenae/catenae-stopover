import falcon
from catenae import Link
from threading import Lock
import time
import bjoern


class CustomLink(Link):
    def setup(self):
        self.lock = Lock()
        self.current_message = None

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

    def on_get(
        self,
        _,
        response,
        message,
    ):
        slot_available = False
        while not slot_available:
            with self.link.lock:
                if not self.link.current_message:
                    slot_available = True
                    self.link.current_message = message
                else:
                    time.sleep(.05)

        response.media = {'message': message, 'status': 'ok'}


link = CustomLink(
    default_output_stream='stream0',
    binnakle_config={'pretty': True},
)
link.start(embedded=True)

api = falcon.App()
api.add_route('/{message}', API(link))
bjoern.run(api, '0.0.0.0', 7474)
