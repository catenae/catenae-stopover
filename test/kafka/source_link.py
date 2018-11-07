#!/usr/bin/env python
# -*- coding: utf-8 -*-

from catenae import Link, Electron, util
import time
import logging


class SourceLink(Link):
    def setup(self):
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug(f'{self.__class__.__name__} -> setup()')
        logging.debug(f'{self.__class__.__name__} -> output_topics: {self.output_topics}')
        self.message_count = 0

    def generator(self):

        # If a key is assigned, all the messages with the same key will
        # be assigned to the same partition, and thus, to the same consumer
        # within a consumer group. If the key is left empty, the messages
        # will be distributed in a round-robin fashion.

        logging.debug(f'{self.__class__.__name__} -> custom_input()')
        while True:
            electron = Electron('source_key_1',
                                f'source_value_{self.message_count}')
            self.send(electron)

            electron = Electron('source_key_2',
                                f'source_value_{self.message_count}')
            self.send(electron)

            logging.debug(f'{self.__class__.__name__} -> message sent')

            self.message_count += 1
            time.sleep(1)

if __name__ == "__main__":
    SourceLink().start(link_mode=Link.CUSTOM_INPUT)