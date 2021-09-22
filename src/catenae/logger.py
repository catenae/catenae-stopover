#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


class Logger:
    def __init__(self, instance, level=None):
        if level is None:
            level = 'info'
        self.instance = instance
        logging.basicConfig(
            format='%(asctime)-15s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        logging.getLogger().setLevel(getattr(
            logging,
            level.upper(),
            logging.INFO)
        )

    def log(self, message='', level='info'):
        if message:
            message = (f'{self.instance.__class__.__name__}/{self.instance.uid}'
                       f' → {message}')
        getattr(logging, level.lower(), 'INFO')(message)
