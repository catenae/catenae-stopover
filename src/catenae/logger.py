#!/usr/bin/env python
# -*- coding: utf-8 -*-

from binnakle import Binnakle


class Logger:
    def __init__(
        self,
        instance,
        level=None,
        config=None,
    ):
        self.level = level.lower() if level else 'info'
        self.instance = instance

        if config is None:
            config = {}
        self._logger = Binnakle(**config)

    def log(
        self,
        message='',
        level=None,
    ):
        level = level.lower() if level else self.level
        getattr(self._logger, level.lower())(
            message,
            stack_depth=2,
            instance=self.instance.uid,
            microservice=self.instance.__class__.__name__,
        )
