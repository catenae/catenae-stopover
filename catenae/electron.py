#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy


class Electron:
    def __init__(self,
                 key=None,
                 value=None,
                 stream=None,
                 previous_stream=None,
                 unpack_if_string=False,
                 callbacks=None,
                 timestamp=None):
        self.key = key
        self.value = value
        self.stream = stream  # Destination stream
        self.previous_stream = previous_stream
        self.unpack_if_string = unpack_if_string
        if callbacks is None:
            self.callbacks = []
        else:
            self.callbacks = callbacks
        self.timestamp = timestamp

    def __bool__(self):
        if self.value != None:
            return True
        return False

    def get_sendable(self):
        copy = self.copy()
        copy.stream = None
        copy.previous_stream = None
        copy.unpack_if_string = False
        copy.callbacks = []
        copy.timestamp = None
        return copy

    def copy(self):
        electron = Electron()
        electron.key = self.key
        electron.value = copy.deepcopy(self.value)
        electron.stream = self.stream
        electron.previous_stream = self.previous_stream
        electron.unpack_if_string = self.unpack_if_string
        electron.callbacks = self.callbacks
        electron.timestamp = self.timestamp
        return electron
