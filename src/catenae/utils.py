#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
import hashlib
from collections import OrderedDict
from uuid import uuid4


def get_timestamp():
    return int(round(time.time()))


def get_timestamp_ms():
    return int(round(time.time() * 1000))


def dump_dict_pretty(input_dict):
    return json.dumps(
        input_dict,
        indent=4,
        ensure_ascii=False
    )


def dump_dict(input_dict):
    return json.dumps(input_dict, separators=(',', ':'), ensure_ascii=False)


def load_dict(str_dict):
    return json.loads(str_dict, object_pairs_hook=OrderedDict)


def blake2b_256(text: str):
    return hashlib.blake2b(text.encode('utf-8'), digest_size=32).hexdigest()


def blake2b_512(text: str):
    return hashlib.blake2b(text.encode('utf-8'), digest_size=64).hexdigest()


def sha3_512(text: str):
    return hashlib.sha3_512(text.encode('utf-8')).hexdigest()


def get_uid():
    return blake2b_256(str(uuid4()))[:12]
