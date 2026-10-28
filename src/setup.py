#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup
from pathlib import Path
import re

ROOT = Path(__file__).parent
VERSION = re.search(r"^__version__ = '([^']+)'",
                    (ROOT / 'catenae/__init__.py').read_text(), re.M)[1]

setup(
    name='catenae-stopover',
    version=VERSION,
    description='Microservices framework for Stopover',
    long_description='Microservices framework for Stopover.',
    long_description_content_type='text/plain',
    python_requires='>=3.10,<4',
    url='https://github.com/catenae',
    author='Rodrigo Martínez',
    author_email='dev@brunneis.com',
    license='GNU General Public License v3.0',
    packages=find_packages(),
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.14",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=[
        'stopover >= 2.2610.0, < 3.0.0',
        'falcon >= 4.3.1, < 5.0.0',
        'waitress >= 3.0.2, < 4.0.0',
        'binnakle >= 2.2610.0, < 3.0.0',
    ]
)
