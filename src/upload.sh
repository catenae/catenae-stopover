#!/bin/bash
cd $(dirname $0)/../src
rm dist/*
python3 setup.py install
python3 setup.py bdist_wheel
twine upload dist/*
