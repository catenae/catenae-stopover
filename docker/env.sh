#!/bin/bash
export VERSION=$(cat ../src/catenae/__init__.py | grep __version__ | cut -f2 -d\')
