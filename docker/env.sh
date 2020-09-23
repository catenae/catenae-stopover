#!/bin/bash
export VERSION=$(cat ../catenae/__init__.py | grep __version__ | cut -f2 -d\')
