#!/bin/sh

black . 
isort . 
xenon --no-assert -a A -m B -b B . 
pylint carina tests asgi
mypy .
