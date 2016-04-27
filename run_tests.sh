#!/bin/bash

for module in registry; do
    coverage run --source=${module}.py tests.py
    coverage report -m
done
