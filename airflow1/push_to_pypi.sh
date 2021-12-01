#!/bin/bash

rm -rf bigeye_airflow1.egg-info
rm -rf build
rm -rf dist
python3 setup.py sdist bdist_wheel
twine upload dist/*
