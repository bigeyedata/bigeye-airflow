#!/usr/bin/bash
rm -rf dist
rm -rf .egg-info
rm -rf build
python3 -m pip install twine setuptools wheel
python3 setup.py bdist_wheel
twine upload --skip-existing dist/* --verbose
twine upload --repository codeartifact --skip-existing dist/*
rm -rf dist
rm -rf .egg-info
rm -rf build