#!/usr/bin/bash

r2p=false

while getopts 'ph' opt; do
  case "$opt" in
    p)
      echo "Releasing to public and private PyPi"
      r2p=true
      ;;

    ?|h)
      echo "Usage: $(basename $0) [-p (public)]"
      exit 1
      ;;
  esac
done

rm -rf dist
rm -rf .egg-info
rm -rf build
python3 -m pip install twine setuptools wheel
python3 setup.py bdist_wheel
if $r2p -eq true
then
  twine upload --skip-existing dist/* --verbose TODO uncomment when we want to push to public
fi
VERSION=$(python3 setup.py --version)
PACKAGE=$(python3 setup.py --name)
aws codeartifact delete-package-versions --domain bigeye --domain-owner 021451147547 \
--repository pypi-store --format pypi \
--package "$PACKAGE" --versions "$VERSION" --profile toro
twine upload --repository codeartifact --skip-existing dist/*
rm -rf dist
rm -rf .egg-info
rm -rf build
