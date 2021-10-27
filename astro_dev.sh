#!/usr/bin/bash

if [[ "$VIRTUAL_ENV" != "" ]]
then
  echo "Deactivating $VIRTUAL_ENV"
  deactivate
fi

VENV=venv
AF=./$VENV/bin/activate
if ! [ -f "$AF" ]; then
    echo "Creating Virtual Environment..."
    python3 -m venv $VENV
fi

. $AF

echo "Installing typer"
pip3 install typer

python3 ./astro_dev.py "$@"