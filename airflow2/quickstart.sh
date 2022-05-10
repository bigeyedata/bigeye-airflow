#!/usr/bin/bash

init=false

while getopts 'ih' opt; do
  case "$opt" in
    i)
      echo "Will initialize airflow."
      init=true
      ;;

    ?|h)
      echo "Usage: $(basename $0) [-i (initialize)]"
      exit 1
      ;;
  esac
done
# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


if $init -eq true
then
  deactivate
  rm -rf venv
  rm airflow.cfg
  rm airflow.db
  rm webserver_config.py
  python3 -m venv venv
  . venv/bin/activate

  pip install wheel
  pip install -r requirements.txt

  # initialize the database
  airflow db init

airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
fi

if [[ "$VIRTUAL_ENV" != "" ]]
then
  . venv/bin/activate
fi

# start the web server, default port is 8080
airflow webserver --port 8080

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler