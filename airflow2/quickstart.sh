#!/usr/bin/bash

init=false
run=false
stop=false

while getopts 'irsh' opt; do
  case "$opt" in
    i)
      echo "Will initialize airflow."
      init=true
      ;;
    r)
      echo "Will run airflow."
      run=true
      ;;
    s)
      echo "Will stop airflow."
      stop=true
      ;;

    ?|h)
      echo "Usage: $(basename $0) [-i (initialize) -r (run) -s (stop)]"
      exit 1
      ;;
  esac
done
# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "AIRFLOW_HOME: $AIRFLOW_HOME"

if $init -eq true
then
  echo "Initializing instance."
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

if $run -eq true
then
export AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=604800
. venv/bin/activate
pip uninstall bigeye-airflow -y
pip install .

# start the web server, default port is 8080
airflow webserver --port 8080 &> ~/airflow_webserver.log &
echo "$!" > ~/running_airflow_webserver.pid

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
airflow scheduler &> ~/airflow_scheduler.log &
echo "$!" > ~/running_airflow_scheduler.pid
fi

if $stop -eq true
then
  if test -f ~/running_airflow_webserver.pid
  then
    echo "killing webserver."
    WPID=$(cat ~/running_airflow_webserver.pid)
    kill "$WPID"
    rm ~/running_airflow_webserver.pid
  fi
  if test -f ~/running_airflow_scheduler.pid
  then
    echo "killing scheduler."
    SPID=$(cat ~/running_airflow_scheduler.pid)
    kill "$SPID"
    rm ~/running_airflow_scheduler.pid
  fi
  if ! test -f ~/running_airflow_webserver.pid && ! test -f ~/running_airflow_scheduler.pid
  then
    echo "airflow not running"
  fi
fi