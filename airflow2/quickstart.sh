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

# Verifying whether api cred has been set.
if [[ ! -v BIGEYE_API_CRED_FILE ]]
then
  exit
fi

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "Installing Json Query CLI"
sudo apt install jq
BIGEYE_USER=$(jq -r '.user' < "$BIGEYE_API_CRED_FILE")
BIGEYE_PASS=$(jq -r '.password' < "$BIGEYE_API_CRED_FILE")
BIGEYE_URL=$(jq -r '.base_url' < "$BIGEYE_API_CRED_FILE")

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
  pip install --no-cache-dir -r requirements.txt

  # initialize the database
  airflow db init
  airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
  airflow connections add 'bigeye_prod' \
    --conn-type 'http' \
    --conn-login "$BIGEYE_USER" \
    --conn-password "$BIGEYE_PASS" \
    --conn-host "$BIGEYE_URL"
fi

if $run -eq true
then
export AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=604800

. venv/bin/activate

pip uninstall bigeye-airflow -y
pip install .

pip uninstall -r requirements.txt -y
pip install --no-cache-dir -r requirements.txt

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
  echo "stopping airflow."
  pkill -9 airflow
  rm -f  ~/running_airflow_webserver.pid
  rm -f ~/running_airflow_scheduler.pid
fi