#!/bin/sh
# Runs on every MWAA container before Airflow starts.
# dbt gets its own venv so its libraries never clash with Airflow's pinned versions.

export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"

# Only workers run tasks, so only they need dbt. Checking "= worker" (not "!= webserver")
# also skips the extra components Airflow 3 adds, keeping their startup fast.
if [ "${MWAA_AIRFLOW_COMPONENT}" = "worker" ]; then
  export PIP_USER=false            # MWAA sets PIP_USER=true; a venv install fails with it on
  python3 -m venv "${DBT_VENV_PATH}"
  "${DBT_VENV_PATH}/bin/pip" install --quiet "dbt-snowflake>=1.10,<2"
  export PIP_USER=true             # restore for Airflow
fi
