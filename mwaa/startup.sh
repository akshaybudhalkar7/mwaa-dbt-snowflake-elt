#!/bin/sh
# Runs on every MWAA container before Airflow starts.
# dbt gets its own venv so its libraries never clash with Airflow's pinned versions.

export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"

# The webserver never runs dbt, so skip it there (faster startup)
if [ "${MWAA_AIRFLOW_COMPONENT}" != "webserver" ]; then
  export PIP_USER=false            # MWAA sets PIP_USER=true; a venv install fails with it on
  python3 -m venv "${DBT_VENV_PATH}"
  "${DBT_VENV_PATH}/bin/pip" install --quiet "dbt-snowflake>=1.10,<2"
  export PIP_USER=true             # restore for Airflow
fi
