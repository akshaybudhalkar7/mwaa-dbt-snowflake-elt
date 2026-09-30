"""Insurance ELT v2: watermark-driven API ingest -> Snowpipe -> bronze/silver/gold (dbt).

plan_request -> invoke_extractor -> wait_manifest -> reconcile -> freshness_gate -> dbt_build
-> advance_watermark  (the ONLY place the watermark moves: everything before it succeeded)
"""


import json
import re
from datetime import timedelta, timezone
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task, PokeReturnValue

from insurance.config import DATA_LAKE_BUCKET, DBT_VENV_PYTHON, EXTRACTOR_FUNCTION, RAW_API_PREFIX

# Runs with the dbt venv's Python (it has the Snowflake connector); lives next to this DAG
CONTROL_SCRIPT = Path(__file__).parent / "insurance" / "control.py"


# dbt project ships inside dags/ (MWAA only syncs dags/ to the workers); dbt runs from its own venv
DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
DBT_BIN = str(Path(DBT_VENV_PYTHON).parent / "dbt")
# Every dbt call of this DAG. Artifacts go to /tmp (the synced dags/ folder must not be written to),
# in a v2 folder so a v1 run on the same worker never overwrites them.
DBT_ARGS = (
    f" --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    " --target-path /tmp/dbt_v2/target --log-path /tmp/dbt_v2/logs"
)



@dag(
    schedule = "@daily",
    start_date=pendulum.datetime(2026,9,30, tz="UTC"),
    catchup= False,
    max_active_runs = 1,
    default_args = {
        "owner":"data-engg",
        "retries":2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["insurance", "elt", "v2"],
)

def insurance_elt_v2():
    @task
    def plan_request(dag_run=None) -> str:
        # Airflow 3: run_after is always set; logical_date can be None for manual runs
        run_after = dag_run.run_after.astimezone(timezone.utc)
        request = {
            "run_id": re.sub(r"[^A-Za-z0-9_.-]", "_", dag_run.run_id),
            "business_date": (run_after - timedelta(days=1)).date().isoformat(),
            "window_end": run_after.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        print(request)
        return json.dumps(request)  # Lambda payload must be a JSON string

    @task.sensor(poke_interval=30, timeout=10 * 60, mode="reschedule")
    def wait_manifest(request: str) -> PokeReturnValue:
        req = json.loads(request)
        key = f"{RAW_API_PREFIX}/_manifests/dt={req['business_date']}/run={req['run_id']}/_manifest.json"

        s3 = S3Hook()
        if not s3.check_for_key(key, bucket_name=DATA_LAKE_BUCKET):
            return PokeReturnValue(is_done=False)

        manifest = json.loads(s3.read_key(key, bucket_name=DATA_LAKE_BUCKET))
        print({entity: info["row_count"] for entity, info in manifest["entities"].items()})
        # is_done + xcom_value: the sensor finishes AND hands the manifest to the next tasks
        return PokeReturnValue(is_done=True, xcom_value=manifest)

    # Source-to-target check: rows Snowpipe loaded from THIS manifest's files == manifest counts.
    # exit 0 = done, 3 = Snowpipe still loading (poke again), anything else = real error
    @task.sensor(poke_interval=30, timeout=15 * 60, mode="reschedule")
    def reconcile(manifest: dict) -> bool:
        import subprocess

        result = subprocess.run(
            [DBT_VENV_PYTHON, str(CONTROL_SCRIPT), "reconcile", json.dumps(manifest)],
            capture_output=True,
            text=True,
        )
        print(result.stdout, result.stderr)

        if result.returncode not in (0, 3):
            raise RuntimeError(f"reconcile failed (exit code {result.returncode})")
        return result.returncode == 0


    # Freshness gate: is the SOURCE itself stale? Reconcile passes on "0 expected = 0 loaded";
    # if the API silently stops sending changes, only this catches it. error_after -> exit 1 -> stop.
    @task.bash(env={"DBT_TARGET": "prod"}, append_env=True)
    def freshness_gate() -> str:
        return f"{DBT_BIN} source freshness --select source:policy_admin_api{DBT_ARGS}"

    # source:policy_admin_api+ = the v2 source and EVERYTHING downstream of it:
    # bronze -> silver -> snapshot -> gold, with their tests. v1 models are not touched.
    # + dim_date: generated (no source), so it isn't downstream of the source - select it too.
    @task.bash(env={"DBT_TARGET": "prod"}, append_env=True)
    def dbt_build() -> str:
        return f"{DBT_BIN} build --select source:policy_admin_api+ dim_date{DBT_ARGS}"

    # The ONLY place the watermark moves - and only if every task above succeeded
    # (default trigger rule all_success). Safe to re-run: MERGE + GREATEST, never backwards.
    @task
    def advance_watermark(manifest: dict) -> None:
        import subprocess

        result = subprocess.run(
            [DBT_VENV_PYTHON, str(CONTROL_SCRIPT), "advance", json.dumps(manifest)],
            capture_output=True,
            text=True,
        )
        print(result.stdout, result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"advance failed (exit code {result.returncode})")

    # Runs only when a task above FAILED (trigger rule one_failed): marks the run FAILED in
    # EXTRACT_RUNS. Watermarks are untouched -> the next run re-pulls the same window.
    @task(trigger_rule="one_failed", retries=0)
    def record_failure(request: str) -> None:
        import subprocess

        req = json.loads(request)
        message = f"DAG run {req['run_id']} failed - see the Airflow task logs"
        result = subprocess.run(
            [DBT_VENV_PYTHON, str(CONTROL_SCRIPT), "fail", req["run_id"], req["business_date"], message],
            capture_output=True,
            text=True,
        )
        print(result.stdout, result.stderr)

    # ------------------------------------------------------------------ flow
    # Calling a @task function creates the task; its XCom output feeds the next call
    request = plan_request()
    invoke_extractor = LambdaInvokeFunctionOperator(
        task_id="invoke_extractor",
        function_name=EXTRACTOR_FUNCTION,
        payload=request,  # plan_request's XCom: frozen -> every retry sends the same window
        invocation_type="RequestResponse",  # synchronous: the task fails if the Lambda fails
        # botocore's default read timeout is 60s but the Lambda may run 5 min. Without this the
        # call "times out" while the Lambda keeps running, Airflow retries -> 2 extracts at once.
        botocore_config={"read_timeout": 330, "connect_timeout": 10, "retries": {"total_max_attempts": 1}},
    )
    manifest = wait_manifest(request)
    reconciled = reconcile(manifest)
    freshness = freshness_gate()
    build = dbt_build()
    advanced = advance_watermark(manifest)
    failure = record_failure(request)

    # Happy path, left to right: the watermark moves only if every step before it succeeded
    request >> invoke_extractor >> manifest >> reconciled >> freshness >> build >> advanced
    # Any failure on the way -> record it (trigger_rule="one_failed")
    [invoke_extractor, manifest, reconciled, freshness, build, advanced] >> failure


insurance_elt_v2()