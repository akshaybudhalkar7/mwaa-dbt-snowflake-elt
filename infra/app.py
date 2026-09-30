import aws_cdk as cdk

from stacks.ingest_stack import IngestStack
from stacks.mwaa_stack import MwaaStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack

env = cdk.Environment(account="656559336744", region="us-east-1")
SNOWFLAKE_ACCOUNT = "XJVCLEX-NNC43628"

app = cdk.App()
cdk.Tags.of(app).add("project", "mwaa-dbt-snowflake-elt")

storage = StorageStack(app, "Elt-Storage", env=env)
network = NetworkStack(app, "Elt-Network", env=env)
ingest = IngestStack(
    app,
    "Elt-Ingest",
    data_lake_bucket=storage.data_lake_bucket,
    snowflake_account=SNOWFLAKE_ACCOUNT,
    env=env,
)
MwaaStack(
    app,
    "Elt-Mwaa",
    vpc=network.vpc,
    mwaa_bucket=storage.mwaa_bucket,
    data_lake_bucket=storage.data_lake_bucket,
    env_name="mwaa-dbt-elt",
    airflow_version="3.3.1",
    dags_dir="../dags",
    config_dir="../mwaa",
    invokable_functions=(ingest.extractor,),
    env=env,
)

app.synth()
