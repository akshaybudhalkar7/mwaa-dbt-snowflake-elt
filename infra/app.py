import aws_cdk as cdk

from stacks.mwaa_stack import MwaaStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack

env = cdk.Environment(account="656559336744", region="us-east-1")

app = cdk.App()
cdk.Tags.of(app).add("project", "mwaa-dbt-snowflake-elt")

storage = StorageStack(app, "Elt-Storage", env=env)
network = NetworkStack(app, "Elt-Network", env=env)

# Blue/green upgrade: "blue" = current Airflow 2 environment, "green" = new Airflow 3 environment.
# Both run side by side until green is validated, then blue is removed.
MwaaStack(
    app,
    "Elt-Mwaa",
    vpc=network.vpc,
    mwaa_bucket=storage.mwaa_bucket,
    data_lake_bucket=storage.data_lake_bucket,
    env_name="mwaa-dbt-elt",
    airflow_version="2.10.3",
    dags_dir="../dags",
    config_dir="../mwaa",
    env=env,
)
MwaaStack(
    app,
    "Elt-Mwaa-V3",
    vpc=network.vpc,
    mwaa_bucket=storage.mwaa_bucket,
    data_lake_bucket=storage.data_lake_bucket,
    env_name="mwaa-dbt-elt-v3",
    airflow_version="3.3.1",
    dags_dir="../dags_v3",
    config_dir="../mwaa_v3",
    s3_prefix="v3/",
    env=env,
)

app.synth()
