import aws_cdk as cdk

from stacks.mwaa_stack import MwaaStack
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack

env = cdk.Environment(account="656559336744", region="us-east-1")

app = cdk.App()
cdk.Tags.of(app).add("project", "mwaa-dbt-snowflake-elt")

storage = StorageStack(app, "Elt-Storage", env=env)
network = NetworkStack(app, "Elt-Network", env=env)
MwaaStack(
    app,
    "Elt-Mwaa",
    vpc=network.vpc,
    mwaa_bucket=storage.mwaa_bucket,
    data_lake_bucket=storage.data_lake_bucket,
    env=env,
)

app.synth()
