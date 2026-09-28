import aws_cdk as cdk

from stacks.storage_stack import StorageStack

env = cdk.Environment(account="656559336744", region="us-east-1")

app = cdk.App()
cdk.Tags.of(app).add("project", "mwaa-dbt-snowflake-elt")

StorageStack(app, "Elt-Storage", env=env)

app.synth()
