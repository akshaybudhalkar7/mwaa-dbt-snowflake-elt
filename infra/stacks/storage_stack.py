from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class StorageStack(Stack):
    """S3 buckets: raw data lake (API -> Parquet) and MWAA (dags, requirements, dbt project)."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        common = dict(
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            # Demo project: tear everything down after the interview
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.data_lake_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=f"mwaa-dbt-elt-data-lake-{self.account}-{self.region}",
            **common,
        )

        # MWAA requires: versioning on, all public access blocked, same region as the environment.
        # The "airflow-" prefix is optional; it matches the AmazonMWAAFullConsoleAccess policy scope.
        self.mwaa_bucket = s3.Bucket(
            self,
            "MwaaBucket",
            bucket_name=f"airflow-mwaa-dbt-elt-{self.account}-{self.region}",
            **common,
        )

        # Role Snowflake assumes to read the data lake (used by the INS_S3_INT storage integration).
        # Fixed name -> the ARN is known before the role exists, so the Snowflake SQL can reference it.
        # Trust needs 2 values from Snowflake's "DESC INTEGRATION INS_S3_INT", passed as CDK context
        # (cdk.json). Until they're set, the role trusts only this account (a harmless placeholder).
        sf_user_arn = self.node.try_get_context("snowflake_iam_user_arn")
        sf_external_id = self.node.try_get_context("snowflake_external_id")
        if sf_user_arn and sf_external_id:
            snowflake_principal = iam.ArnPrincipal(sf_user_arn).with_conditions(
                {"StringEquals": {"sts:ExternalId": sf_external_id}}
            )
        else:
            snowflake_principal = iam.AccountRootPrincipal()

        self.snowflake_s3_role = iam.Role(
            self,
            "SnowflakeS3ReadRole",
            role_name="mwaa-dbt-elt-snowflake-s3-read",
            assumed_by=snowflake_principal,
        )
        self.data_lake_bucket.grant_read(self.snowflake_s3_role, "raw/*")

        # Snowpipe auto-ingest: new file under raw/ -> Snowflake's SQS queue -> pipe loads it.
        # Queue ARN comes from SHOW PIPES (notification_channel), passed as CDK context.
        pipe_queue_arn = self.node.try_get_context("snowflake_pipe_sqs_arn")
        if pipe_queue_arn:
            self.data_lake_bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.SqsDestination(sqs.Queue.from_queue_arn(self, "SnowpipeQueue", pipe_queue_arn)),
                s3.NotificationKeyFilter(prefix="raw/"),
            )

        CfnOutput(self, "DataLakeBucketName", value=self.data_lake_bucket.bucket_name)
        CfnOutput(self, "SnowflakeS3RoleArn", value=self.snowflake_s3_role.role_arn)
        CfnOutput(self, "MwaaBucketName", value=self.mwaa_bucket.bucket_name)
