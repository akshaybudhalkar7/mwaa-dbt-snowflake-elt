from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
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

        CfnOutput(self, "DataLakeBucketName", value=self.data_lake_bucket.bucket_name)
        CfnOutput(self, "MwaaBucketName", value=self.mwaa_bucket.bucket_name)
