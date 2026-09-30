from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct

from stacks.lambda_build import python_asset

REPO = Path(__file__).resolve().parents[2]
RAW_API_PREFIX = "raw/api"  # under raw/ -> existing storage integration + Snowpipe notification cover it


class IngestStack(Stack):
    """API ingestion (v2): mock source API + extractor Lambda that lands NDJSON in the data lake.

    Airflow invokes the extractor; it reads the watermark from Snowflake, pages the source API
    and writes files + _manifest.json under raw/api/, where Snowpipe picks them up.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        data_lake_bucket: s3.IBucket,
        snowflake_account: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- 1. Mock source API: generator.py behind a Function URL (IAM auth -> no API keys) ---
        source_api = lambda_.Function(
            self,
            "SourceApiFunction",
            function_name="mwaa-dbt-elt-source-api",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                python_asset(
                    "source_api",
                    {
                        "handler.py": REPO / "lambdas/source_api/handler.py",
                        # Same generator as v1 -> both pipelines see identical data
                        "insurance/__init__.py": REPO / "dags/insurance/__init__.py",
                        "insurance/config.py": REPO / "dags/insurance/config.py",
                        "insurance/generator.py": REPO / "dags/insurance/generator.py",
                    },
                )
            ),
            memory_size=256,
            timeout=Duration.seconds(30),
            environment={"THROTTLE_RATE": "0"},  # failure drill: 0.2 -> extractor must retry 429s
            log_group=self._log_group("SourceApiLogs", "mwaa-dbt-elt-source-api"),
        )
        source_api_url = source_api.add_function_url(auth_type=lambda_.FunctionUrlAuthType.AWS_IAM)

        # --- 2. Extractor role: FIXED name -> Snowflake user INS_LAMBDA_SVC trusts exactly this ARN ---
        extractor_role = iam.Role(
            self,
            "ExtractorRole",
            role_name="mwaa-dbt-elt-extractor",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
        )
        # Write-only, and only its own prefix: it can't read or overwrite v1 data
        data_lake_bucket.grant_put(extractor_role, f"{RAW_API_PREFIX}/*")
        # IAM-auth Function URL: grants InvokeFunctionUrl + InvokeFunction (needed for URLs since 2025),
        # the latter only "InvokedViaFunctionUrl" -> the API can't be called directly, bypassing the URL
        source_api_url.grant_invoke_url(extractor_role)

        # --- 3. Extractor ---
        self.extractor = lambda_.Function(
            self,
            "ExtractorFunction",
            function_name="mwaa-dbt-elt-extractor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                python_asset(
                    "extractor",
                    {"handler.py": REPO / "lambdas/extractor/handler.py"},
                    requirements=REPO / "lambdas/extractor/requirements.txt",
                )
            ),
            role=extractor_role,
            memory_size=512,  # the Snowflake connector is heavy; more memory = more CPU = faster start
            timeout=Duration.minutes(5),
            retry_attempts=0,  # Lambda's async retries off: Airflow owns retries
            environment={
                "DATA_LAKE_BUCKET": data_lake_bucket.bucket_name,
                "RAW_API_PREFIX": RAW_API_PREFIX,
                "SOURCE_API_URL": source_api_url.url,
                "SOURCE_NAME": "policy_admin_api",
                "BUFFER_HOURS": "2",
                "SNOWFLAKE_AUTH": "wif",
                "SNOWFLAKE_ACCOUNT": snowflake_account,
                "SNOWFLAKE_USER": "INS_LAMBDA_SVC",
                "SNOWFLAKE_ROLE": "INS_EXTRACTOR",
                "SNOWFLAKE_WAREHOUSE": "INS_WH",
                # Lambda's file system is read-only except /tmp; the connector writes caches under $HOME
                "HOME": "/tmp",
            },
            log_group=self._log_group("ExtractorLogs", "mwaa-dbt-elt-extractor"),
        )

        CfnOutput(self, "ExtractorFunctionName", value=self.extractor.function_name)
        CfnOutput(self, "SourceApiUrl", value=source_api_url.url)

    def _log_group(self, construct_id: str, function_name: str) -> logs.LogGroup:
        # Explicit log group: 1-week retention (the default keeps logs forever) and deleted with the stack
        return logs.LogGroup(
            self,
            construct_id,
            log_group_name=f"/aws/lambda/{function_name}",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
