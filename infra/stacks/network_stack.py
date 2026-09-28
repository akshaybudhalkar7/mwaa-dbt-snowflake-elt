from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class NetworkStack(Stack):
    """VPC for MWAA: 2 AZs, public + private subnets, one NAT gateway for outbound internet."""

    @property
    def availability_zones(self) -> list[str]:
        # Fixed AZs: avoids an AWS lookup at synth time
        return ["us-east-1a", "us-east-1b"]

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.10.0.0/16"),
            # MWAA needs 2 private subnets in different AZs
            max_azs=2,
            # One NAT (not one per AZ) to keep demo cost down
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24),
                ec2.SubnetConfiguration(
                    name="private", subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS, cidr_mask=24
                ),
            ],
        )
