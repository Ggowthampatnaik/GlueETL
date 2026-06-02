from aws_cdk import (
    Aws,
    Fn,
    Stack,
    aws_glue as glue,
)
from constructs import Construct


class StoreOpsGlueConnectionsStack(Stack):
    """
    Sets up resources for Glue Connections.
    """

    def __init__(self, scope: Construct, construct_id: str, env_name, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        environment = env_name

        env_config = {
            "dev": {
                "subnet_id_1": Fn.import_value("SignetNetworkBaseline:GLUESubnet2Id"),
                "subnet_id_2": Fn.import_value("SignetNetworkBaseline:GLUESubnet3Id"),
                "sg": [
                    "sg-059dccd5283e95420",
                    "sg-0953e6fc57ff4f6b6",
                    "sg-09ce0e9461c7af9ff",
                    "sg-0c384edc21ffbe7bd",
                ],
            },
            "test": {
                "subnet_id_1": Fn.import_value("SignetNetworkBaseline:GLUESubnet3Id"),
                "subnet_id_2": Fn.import_value("SignetNetworkBaseline:GLUESubnet4Id"),
                "sg": [
                    "sg-05d9d7311c19c093d",
                    "sg-0a16ebce524f17d3a",
                    "sg-0a6a4bfb64d09dd82",
                    "sg-0f2012e3ada80746b",
                ],
            },
            "prod": {
                "subnet_id_1": "subnet-0c854bba1a91c6686",
                "subnet_id_2": "subnet-098cdb5496cdb76fd",
                "sg": [
                    "sg-02f9f6c35b3333221",
                    "sg-0630700be9de941d3",
                    "sg-074234bd628f78fa8",
                    "sg-0a0c37af9fe53360c",
                ],
            },
        }

        config = env_config.get(environment)
        if config is None:
            raise ValueError(f"Unknown environment: {environment}")

        glue.CfnConnection(
            self,
            f"{environment}RedshiftNetworkConnection",
            catalog_id=Aws.ACCOUNT_ID,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                connection_type="NETWORK",
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    subnet_id=config["subnet_id_2"],
                    security_group_id_list=config["sg"],
                    availability_zone="us-east-2b",
                ),
                name="Redshift-Network",
            ),
        )
