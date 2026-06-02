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
                "subnet_id_1": "subnet-0fc890a7571121f58",
                "subnet_id_2": "subnet-05655f31ed833de3b",
                "sg": [
                    "sg-0c7f49465ab08bd5e"
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
                    availability_zone="us-east-1a",
                ),
                name="Redshift-Network",
            ),
        )
