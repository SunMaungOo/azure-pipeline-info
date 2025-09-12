from client import AzureClient
from decouple import config

AZURE_CLIENT_ID = config("AZURE_CLIENT_ID",cast=str)

AZURE_TENANT_ID = config("AZURE_TENANT_ID",cast=str)

AZURE_CLIENT_SECRET = config("AZURE_CLIENT_SECRET",cast=str)

AZURE_SUBSCRIPTION_ID = config("AZURE_SUBSCRIPTION_ID",cast=str)

AZURE_RESOURCE_GROUP_NAME = config("AZURE_RESOURCE_GROUP_NAME",cast=str)

AZURE_DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME = config("AZURE_DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME",cast=str)

IS_AZURE_DATA_FACTORY = config("IS_AZURE_DATA_FACTORY",default=True,cast=bool)

DAYS_SEARCH = config("DAYS_SEARCH",default=1,cast=int)

OUTPUT_FILE_PATH = config("OUTPUT_FILE_PATH",default="data/pipeline-info.json",cast=str)


def get_api_client()->AzureClient:

    return AzureClient(azure_client_id=AZURE_CLIENT_ID,\
        azure_tenant_id=AZURE_TENANT_ID,\
        azure_client_secret=AZURE_CLIENT_SECRET,\
        subscription_id=AZURE_SUBSCRIPTION_ID,\
        resource_group_name=AZURE_RESOURCE_GROUP_NAME,\
        data_factory_or_workspace=AZURE_DATA_FACTORY_OR_SYNAPSE_WORKSPACE_NAME,\
        is_data_factory=IS_AZURE_DATA_FACTORY)

