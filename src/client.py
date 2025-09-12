import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from typing import List,Optional
from model import APIDatasetResource,APIPipelineResource,APILinkedServiceResource
from azure.synapse.artifacts import ArtifactsClient

class AzureClient:
    """
    Azure client for both azure data factory and azure synapse
    """
    def __init__(self,\
                 azure_client_id:str,\
                 azure_tenant_id:str,\
                 azure_client_secret:str,\
                 subscription_id:str,
                 resource_group_name:str,
                 data_factory_or_workspace:str,\
                 is_data_factory:bool=True):
                
        os.environ["AZURE_CLIENT_ID"] = azure_client_id
        os.environ["AZURE_TENANT_ID"] = azure_tenant_id
        os.environ["AZURE_CLIENT_SECRET"] = azure_client_secret

        self.resource_group_name = resource_group_name
        self.data_factory_or_workspace = data_factory_or_workspace

        if is_data_factory:
            self.client = DataFactoryClient(credential=DefaultAzureCredential(),\
                                        subscription_id=subscription_id,\
                                        resource_group_name=resource_group_name,\
                                        data_factory_name=data_factory_or_workspace)
        else:
            self.client = SynapseClient(credential=DefaultAzureCredential(),\
                                        workspace_name=data_factory_or_workspace)
            
    def get_datasets(self)->Optional[List[APIDatasetResource]]:
        return self.client.get_datasets() 
    
    def get_linked_service(self)->List[APILinkedServiceResource]:
        return self.client.get_linked_service()
    
    def get_pipelines(self)->Optional[List[APIPipelineResource]]:
        return self.client.get_pipelines()


   

class DataFactoryClient:

    def __init__(self,\
                 credential:DefaultAzureCredential,\
                 subscription_id:str,\
                 resource_group_name:str,\
                 data_factory_name:str):
        
        self.client = DataFactoryManagementClient(
                credential=credential,
                subscription_id=subscription_id
        )

        self.resource_group_name = resource_group_name

        self.data_factory_name = data_factory_name

    def get_datasets(self)->Optional[List[APIDatasetResource]]:
        
        try:
            return [
                APIDatasetResource(
                    dataset_name=dataset_resource.name,\
                    linked_service_name=dataset_resource.properties.linked_service_name.reference_name,\
                    azure_data_type=dataset_resource.properties.type,\
                    properties=dataset_resource.properties
                )
                for dataset_resource in self.client.datasets.list_by_factory(resource_group_name=self.resource_group_name,\
                                                                            factory_name=self.data_factory_name)
            ]
        except Exception:
            return None
        
    def get_linked_service(self)->List[APILinkedServiceResource]:

        try:
            return [
                APILinkedServiceResource(
                    linked_service_name=linked_service_resource.name,
                    azure_data_type=linked_service_resource.properties.type,
                    properties=linked_service_resource.properties
                )
                for linked_service_resource in self.client.linked_services.list_by_factory(resource_group_name=self.resource_group_name,\
                                                                                           factory_name=self.data_factory_name)
            ]
        except Exception:
            return None
  
    def get_pipelines(self)->Optional[List[APIPipelineResource]]:

        pipelines:List[APIPipelineResource] = list()

        try:
            for pipeline_resource in self.client.pipelines.list_by_factory(
                resource_group_name=self.resource_group_name,\
                factory_name=self.data_factory_name
            ):
                pipeline_name = pipeline_resource.name

                activities = list()

                if hasattr(pipeline_resource,"activities"):
                    activities = pipeline_resource.activities

                pipelines.append(
                    APIPipelineResource(
                        name=pipeline_name,\
                        activities=activities
                    )
                )

            return pipelines
        except Exception:
            return None


class SynapseClient:

    def __init__(self,\
                 credential:DefaultAzureCredential,\
                 workspace_name:str):
        
        self.client = ArtifactsClient(credential=credential,\
                                      endpoint=f"https://{workspace_name}.dev.azuresynapse.net")
        
    
    def get_datasets(self)->List[APIDatasetResource]:

        dataset:List[APIDatasetResource] = list()

        try:

            for dataset_resource in self.client.dataset.get_datasets_by_workspace():

                # SqlPoolTable does not have the linked servie reference name
                
                linked_service_name = ""

                if dataset_resource.properties.linked_service_name is not None:
                    linked_service_name = dataset_resource.properties.linked_service_name.reference_name


                dataset.append(
                    APIDatasetResource(
                        dataset_name=dataset_resource.name,\
                        linked_service_name=linked_service_name,\
                        azure_data_type=dataset_resource.properties.type,\
                        properties=dataset_resource.properties
                    )
                )
            
            return dataset

        except Exception as e:
            return None
        
    def get_linked_service(self)->List[APILinkedServiceResource]:

        try:
            return [
                APILinkedServiceResource(
                    linked_service_name=linked_service_resource.name,
                    azure_data_type=linked_service_resource.properties.type,
                    properties=linked_service_resource.properties
                )
                for linked_service_resource in self.client.linked_service.get_linked_services_by_workspace() 
            ]
        except Exception:
            return None

    def get_pipelines(self)->Optional[List[APIPipelineResource]]:

        pipelines:List[APIPipelineResource] = list()


        try:
            for pipeline_resource in self.client.pipeline.get_pipelines_by_workspace():
                pipeline_name = pipeline_resource.name

                activities = list()

                if hasattr(pipeline_resource,"activities"):
                    activities = pipeline_resource.activities

                pipelines.append(
                    APIPipelineResource(
                        name=pipeline_name,\
                        activities=activities
                    )
                )

            return pipelines
        except Exception:
            return None


   