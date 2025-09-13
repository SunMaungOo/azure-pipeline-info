from config import get_api_client,OUTPUT_FILE_PATH,AZURE_TENANT_ID
from typing import Dict,Tuple,List,Optional
from model import APIDatasetResource,APILinkedServiceResource,PipelineInfo,DatasetInfo,ActivityInfo
from azure.mgmt.datafactory.models import Activity
from dataclasses import asdict
from pathlib import Path
from func import get_dataset_func
import json

DatasetMap = Dict[str,Tuple[Optional[APIDatasetResource],Optional[APILinkedServiceResource]]]

 
def get_dataset_mapping(datasets:List[APIDatasetResource],\
                        linked_services:List[APILinkedServiceResource])->DatasetMap:
    """
    Return key = dataset name

    value = (None,None) for only linked service name
    """
    
    dataset_map:DatasetMap = dict()

    for dataset in datasets:
        for ls in linked_services:
            if dataset.linked_service_name==ls.linked_service_name:
                dataset_map[dataset.dataset_name]=(dataset,ls)

    dataset_map["<SqlServerStoredProcedureActivity:Dataset>"] = (None,None)
    dataset_map["<ScriptActivity:Dataset>"] = (None,None)

    return dataset_map

def get_dataset_info(actv:Activity,\
                     dataset_map:DatasetMap)->List[DatasetInfo]:

    datasets:List[DatasetInfo] = list()

    func = get_dataset_func(actv=actv)

    if func is None:
        return list()
    
    for dataset_name in func(actv):
        result = dataset_map[dataset_name]
                    
        dataset_type = "<NONE>"

        linked_service_name = "<NONE>"

        linked_service_type = "<NONE>"

        if result is not None:
            
            dataset_info = result[0]

            linked_service_info = result[1]

            if dataset_info is not None:
                dataset_type = dataset_info.azure_data_type

            if linked_service_info is not None:
                linked_service_name = linked_service_info.linked_service_name

                linked_service_type = linked_service_info.azure_data_type

            datasets.append(DatasetInfo(
                dataset_name=dataset_name,
                dataset_type=dataset_type,
                linked_service_name=linked_service_name,
                linked_service_type=linked_service_type
            ))

    return datasets

def main():

    api_client = get_api_client()

    datasets = api_client.get_datasets()

    linked_services = api_client.get_linked_service()

    dataset_map = get_dataset_mapping(datasets=datasets,linked_services=linked_services)

    pipelines:List[PipelineInfo] = list()

    for x in api_client.get_pipelines():

        pipeline_name = x.name

        activities:List[ActivityInfo] = list()

        for actv in x.activities:

            activity_name = actv.name

            datasets = get_dataset_info(actv=actv,dataset_map=dataset_map)

            if len(datasets)>0:
                activities.append(ActivityInfo(
                    name=activity_name,
                    datasets=datasets
                ))


        if len(activities)>0:
            pipelines.append(PipelineInfo(
                name=pipeline_name,\
                activities=activities
            ))

    output_file_path = Path(OUTPUT_FILE_PATH)
    output_file_path.parent.mkdir(parents=True,exist_ok=True)

    with output_file_path.open(mode="w") as file:
        json.dump([asdict(pipeline) for pipeline in pipelines],file,indent=4)


if __name__ == "__main__":
    main()
