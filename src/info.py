from config import get_api_client,OUTPUT_FILE_PATH,AZURE_TENANT_ID
from typing import Dict,Tuple,List,Optional
from model import APIDatasetResource,APILinkedServiceResource,PipelineInfo,DatasetInfo,ActivityInfo
from azure.mgmt.datafactory.models import Activity
from dataclasses import asdict
from pathlib import Path
from func import get_dataset_func
import json
import logging

logger = logging.getLogger("azure-pipeline-info")

logger.setLevel(logging.DEBUG)

# Prevent propagation to root logger (avoids duplicate logs if used in packages)
logger.propagate = False

formatter = logging.Formatter(
    fmt='%(asctime)s | %(levelname)-8s | %(name)-15s | %(lineno)-3d | %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S.%fZ'  # ISO 8601 with microseconds → Z for UTC
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

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

    logger.info("Extracting datasets:",end="")

    datasets = api_client.get_datasets()

    if dataset is None:
        logger.info("fail")
    else:
        logger.info("success")

    logger.info("Extracting linked service:",end="")

    linked_services = api_client.get_linked_service()

    if linked_services is None:
        logger.info("fail")
    else:
        logger.info("success")

    dataset_map = get_dataset_mapping(datasets=datasets,linked_services=linked_services)

    pipeline_infos:List[PipelineInfo] = list()

    logger.info("Extracting pipeline:",end="")

    pipelines = api_client.get_pipelines()

    if pipelines is None:
        logger.info("fail")
    else:
        logger.info("success")

    logger.info("Getting pipeline info:",end="")

    for x in pipelines:

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
            pipeline_infos.append(PipelineInfo(
                name=pipeline_name,\
                activities=activities
            ))


    logger.info(f"{len(pipeline_infos)} info found")

    logger.info(f"Saving pipeline info to {OUTPUT_FILE_PATH}:",end="")

    try:
        output_file_path = Path(OUTPUT_FILE_PATH)
        output_file_path.parent.mkdir(parents=True,exist_ok=True)

        with output_file_path.open(mode="w") as file:
            json.dump([asdict(pipeline_info) for pipeline_info in pipeline_infos],file,indent=4)

        logger.info("success")

    except:
        logger.info("fail")

if __name__ == "__main__":
    main()
