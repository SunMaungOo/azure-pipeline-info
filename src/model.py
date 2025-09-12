from dataclasses import dataclass
from typing import List,Any,Dict


@dataclass
class APIDatasetResource:
    dataset_name:str
    linked_service_name:str
    azure_data_type:str
    #dataset properties
    properties:Dict[str,Any]

@dataclass
class APILinkedServiceResource:
    linked_service_name:str
    azure_data_type:str
    #linked service properties
    properties:Dict[str,Any]

@dataclass
class APIPipelineResource:
    name:str
    #list of activities object
    activities:List[Any]

@dataclass
class DatasetInfo:
    dataset_name:str
    dataset_type:str
    linked_service_name:str
    linked_service_type:str

@dataclass
class ActivityInfo:
    name:str
    datasets:List[DatasetInfo]

@dataclass
class PipelineInfo:
    name:str
    activities:List[ActivityInfo]

