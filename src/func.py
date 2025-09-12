from typing import List,Callable,Optional
from azure.mgmt.datafactory.models import\
(
    CopyActivity,\
    GetMetadataActivity,\
    ForEachActivity,\
    Activity,\
    SwitchActivity,\
    IfConditionActivity,\
    LookupActivity,\
    SqlServerStoredProcedureActivity,\
    ScriptActivity
)

def get_dataset_func(actv:Activity)->Optional[Callable[[Activity],List[str]]]:

    if isinstance(actv,CopyActivity):
        return handle_copy_activity
    elif isinstance(actv,GetMetadataActivity):
        return handle_metadata_activity
    elif isinstance(actv,ForEachActivity):
        return handle_foreach_activity
    elif isinstance(actv,SwitchActivity):
        return handle_switch_activity
    elif isinstance(actv,IfConditionActivity):
        return handle_if_condition_activity
    elif isinstance(actv,LookupActivity):
        return handle_lookup_activity
    elif isinstance(actv,SqlServerStoredProcedureActivity):
        return handle_sql_server_store_procedure_activity
    elif isinstance(actv,ScriptActivity):
        return handle_script_activity
      
    return None

def handle_copy_activity(actv:CopyActivity)->List[str]:

    dataset_names:List[str] = list()

    dataset_names.append(actv.inputs[0].reference_name)
    dataset_names.append(actv.outputs[0].reference_name)

    return dataset_names

def handle_metadata_activity(actv:GetMetadataActivity)->List[str]:
    return [actv.dataset.reference_name]


def handle_foreach_activity(actv:ForEachActivity)->List[str]:
    return handle_activities(activities=actv.activities)

def handle_switch_activity(actv:SwitchActivity)->List[str]:

    dataset_names:List[str] = list()

    for case in actv.cases:
        dataset_names.extend(handle_activities(activities=case.activities))
        
    return dataset_names

def handle_activities(activities:List[Activity])->List[str]:
    dataset_names:List[str] = list()

    for actv in activities:
        func = get_dataset_func(actv=actv)

        if func is not None:
            dataset_names.extend(func(actv))

    return dataset_names

def handle_if_condition_activity(actv:IfConditionActivity)->List[str]:

    dataset_names:List[str] = list()

    if actv.if_true_activities:
        dataset_names.extend(handle_activities(activities=actv.if_true_activities))

    if actv.if_false_activities:
        dataset_names.extend(handle_activities(activities=actv.if_false_activities))

    return dataset_names

def handle_lookup_activity(actv:LookupActivity)->List[str]:
    return [actv.dataset.reference_name]

def handle_sql_server_store_procedure_activity(actv:SqlServerStoredProcedureActivity)->List[str]:
    return ["<SqlServerStoredProcedureActivity:Dataset>"]

def handle_script_activity(actv:ScriptActivity)->List[str]:
    return ["<ScriptActivity:Dataset>"]