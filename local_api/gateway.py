import json
from types import SimpleNamespace
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Union, Tuple, List, Dict, Set, Optional
import datahub.emitter.mce_builder as builder
from uuid import uuid4

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass


def del_none(d):
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d  # For convenience


class Input:
    namespace: str
    name: str
    facet: Dict[str, str]

class Output:
    namespace: str
    name: str
    facets: Dict[str, str]

@dataclass
class Run:
    runId: str
    facets: Dict[str, str]

@dataclass_json
@dataclass
##open lineage record
class Record:
    eventTime: str
    eventType: str
    inputs: Optional[List[Input]]
    job: Dict[str, str]
    outputs: Optional[List[Output]]
    producer: str
    run: Run

@dataclass_json
@dataclass
class DatahubRecord:
    entityType: str
    entityUrn: str
    changeType: str
    aspectName: Optional[str] #todo required?
    aspect: Optional[Dict[str, str]] #todo required?


# check openlineage model:
# https://openlineage.io/docs/spec/object-model
#https://openlineage.io/docs/spec/facets/dataset-facets/storage

#datahub sample
#{"proposal": {"entityType": "dataset", "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:mendix,my-project.my-dataset.user-table,PROD)", "changeType": "UPSERT", "aspectName": "datasetProperties", "aspect": {"value": "{\"customProperties\": {\"governance\": \"ENABLED\"}, \"description\": \"This table stored the canonical User profile\", \"tags\": []}", "contentType": "application/json"}}}



test_json = """{"eventTime": "2021-11-03T10:53:52.427343", "eventType": "START", "inputs": [], "job": {"facets": {}, "name": "job", "namespace": "openlineage"}, "outputs": [], "producer": "producer", "run": {"facets": {}, "runId": "69f4acab-b87d-4fc0-b27b-8ea950370ff3"}}"""
test_json_simple_complete_run = """{"eventTime": "2022-12-03T21:27:53.931729", "eventType": "COMPLETE", "inputs": [{"facets": {}, "name": "public.inventory", "namespace": "food_delivery"}], "job": {"facets": {}, "name": "example.order_data", "namespace": "food_delivery"}, "outputs": [{"facets": {}, "name": "public.menus_1", "namespace": "food_delivery"}, {"facets": {}, "name": "public.orders_1", "namespace": "food_delivery"}], "producer": "producer", "run": {"facets": {}, "runId": "cacfe14b-8fb6-4542-bb8b-f686fa9cf401"}}"""

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})
emitter.test_connection()


def handle_openlineage_record(data):
    #print(data)
    #x = json.loads(data, object_hook=lambda d: SimpleNamespace(**d))
    record = Record.from_json(str(data).replace('\'',"\""))
    print(f"Record {record}")
    convert_record_to_datahub_format(record)

def construct_urn(record: Record):
    dataset_urn = ""
    job_id = record.run.runId
    dataset_urn=builder.make_data_job_urn("functions", flow_id=job_id, job_id = job_id)
    return dataset_urn

def construct_urns_dataset(record):
    dataset_urns = []
    for input in record.inputs:
        print(f'input datasets {input}')
        dataset_urns.append(builder.make_dataset_urn(input["name"], input["namespace"]))
    for output in record.outputs:
        dataset_urns.append(builder.make_dataset_urn(output["name"], output["namespace"]))
    return dataset_urns

def convert_record_to_datahub_format_job(record: Record):
    # entityType = "job" #https://datahubproject.io/docs/graphql/enums#entitytype
    entityUrn = construct_urn(record)
    # changeType = "UPSERT"
    # data = DatahubRecord(entityType, entityUrn, changeType, None, None)
    # json_res = del_none(json.loads(data.to_json()))
    # print(f"Result {json_res}")
    metadata_event = MetadataChangeProposalWrapper(
        entityType="datajob",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=entityUrn,
    )

    emitter.emit(metadata_event)


def convert_record_to_datahub_format(record: Record):
    for urn in construct_urns_dataset(record):
        dataset_properties = DatasetPropertiesClass(description="")
        metadata_event = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=urn,
            aspectName = "datasetProperties",
            aspect = dataset_properties,
        )
        emitter.emit(metadata_event)

   #convert_record_to_datahub_format_job(record)


#handle_openlineage_record(test_json)

