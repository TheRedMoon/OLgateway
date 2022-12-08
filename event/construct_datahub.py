import datetime
from uuid import uuid4

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass, DataJobKeyClass

from datahub.emitter.rest_emitter import DatahubRestEmitter


### USED TO DIRECTLY TEST THE DATAHUB API

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://localhost:8080", extra_headers={})

# Test the connection
emitter.test_connection()

# Construct a dataset properties object
## GO INTO SCHEMA_CLASSES TO SEE WHAT THERE IS
dataset_properties = DatasetPropertiesClass(description="This table stored the canonical User profile",
    customProperties={
         "governance": "ENABLED"
    })


# Construct a MetadataChangeProposalWrapper object.

#aspect event
metadata_event = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_dataset_urn("mendix2", "my-project.my-dataset.user-table2"),
    aspectName="datasetProperties",
    aspect=dataset_properties,
)

#does not work, implies needing aspects.
metadata_event_simple = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_dataset_urn("mendix2", "my-project.my-dataset.user-table-3"),
)


id = str(uuid4())
flow_id = "well" #datetime.datetime.now()




#https://datahubproject.io/docs/graphql/objects/#datajob
# metadata_event_job = MetadataChangeProposalWrapper(
#     entityType="datajob", #https://datahubproject.io/docs/graphql/enums/#entitytype entity types
#     entityUrn=builder.make_data_job_urn("Event grid", f"{flow_id}", id),
# )



# Emit metadata! This is a blocking call
emitter.emit(metadata_event_job)
#emitter.emit(metadata_event_simple)