import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create an emitter to DataHub over REST
emitter = DatahubRestEmitter(gms_server="http://127.0.0.1:5000", extra_headers={})

# Test the connection
#emitter.test_connection()

# Construct a dataset properties object
dataset_properties = DatasetPropertiesClass(description="This table stored the canonical User profile",
    customProperties={
         "governance": "ENABLED"
    })

# Construct a MetadataChangeProposalWrapper object.

#translate openlineage to this format

#aspect event
metadata_event = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_dataset_urn("mendix", "my-project.my-dataset.user-table"),
    aspectName="datasetProperties",
    aspect=dataset_properties,
)


#simple event
metadata_event_simple = MetadataChangeProposalWrapper(
    entityType="datajob",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=builder.make_data_job_urn("mendix", "my-project.my-dataset.user-table"),
)

# Emit metadata! This is a blocking call
#emitter.emit(metadata_event)
emitter.emit(metadata_event_simple)