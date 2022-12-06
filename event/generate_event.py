from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.client import OpenLineageClient
from datetime import datetime
from uuid import uuid4
import logging


def generate_event():
    client = OpenLineageClient()


    #translate to https://datahubproject.io/docs/metadata-modeling/metadata-model/ /

    #data flow ? , https://datahubproject.io/docs/generated/metamodel/entities/dataflow
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)

    #data set: https://datahubproject.io/docs/generated/metamodel/entities/dataset /https://datahubproject.io/docs/generated/metamodel/entities/dataplatform

    inventory = Dataset(namespace="food_delivery", name ="public.inventory")
    menus = Dataset(namespace="food_delivery", name ="public.menus_1")
    orders = Dataset(namespace="food_delivery", name ="public.orders_1")

    # job event: https://datahubproject.io/docs/generated/metamodel/entities/datajob
    job = Job(namespace="food_delivery", name ="example.order_data")
    # client.emit(
    #     RunEvent(
    #         RunState.START,
    #         "2021-11-03T10:53:52.427343",
    #         Run("69f4acab-b87d-4fc0-b27b-8ea950370ff3"),
    #         Job("openlineage", "job"),
    #         "producer"
    #     )
    # )
    producer = "producer"

    run = Run(str(uuid4()))

    client.emit(
        RunEvent(
            RunState.START,
            datetime.now().isoformat(),
            run, job, producer
        )
    )

    client.emit(
        RunEvent(
            RunState.COMPLETE,
            datetime.now().isoformat(),
            run, job, producer,
            inputs=[inventory],
            outputs=[menus, orders],
        )
    )

generate_event()