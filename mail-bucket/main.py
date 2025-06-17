import json
import os

from dapr.clients import DaprClient
from dapr.clients.grpc._request import TransactionalStateOperation, TransactionOperationType
from fastapi import FastAPI, HTTPException
from models.cloud_events import CloudEvent,Event
import grpc

import logging


app = FastAPI()


logging.basicConfig(level=logging.INFO)


@app.post('/emails')
def emails(event: CloudEvent):
    with DaprClient() as d:

        logging.info(f'Received event: %s:' % {event.model_dump_json()})


@app.post('/subscription-emails')
def subscribe_emails(event: CloudEvent):
    with DaprClient() as d:

        logging.info(f'Received event: %s:' % {event.model_dump_json()})

@app.post('/emails')
def default_subscribe(event: Event):
    with DaprClient() as d:
        d.publish_event(
            pubsub_name="aws-sqs",
            topic_name="emails",
            data=json.dumps(event),
            data_content_type='application/json',
        )

        logging.info(f' event sent: %s:' % {event.model_dump_json()})
