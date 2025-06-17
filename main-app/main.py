import json
import os

from dapr.clients import DaprClient
from dapr.clients.grpc._request import TransactionalStateOperation, TransactionOperationType
from fastapi import FastAPI, HTTPException
import grpc

import logging


app = FastAPI()


logging.basicConfig(level=logging.INFO)

@app.post("/publish_event")
def publish_event():
    with DaprClient() as d:
        try:
            logging.info(f'sending event event: s3Emails')
            d.publish_event(pubsub_name="aws-sqs", topic_name="emails",
                            data=json.dumps({"key": "this is a message", "event_type": "s3Emails"}),
                            data_content_type="application/json")

            logging.info(f'sending event event: s3Emails')
        except grpc.RpcError as err:
            print(f"Error={err.details()}")
            raise HTTPException(status_code=500, detail=err.details())




