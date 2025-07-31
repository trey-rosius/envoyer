import json
import os
from dapr.ext.workflow import DaprWorkflowClient
from dapr.clients import DaprClient
from dapr.clients.grpc._request import TransactionalStateOperation, TransactionOperationType
from fastapi import FastAPI, HTTPException
import grpc
from typing import Dict, Any
import logging

app = FastAPI()

logging.basicConfig(level=logging.INFO)
pub_sub = os.getenv('DAPR_PUB_SUB', 'aws-sqs')

@app.post("/publish_event")
def publish_event(payload: Dict[str, Any]):
    with DaprClient() as d:
        try:
            logging.info(f'sending event event: s3Emails')

            d.publish_event(pubsub_name=pub_sub, topic_name="emails",
                            data=json.dumps({"payload": payload, "event_type": "s3Emails"}),
                            data_content_type="application/json")

            logging.info(f'sending event event: s3Emails')
        except grpc.RpcError as err:
            print(f"Error={err.details()}")
            raise HTTPException(status_code=500, detail=err.details())
