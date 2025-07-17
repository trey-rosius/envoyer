
from fastapi import FastAPI, HTTPException
from dapr.clients import DaprClient
from dapr.ext.workflow import DaprWorkflowClient
from models.cloud_events import CloudEvent
import os, json, logging
from workflow_runtime import email_agent_workflow
app = FastAPI()
state_store = os.getenv('DAPR_STATE_STORE', '')
user_emails = []

@app.get('/get_all_emails')
def get_all_emails():
    with DaprClient() as d:
        query = json.dumps({
            "filter": {"EQ": {"to": "rosius@846agents.com"}},
            "sort": [{"key": "date", "order": "DESC"}]
        })
        kv = d.query_state(store_name=state_store, query=query)
        emails = [json.loads(item.value) for item in kv.results]
        return emails

@app.post('/emails')
async def emails(event: CloudEvent):
    parsed_email_payload = event.data['payload']
    try:
        wf_client = DaprWorkflowClient()
        instance_id = wf_client.schedule_new_workflow(
            workflow=email_agent_workflow,
            input=parsed_email_payload
        )
        logging.info(f"Workflow started. Instance ID: {instance_id}")
        state = wf_client.wait_for_workflow_completion(instance_id)
        return {"status": state.runtime_status}
    except Exception as err:
        logging.error(f"Error scheduling workflow: {err}")
        raise HTTPException(status_code=500, detail=str(err))
