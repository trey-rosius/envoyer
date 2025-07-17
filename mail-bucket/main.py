import json
import os
import re
from contextlib import asynccontextmanager
from time import sleep
import asyncio
import shortuuid

from dapr_agents import tool, Agent
from dotenv import load_dotenv
from dapr.clients import DaprClient
import dapr.ext.workflow as wf
from dapr_agents.workflow import WorkflowApp, workflow, task

from dapr.clients.grpc._request import TransactionalStateOperation, TransactionOperationType
from fastapi import FastAPI, HTTPException
from models.cloud_events import CloudEvent, Event
import grpc

import logging

state_store = os.getenv('DAPR_STATE_STORE', '')
os.getenv('OPENAI_API_KEY',
          '')

# app = FastAPI()

key_uuid = shortuuid.uuid()

EMAIL_SYSTEM_PROMPT = """
You are an **AI Email Intelligence Assistant**. You can:

1. Parse RFC-822 e-mail objects that have already been converted to JSON
   (headers, plain_body, html_body, attachments, etc.).
2. Produce **succinct, user-friendly summaries** of the message body.
3. Assign the message to one **category**:
   “Work”, “Personal”, “Finance”, “Marketing / Promotions”,
   “Social”, “Spam”, “Travel”, “Receipts”, or “Other”.
4. Perform **sentiment analysis** of the sender’s tone
   (Positive, Neutral, Negative, or Mixed).
5. Extract and surface **key information**:
   • dates & times
   • monetary amounts & currencies
   • action items / requests
   • named people & organisations
   • links & attachment filenames
   • reply-by / due-by hints
6. Flag messages that are **urgent** (requesting immediate action,
   containing deadlines < 48 h, critical alert words, etc.).
7. Return a single JSON object that the calling service can store in DynamoDB.

────────────────────
### Input
You receive a JSON payload with at least these keys:

{
  "from":        "<display name & email>",
  "to":          "<comma-separated list>",
  "cc":          "<nullable>",
  "subject":     "<string>",
  "date":        "<RFC-2822 timestamp>",
  "plain_body":  "<string>",        // canonical source for NLP
  "html_body":   "<string|null>",
  "attachments": [
    { "filename": "...", "s3_key": "..." }
  ]
}

────────────────────
### Output
Respond **only** with valid JSON using this schema:

{
  "summary":        "<≤ 60 words>",
  "category":       "<one term from list above>",
  "sentiment":      "<Positive|Neutral|Negative|Mixed>",
  "is_urgent":      <true|false>,
  "key_dates":      ["<ISO-8601>", ...],
  "amounts":        ["<100 USD>", ...],
  "action_items":   ["<string>", ...],
  "entities":       ["<Acme Corp>", "<John Doe>", ...],
  "links":          ["https://...", ...],
  "attachments":    ["invoice.pdf", "photo.jpg"]
}

────────────────────
### Processing guidelines
1. **Prefer `plain_body`** for analysis; fall back to stripped `html_body`
   if needed.
2. When multiple addresses appear in *To* or *Cc*, list them all in extracted
   entities but pick the first address as the primary “user”.
3. Redact PII in the summary if the message is obviously spam or phishing.
4. For monetary amounts, capture the **original currency symbol or code**.
5. Treat emoji or casual language as sentiment clues (🙂 → positive, 😡 → negative).
6. If no meaningful items exist for a field (`key_dates`, `amounts`, …)
   return an **empty array**, not `null`.
7. Never hallucinate facts; base every extraction on explicit text in the e-mail.

────────────────────
### Error handling
If the body is empty or unparsable, return:

{ "error": "EmptyMessage" }

Always strive for precise, context-aware extractions and compact, actionable summaries.
"""

logging.basicConfig(level=logging.INFO)

# Initialize Workflow Instance
wfr = wf.WorkflowRuntime()

user_emails = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    wfr.start()  # ✅ Start the Dapr Workflow Runtime once
    yield
    # Shutdown code (optional cleanup here)


app = FastAPI(lifespan=lifespan)


# Define Workflow logic
@wfr.workflow(name="email_agent_workflow")
def email_agent_workflow(ctx: wf.DaprWorkflowContext, email: any):
    # parse_email_str = email.model_dump_json()
    email_str = yield ctx.call_activity(display_email_content, input=email)
    ai_email = yield ctx.call_activity(ai_transform_email, input={"email": email_str})
    payload = {
        "ai_email": ai_email,
        "original_email": email
    }
    merged_payload = yield ctx.call_activity(extract_and_merge, input=payload)
    result = yield ctx.call_activity(save_ai_email_content, input=merged_payload)

    return result


# Activity 1
@wfr.activity(name="display_email_content")
def display_email_content(ctx, parse_email: str):
    return parse_email


@wfr.activity(name="extract_and_merge")
def extract_and_merge(ctx, email_payload: dict):
    import json, re, logging

    ai_email_insight = email_payload['ai_email']
    original_email = email_payload['original_email']
    logging.info(f"ai_email_insight: {ai_email_insight}")

    match = re.search(r"```json\n(.*?)\n```", ai_email_insight, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:

        json_str = ai_email_insight.strip()

    try:
        response_dict = json.loads(json_str)
        logging.info("AI email response (as dict): %s", response_dict)
    except json.JSONDecodeError as e:
        logging.error("Failed to decode JSON: %s", e)
        response_dict = {}

    # Merge into original email
    merged_email = {
        **original_email,
        "ai_insight": response_dict
    }

    return merged_email


@wfr.activity(name="save_ai_email_content")
def save_ai_email_content(ctx, ai_email: dict):
    with DaprClient() as d:
        email_id = f"{ai_email['to']}#{ai_email['messageId']}"
        d.save_state(store_name=state_store,
                     key=email_id, value=json.dumps(ai_email),
                     state_metadata={"contentType": "application/json"})
        logging.info(f'Save state item successful with key {str(key_uuid)}')
        return "email saved"


@wfr.activity(name="ai_transform_email")
def ai_transform_email(ctx, email: dict):

    logging.info(f' ai email response is : %s:')

    return "inside the ai response"


@app.get('/get_all_emails')
def get_all_emails():
    with DaprClient() as d:
        query_dict = {
            "filter": {
                "EQ": {"to": "rosius@846agents.com"}
            },
            "sort": [
                {
                    "key": "date",
                    "order": "DESC"
                }
            ]
        }

        query = json.dumps(query_dict)
        kv = d.query_state(store_name=state_store, query=query)

        logging.info(f"response is {kv.results}")
        print(f"packages are {kv.results}")

        for item in kv.results:
            logging.info(f"item is {json.loads(item.value)}")
            user_emails.append(json.loads(item.value))

        return user_emails

@app.post('/emails')
async def emails(event:CloudEvent):
    with DaprClient() as d:

        print("event triggered")
        parsed_email_payload = event.data['payload']
        wf_client = wf.DaprWorkflowClient()
        instance_id = wf_client.schedule_new_workflow(workflow=email_agent_workflow,
                                                      input=parsed_email_payload)
        print(f"Workflow started. Instance ID: {instance_id}")
        state = wf_client.wait_for_workflow_completion(instance_id)
        logging.info(f"state is {state.runtime_status}")

'''
@app.post('/emails')
async def emails(event: CloudEvent):
    with DaprClient() as d:
        logging.info(f'Received event: ')

        parsed_email_payload = event.data['payload']
        try:
            #wfr.start()
            #sleep(5)  # wait for workflow runtime to start

            wf_client = wf.DaprWorkflowClient()
            instance_id = wf_client.schedule_new_workflow(workflow=email_agent_workflow,
                                                          input=parsed_email_payload)
            print(f"Workflow started. Instance ID: {instance_id}")
            state = wf_client.wait_for_workflow_completion(instance_id)

            print(f"state information: {state}")

            print(f"Workflow completed! Status: {state.runtime_status}")



        except grpc.RpcError as err:
            logging.info('Error occurred while saving state item %s. Exception= %')
            raise HTTPException(status_code=500, detail=err.details())
'''