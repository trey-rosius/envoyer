# workflow_runtime.py

from dapr_agents.workflow import WorkflowApp
from dapr.ext.workflow import (
    WorkflowRuntime,
    DaprWorkflowContext,
    WorkflowActivityContext,
    RetryPolicy,
    DaprWorkflowClient,
    when_any,
)
# Create runtime instance
wfr = WorkflowRuntime()


# Register workflows and activities
@wfr.workflow(name="email_agent_workflow")
def email_agent_workflow(ctx: WorkflowActivityContext, email: any):
    email_str = yield ctx.call_activity(display_email_content, input=email)
    ai_email = yield ctx.call_activity(ai_transform_email, input={"email": email_str})
    #payload = {"ai_email": ai_email, "original_email": email}
    #merged_payload = yield ctx.call_activity(extract_and_merge, input=payload)
    #result = yield ctx.call_activity(save_ai_email_content, input=merged_payload)
    return ai_email


@wfr.activity(name="display_email_content")
def display_email_content(ctx, parse_email: str):
    return parse_email


@wfr.activity(name="extract_and_merge")
def extract_and_merge(ctx, email_payload: dict):
    import json, re, logging
    match = re.search(r"```json\n(.*?)\n```", email_payload['ai_email'], re.DOTALL)
    json_str = match.group(1) if match else email_payload['ai_email'].strip()

    try:
        response_dict = json.loads(json_str)
    except json.JSONDecodeError:
        response_dict = {}

    merged = {**email_payload['original_email'], "ai_insight": response_dict}
    return merged


@wfr.activity(name="save_ai_email_content")
def save_ai_email_content(ctx, ai_email: dict):
    from dapr.clients import DaprClient
    import os, json
    state_store = os.getenv("DAPR_STATE_STORE", "")
    with DaprClient() as d:
        d.save_state(store_name=state_store,
                     key=f"{ai_email['to']}#{ai_email['messageId']}",
                     value=json.dumps(ai_email),
                     state_metadata={"contentType": "application/json"})
    return "email saved"


@wfr.activity(name="ai_transform_email")
def ai_transform_email(ctx, email: dict):
    return "inside the ai response"
