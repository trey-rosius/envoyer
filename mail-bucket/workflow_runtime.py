# workflow_runtime.py
import json
import os
import re
from dapr_agents import tool, Agent
from time import sleep
from dapr.clients import DaprClient
import os, json
import asyncio
import shortuuid
from dapr_agents.workflow import WorkflowApp
from dapr.ext.workflow import (
    WorkflowRuntime,
    DaprWorkflowContext,
    WorkflowActivityContext,
    RetryPolicy,
    DaprWorkflowClient,
    when_any,
)
import logging

state_store = os.getenv('DAPR_STATE_STORE', '')
os.getenv('OPENAI_API_KEY',
          '')



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

# Create runtime instance
wfr = WorkflowRuntime()


# Register workflows and activities
@wfr.workflow(name="email_agent_workflow")
def email_agent_workflow(ctx: WorkflowActivityContext, email: any):
    email_str = yield ctx.call_activity(display_email_content, input=email)
    ai_email = yield ctx.call_activity(ai_transform_email, input={"email": email_str})
    payload = {"ai_email": ai_email, "original_email": email}
    merged_payload = yield ctx.call_activity(extract_and_merge, input=payload)
    result = yield ctx.call_activity(save_ai_email_content, input=merged_payload)
    return result


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

    state_store = os.getenv("DAPR_STATE_STORE", "")
    with DaprClient() as d:
        d.save_state(store_name=state_store,
                     key=f"{ai_email['to']}#{ai_email['messageId']}",
                     value=json.dumps(ai_email),
                     state_metadata={"contentType": "application/json"})
    return "email saved"


@wfr.activity(name="ai_transform_email")
def ai_transform_email(ctx, email: dict):
    json_email = json.dumps(email)
    email_agent = Agent(
        name="EmailAgent",
        role="Email Assistant",
        instructions=[EMAIL_SYSTEM_PROMPT],
        tools=[],
    )

    response = asyncio.run(email_agent.run(json_email))
    logging.info(f' ai email response is : %s:' % {response})

    return response
