from fastapi import FastAPI, HTTPException
from dapr.clients import DaprClient
from dapr.ext.workflow import DaprWorkflowClient
from models.cloud_events import CloudEvent
import os, json, logging, hashlib, re

from workflow_runtime import email_agent_workflow

app = FastAPI()
state_store = os.getenv('DAPR_STATE_STORE', '')
os.getenv('OPENAI_API_KEY',
          '')
logging.basicConfig(level=logging.INFO)


@app.get('/get_all_emails')
def get_all_emails():
    with DaprClient() as d:
        query = json.dumps({
            "filter": {"EQ": {"to": "rosius@846agents.com"}},
            "sort": [{"key": "date", "order": "DESC"}]
        })
        kv = d.query_state(store_name=state_store, query=query)
        emails = [
            json.loads(item.value.decode('utf-8') if isinstance(item.value, (bytes, bytearray)) else item.value)
            for item in kv.results
        ]
        return emails


def _sanitize_mid(mid: str) -> str:
    """
    Gmail-style Message-IDs include angle brackets and '@'.
    We don't rely on the raw string as an ID; we hash it so it's short and safe.
    """
    return hashlib.sha256(mid.encode("utf-8")).hexdigest()[:32]


def _stable_instance_id_from_payload(payload: dict, cloud_event: CloudEvent | None = None) -> str:
    # Prefer the email's Message-ID for dedup across transports/sources
    mid = payload.get("messageId") or payload.get("message_id") or payload.get("MessageId")
    if isinstance(mid, str) and mid.strip():
        digest = _sanitize_mid(mid.strip())
        return f"email-agent-{digest}"

    # Fallbacks: CloudEvent id, then canonicalized payload hash
    ce_id = getattr(cloud_event, "id", None)
    if ce_id:
        return f"email-agent-{hashlib.sha256(str(ce_id).encode('utf-8')).hexdigest()[:32]}"

    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"email-agent-{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:32]}"


@app.post('/emails')
def emails(event: CloudEvent):

    try:
        payload = event.data['payload']
        if not isinstance(payload, dict):
            raise ValueError("data.payload must be an object")
    except Exception as e:
        logging.error(f"Malformed event payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid event: missing or bad data.payload")

    instance_id = _stable_instance_id_from_payload(payload, event)

    try:
        wf = DaprWorkflowClient()

        # Idempotent start: duplicates will hit "already exists / conflict"
        wf.schedule_new_workflow(
            workflow=email_agent_workflow,
            input=payload,
            instance_id=instance_id
        )
        logging.info(f"Workflow start requested. instance_id={instance_id}")

        # Optionally block for visibility (keep if you want sync behavior)
        wf.wait_for_workflow_start(instance_id)
        state = wf.wait_for_workflow_completion(instance_id)
        return {"instance_id": instance_id, "status": state.runtime_status}

    except Exception as err:
        msg = str(err).lower()
        if any(sig in msg for sig in ["already exists", "instance already started", "conflict"]):
            # Duplicate delivery: return the existing instance
            logging.info(f"Duplicate detected; returning existing workflow state for {instance_id}")
            try:
                wf = DaprWorkflowClient()
                wf.wait_for_workflow_start(instance_id)
                state = wf.wait_for_workflow_completion(instance_id)
                return {"instance_id": instance_id, "status": state.runtime_status}
            except Exception as inner:
                logging.error(f"Failed to read existing instance {instance_id}: {inner}")
                raise HTTPException(status_code=500, detail=f"Existing workflow fetch failed: {inner}")

        logging.error(f"Error scheduling workflow: {err}")
        raise HTTPException(status_code=500, detail=str(err))
