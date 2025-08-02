 # Dapr AI Hackathon - Project Submission Template

Welcome to the official project submission repository template for the **Dapr AI Hackathon x Diagrid**! This GitHub template repository is designed to help you structure and submit your hackathon project efficiently.

## About This Template

This repository is set up as a GitHub template that you can use as a starting point for your hackathon project. By using this template, you'll:

1. Start with the recommended project structure
2. Have clear documentation guidelines
3. Ensure your submission meets all requirements
4. Make it easier for judges to evaluate your project

## About the Hackathon

The Dapr AI Hackathon (May 16 - June 27, 2025) is a global challenge to build intelligent, resilient, and scalable AI applications using the power of Dapr Workflows and Dapr Agents. Whether you're orchestrating autonomous agents, designing durable AI pipelines, or ensuring responsible AI behavior, this hackathon is your opportunity to push the boundaries of distributed intelligence. To learn more, visit: [https://github.com/diagrid-labs/dapr-ai-hackathon](https://github.com/diagrid-labs/dapr-ai-hackathon).

## Getting Started with This Template

> [!TIP]
> Feel free to adapt this structure to your project's specific needs. The most important thing is to have a clear, organized structure with adequate documentation that meets the submission criteria.

To use this template for your Dapr AI Hackathon submission:

1. **Create a new repository from this template**: Click the "Use this template" button at the top of this repository
1. **Name your new repository**: Choose a name related to your project
1. **Clone your new repository**: Clone the repository to your local machine
1. **Provide Hackathon organizer's access**: Add @kendallroden on GitHub to access your project template
1. **Update this README.md**: Replace the content in the "Project Details" section with your project information as you iterate
1. **Add your project code**: Implement your hackathon project in this repository
1. **Submit your project**: Open an issue in the [official Dapr AI Hackathon repository](https://github.com/diagrid-labs/dapr-ai-hackathon/issues/new/choose) using the Project Submission template and include a link to your private repository

## Project Details

### App Ids
This project has 2 catalyst Applications
- **mail-bucket** 
- **main-app**

### Components
- **ai-email-db** is a mongoDb database for state management.
- **aws-sqs** is an sqs/sns aws service for pubsub

### PubSub Subscriptions
- **mailsubscriptions** has a topic called `emails`. AppIds subscribed to that topic receive events 
when a new email arrives.

### Agentic Workflow
When AWS SES receives a new email, it stores a copy of that email in an S3 bucket and then triggers a 
lambda function. Within this lambda function is an apprunner endpoint for our `main-app` catalyst service. 

This endpoint has an extension called `publish_events`. This endpoint pushes the details of the email as
an event to an the pubsub service `aws-sqs`. 

Subscribed to that service is the `mail-bucket` application. 

The endpoint `/emails` receives the event and then invokes the agentic workflow

```python
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

```
I added a hack to only run this workflow once per event received. Currently, workflows have a bug which 
causes the workflow to be triggered multiple times and sometimes the workflows get stuck and fail. 

This discord thread outlines all the issues i faced https://discord.com/channels/1255285156739285114/1383062057045458985

Due to the numerous problems with workflows, i couldn't add different steps to my workflow like saving 
to a vector database and connecting to gmail calendar for enhanced email features. 

I spent a lot of time fighting catalyst than being productive.

### 🚀 Project Name

Envoyer: An AI Email Ops Client for AWS SES Customers

### 📝 Summary

I work with teams who send/receive a high volume of emails via AWS Simple Email Service (SES). They all share the same pain: inbox chaos translates directly into missed leads, slower support, and lost revenue. 

While AWS SES is exceptionally reliable and cost-effective for sending and receiving, the last mile—triaging, understanding, and acting on messages—still depends on humans reading every thread.

I built an AI Email Ops Agent that sits next to SES to summarize, classify, prioritize, and propose actions (reply templates, routing, or escalation) the moment an email lands, so teams can move from “read and react” to “route and resolve.”

### 🏆 Category

Choose one of the following solution categories:

- Collaborative Intelligence
- Workflow Resilience
- Distributed Architecture
- Responsible AI

### 💻 Technology Used

- **Platform**: [Catalyst]
- **Dapr APIs**: [Workflow API, Pub/Sub, State,)]
- **Programming Languages**: [Python, Typescript]
- **Additional Technologies**: [AWS CDK, AWS Lambda, AWS S3, AWS SES]

### 📋 Project Features

- Invoke an agentic workflow for each email received through AWS SES

- Summarize the email in 1–3 sentences (who/what/when/urgency).

- Classify into business categories (Sales, Support, Billing, Marketing/Promos, Spam, Other).

- Extract critical entities (dates, amounts, contacts, order IDs, SLAs, sentiment).

- Prioritize (P0–P3) using rules + signals (VIP sender, negative sentiment, due date).


### 🏗️ Architecture

![solutions_architecture](assets/solutions_arch.png)

The application starts when an email is sent to this SES Inbound Email Address(`rosius@846agents.com`).

AWS SES saves the email in an S3 bucket. S3 triggers a lambda function. This lambda function grabs the email, 
extracts it's content and sends a message into an SQS queue through the Catalyst endpoint hosted in AWS Apprunner.

A subscribed service receives the event, summarizes, classifies.... and saves the information into a mongodb table.

We also have an aws appsync endpoint to help us retrieve emails from mongodb and also send emails.

### 🎬 Demo

https://youtu.be/xAlz16Oa1gE

## Installation & Deployment Instructions

### Prerequisites
- AWS CLI
- AWS Credentials
- AWS CDK CLI
- Docker
- Python 3.11.6
- Create and Setup a domain DKIM(https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html#verify-domain-procedure)

### Additional Set-Up
#### Step 1
Update the OpenAI Key and run the application like a catalyst app. 

Push the catalyst application to AWS ECR and then AppRunner.

Navigate to the `email_cdk_app` inside the `email_cdk_app-stack.ts` file, replace the domain name with 
your verified domain name from AWS SES

```typescript
  const hostedZone = route53.HostedZone.fromLookup(this, "Zone", {
      domainName: "REPLACE_WITH_YOUR_DOMAIN",
    });
```
#### Step 2

Navigate the the `api-contruct.ts` file and replace the endpoint with the Apprunner endpoint for the
`mail-bucket` service.
```ts
    const emailServiceAPIDatasource = this.api.addHttpDataSource(
      "MailBucketService",
      "https://xxxxxxxxxxx.us-east-1.awsapprunner.com"
    );
```

#### Step 3
Navigate to the `index.py` file in the directory `lambda/email-processor` and update the endpoint on line
89
```pycon

  logger.info("Parsed e-mail", email_metadata=out)

        url = "https://xxxxxxx.us-east-1.awsapprunner.com/publish_event"

        headers = {"Content-Type": "application/json"}
```

#### Step 4

Synthesize and deploy the application 

```bash
cdk synth
cdk bootstrap
cdk deploy


```

## Application Frontend 
https://main.d1twti1ffo94b8.amplifyapp.com/

## Team Members

- Me

## License

No License
