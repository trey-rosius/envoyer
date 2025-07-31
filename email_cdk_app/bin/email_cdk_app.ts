#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { EmailCdkAppStack } from '../lib/email_cdk_app-stack';

const app = new cdk.App();
new EmailCdkAppStack(app, 'EmailCdkAppStack', {
   env: { account: "132260253285", region: "us-east-2" },
});