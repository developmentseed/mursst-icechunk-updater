import boto3
import json
import os
from config import QUEUE_NAME, QUEUE_ARN_FILE

def create_sqs_queue():
    # Initialize AWS clients
    sqs = boto3.client('sqs')
    sts = boto3.client('sts')
    
    # Get AWS account ID and region
    aws_account_id = sts.get_caller_identity()['Account']
    
    try:
        # Create the queue
        response = sqs.create_queue(
            QueueName=QUEUE_NAME
        )
        queue_url = response['QueueUrl']
        
        # Get queue ARN
        queue_attributes = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )
        queue_arn = queue_attributes['Attributes']['QueueArn']
        
        # Create queue policy
        queue_policy = {
            "Version": "2012-10-17",
            "Id": "__default_policy_ID",
            "Statement": [
                {
                    "Sid": "__owner_statement",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": f"arn:aws:iam::{aws_account_id}:root"
                    },
                    "Action": [
                        "SQS:*"
                    ],
                    "Resource": queue_arn
                },
                {
                    "Sid": "AllowSNSPublish",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "sns.amazonaws.com"
                    },
                    "Action": "SQS:SendMessage",
                    "Resource": queue_arn
                }
            ]
        }
        
        # Set queue policy
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                'Policy': json.dumps(queue_policy)
            }
        )
        
        # Save queue ARN to file
        with open(QUEUE_ARN_FILE, 'w') as f:
            f.write(queue_arn)
        
        print(f"Successfully created queue: {QUEUE_NAME}")
        print(f"Queue URL: {queue_url}")
        print(f"Queue ARN: {queue_arn}")
        print(f"Queue ARN saved to {QUEUE_ARN_FILE}")
        
    except Exception as e:
        print(f"Error creating queue: {str(e)}")

if __name__ == "__main__":
    create_sqs_queue() 