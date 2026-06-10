# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "boto3>=1.35",
# ]
# ///
"""Minimal NerveMQ example using boto3, the official AWS SDK for Python.

Run with uv (https://docs.astral.sh/uv/), which resolves the inline
dependencies above automatically:

    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... uv run example.py

Credentials are a NerveMQ API key (the queue is created in the key's
namespace), minted via the admin UI or the CLI:

    nervemq apikey add --name python-example --namespace <namespace>
"""

import os
import sys

import boto3

ENDPOINT_URL = os.environ.get("NERVEMQ_ENDPOINT", "http://localhost:8080/api/sqs")


def main():
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        sys.exit(
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to a NerveMQ "
            "API key (create one with: nervemq apikey add --name "
            "python-example --namespace <namespace>)"
        )

    sqs = boto3.client(
        "sqs",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-west-1",
        endpoint_url=ENDPOINT_URL,
    )

    try:
        res = sqs.get_queue_url(QueueName="bruh")
        url = res.get("QueueUrl")
    except sqs.exceptions.ClientError:
        res = sqs.create_queue(QueueName="bruh")
        url = res.get("QueueUrl")

    print(f"Queue URL: {url}")

    response = sqs.send_message(
        QueueUrl=url,
        MessageBody="Hello World!",
        MessageAttributes={
            "Test": {"StringValue": "TestString", "DataType": "String"}
        },
    )

    print(f"Message ID: {response.get('MessageId')}")

    response = sqs.receive_message(
        QueueUrl=url,
        AttributeNames=["All"],
        MessageAttributeNames=["Test"],
        MaxNumberOfMessages=10,
        VisibilityTimeout=0,
        WaitTimeSeconds=0,
        ReceiveRequestAttemptId="1",
    )

    print(f"Messages: {response.get('Messages')}")


if __name__ == "__main__":
    main()
