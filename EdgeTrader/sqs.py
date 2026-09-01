"""
sqs.py — SQS client and message helpers for headless node processes.

Provides a cached boto3 SQS client and async functions to receive/delete messages,
and a helper to unwrap SNS notification envelopes.

These helpers are shared between all per‑target node processes (real and virtual).
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Optional

try:
    import boto3
except ImportError:
    print("❌ boto3 not installed. Run: pip install boto3", file=sys.stderr)
    sys.exit(1)


_sqs_client = None


def get_sqs_client() -> boto3.client:
    """Return a cached SQS client (singleton)."""
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client(
            "sqs",
            region_name=os.getenv("AWS_REGION", "ap-southeast-1")
        )
    return _sqs_client


async def receive_trade_events(
    sqs_client,
    queue_url: str,
    max_messages: int = 10,
    wait_seconds: int = 20,
) -> List[Dict[str, Any]]:
    """
    Long‑poll SQS for messages. Returns a list of raw message dicts,
    or an empty list on error.
    """
    loop = asyncio.get_running_loop()
    try:
        response = await loop.run_in_executor(
            None,
            lambda: sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_seconds,
                MessageAttributeNames=["All"],
            ),
        )
    except Exception as e:
        print(f"❌ SQS receive error: {e}", file=sys.stderr)
        return []
    return response.get("Messages", [])


async def delete_message(sqs_client, queue_url: str, receipt_handle: str) -> bool:
    """Delete a message from the queue by its receipt handle. Returns True on success."""
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle),
        )
        return True
    except Exception as e:
        print(f"❌ SQS delete error: {e}", file=sys.stderr)
        return False


def parse_event(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Unwrap an SQS message that may be wrapped in an SNS notification envelope.
    Returns the raw event dict, or None if the body cannot be parsed as JSON.
    """
    try:
        body = json.loads(msg["Body"])
        if body.get("Type") == "Notification":
            return json.loads(body["Message"])
        return body
    except (json.JSONDecodeError, KeyError):
        return None