"""Tests standard tap features using the built-in SDK tests library."""

import datetime

import json
from singer_sdk.testing import get_standard_tap_tests

from tap_ms_graph.tap import TapMSGraph

with open(".secrets/config.json", "r") as f:
    CONFIG = json.loads(f.read())


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapMSGraph,
        config=CONFIG
    )
    for test in tests:
        test()


def test_user_id_in_schema():
    tap = TapMSGraph(config=CONFIG, parse_env_config=True)
    for name, stream in tap.streams.items():
        if name in ["user_events", "user_messages"]:
            schema_props = set(stream.schema["properties"].keys())
            assert "user_id" in schema_props
        elif name == "users":
            users_stream = stream

    
    users_stream._MAX_RECORDS_LIMIT = 1
    records = users_stream.get_records(None)
    user_id = next(records)["id"]
    for child_stream in users_stream.child_streams:
        child_stream._MAX_RECORDS_LIMIT = 1
        child_context = {"user_id": user_id}
        records = child_stream.get_records(child_context)
        records = [record for record in records]
