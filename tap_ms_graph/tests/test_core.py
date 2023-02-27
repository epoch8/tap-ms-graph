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


# TODO: Create additional tests as appropriate for your tap.
