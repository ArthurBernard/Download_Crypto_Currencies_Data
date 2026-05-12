#!/usr/bin/env python3
# coding: utf-8

import os

import pytest


def pytest_collection_modifyitems(items):
    if os.getenv("CI"):
        skip = pytest.mark.skip(reason="network tests skipped in CI (mock in progress)")
        for item in items:
            item.add_marker(skip)
