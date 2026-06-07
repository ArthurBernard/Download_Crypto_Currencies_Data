"""Operation Registry — maps operation names to schemas and callables."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

__all__ = ["OperationRegistry", "OperationSpec", "REGISTRY"]


class OperationSpec(BaseModel):
    """Metadata for one registered operation."""

    name: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]


class OperationRegistry:
    """Maps operation names to their specs.

    Used by the test-of-parity check and interface bindings.
    """

    def __init__(self) -> None:
        self._ops: dict[str, OperationSpec] = {}

    def register(self, spec: OperationSpec) -> None:
        """Register an operation spec."""
        self._ops[spec.name] = spec

    def get(self, name: str) -> OperationSpec:
        """Return the spec for *name* (raises KeyError if unknown)."""
        if name not in self._ops:
            raise KeyError(f"Unknown operation: {name!r}")
        return self._ops[name]

    @property
    def operations(self) -> list[str]:
        """Names of all registered operations."""
        return list(self._ops.keys())


REGISTRY = OperationRegistry()

REGISTRY.register(OperationSpec(
    name="backfill",
    description="Download historical data to the local store.",
    input_schema={
        "exchange": "str",
        "symbol": "str",
        "data_type": "str",
        "span": "int | None",
        "start": "str | int",
    },
    output_schema={"run_id": "str", "rows_written": "int"},
))

REGISTRY.register(OperationSpec(
    name="stream",
    description="Stream live data continuously.",
    input_schema={
        "exchange": "str",
        "symbol": "str",
        "data_type": "str",
        "span": "int | None",
        "depth": "int | None",
        "snapshot_interval": "int | None",
    },
    output_schema={"run_id": "str"},
))

REGISTRY.register(OperationSpec(
    name="read",
    description="Read stored data for a dataset.",
    input_schema={
        "exchange": "str",
        "symbol": "str",
        "data_type": "str",
        "span": "int | None",
        "start_ns": "int | None",
        "end_ns": "int | None",
    },
    output_schema={"rows": "int", "data": "list"},
))

REGISTRY.register(OperationSpec(
    name="inventory",
    description="List all stored datasets.",
    input_schema={},
    output_schema={"datasets": "list"},
))
