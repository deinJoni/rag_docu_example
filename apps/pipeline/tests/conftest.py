"""Shared test config and fixtures.

Exposes ``FIXTURES_DIR`` so per-format tests can reach the binary fixtures
in ``tests/fixtures/`` without hard-coding paths.
"""

from __future__ import annotations

from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"
