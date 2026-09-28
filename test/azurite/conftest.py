"""Azurite test-scoped helpers. Seeding is now EAGER (see ../conftest.py `_populate`), so these tests
just verify the result — this fixture gives them the rclone Remote to read azurite back.
"""

import pytest

from ducktest.resources.azurite import rclone_remote
from ducktest.tools import rclone  # noqa: F401  (re-exported for tests importing from here)


@pytest.fixture(scope="session")
def azurite_remote(azurite):
    """rclone Remote for the active azurite (booted or attached) — for reading the seeded containers."""
    return rclone_remote(vars(azurite))
