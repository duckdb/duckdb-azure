"""Azure test suites on ducktest.

- ``local`` (test/azurite/) — azurite-backed, smoke / DEFAULT. Eager azurite boots, seeds ``data/`` (the
  rclone equivalent of scripts/upload_test_files_to_azurite.sh), and adopts the azure connection env, so
  the bare ``.test`` bodies run with no manual env script.
- ``cloud`` (test/azure/) — real Azure (AZURE_* creds / ABFSS). Opt-in (``-m cloud``); the bodies'
  ``require-env`` gates them until creds are present (a ``credential`` wiring is future work).
- ``proxy`` (test/proxy/) — via squid. Opt-in, deferred (needs a squid service that ``depends_on`` azurite).

Suite/marker names are the ROLES (local/cloud/proxy); the directories are the backing (azurite/azure).
"""

import pathlib
import types

import pytest

from ducktest import provision_service, register_suite, use_service
from ducktest.resources.azurite import AZURITE_SERVICE, azurite_env, rclone_remote
from ducktest.tools import rclone

DATA = pathlib.Path(__file__).parent.parent / "data"  # repo data/
PRIVATE, PUBLIC, WRITES = (
    "testing-private",
    "testing-public",
    "writes",
)  # match the upload script


def _populate(block, config):
    """Structure + data (idempotent): create the containers, sync ``data/`` into the read ones."""
    remote = rclone_remote(block)
    for c in (PRIVATE, PUBLIC, WRITES):
        rclone.mkdir(remote, c)
    for c in (PRIVATE, PUBLIC):
        rclone.sync(str(DATA), remote, c)  # data/l.csv -> <c>/l.csv, etc.


def _azure_env(block):
    """The env a bare ``.test`` body needs: azurite's connection string/account + the data/temp dirs."""
    return {**azurite_env(block), "AZ_DATA_DIR": PRIVATE, "AZ_TEMP_DIR": WRITES}


def pytest_configure(config):
    register_suite(
        config,
        "local",
        path="test/azurite",
        marker="local",
        default=True,
        services=[
            use_service(
                AZURITE_SERVICE,
                to_env=_azure_env,
                populate=_populate,
            )
        ],
    )
    register_suite(config, "cloud", path="test/azure", marker="cloud", default=False)
    register_suite(config, "proxy", path="test/proxy", marker="proxy", default=False)


@pytest.fixture(scope="session")
def azurite(request):
    """The azurite block for a Python test (already eagerly booted — this returns the shared instance)."""
    return types.SimpleNamespace(**provision_service(request.config, AZURITE_SERVICE))
