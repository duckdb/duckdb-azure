"""Verify EAGER azurite provisioning: on suite-selection the driver booted azurite, populated `data/`
(the rclone equivalent of scripts/upload_test_files_to_azurite.sh), and adopted the azure env — all
before any test ran, with no fixture pulled.

    uv run pytest test/azurite -s              # BOOTS azurite eagerly, populates, adopts env
    uv run pytest test/azurite --existing-service azurite=http://127.0.0.1:10000/   # ATTACH + populate
"""

import os

from ducktest.tools import rclone


def _names(listing):
    return {line.split(maxsplit=1)[1] for line in listing.splitlines() if line.strip()}


def test_env_adopted_for_bare_test_bodies():
    # eager to_env landed in os.environ pre-fork — this is what a bare `.test` require-env sees
    assert os.environ["AZ_DATA_DIR"] == "testing-private"
    assert os.environ["AZ_STORAGE_ACCOUNT"] == "devstoreaccount1"
    assert "BlobEndpoint=" in os.environ["AZURE_STORAGE_CONNECTION_STRING"]


def test_data_populated_to_private(azurite_remote):
    names = _names(rclone.ls(azurite_remote, "testing-private"))
    assert "l.parquet" in names
    assert "lineitem.csv" in names
    assert any(n.startswith("partitioned/") for n in names)


def test_public_container_also_populated(azurite_remote):
    assert "l.parquet" in _names(rclone.ls(azurite_remote, "testing-public"))
