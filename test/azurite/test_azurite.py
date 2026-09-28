"""The dumbest Azurite trigger: prove the service fixture yields a usable block.

    uv run pytest test/azurite -s                                   # BOOTS azurite, runs, stops it
    uv run pytest test/azurite -s --existing-service azurite=http://127.0.0.1:10000/   # ATTACHES
    DUCKTEST_EXISTING_SERVICE_AZURITE=1 uv run pytest test/azurite -s                   # ATTACHES (env)

The assertions are identical across all three — only timing + the `attached` flag differ.
"""


def test_azurite_reachable(azurite):
    assert azurite.account == "devstoreaccount1"
    assert azurite.blob_endpoint.endswith("/devstoreaccount1")
    assert "BlobEndpoint=" in azurite.connection_string
    print(f"\nazurite endpoint={azurite.endpoint} attached={getattr(azurite, 'attached', False)}")
