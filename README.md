# DuckDB Azure Extension

This extension adds a filesystem abstraction for Azure blob storage to DuckDB. To use it, install latest DuckDB. The extension currently supports only **reads** and **globs**.

When debugging issues, especially authentication, start by adding the environment variable `AZURE_LOG_LEVEL=verbose` to duckdb.

## Basics

Setup authentication (leverages either Azure CLI or Managed Identity):

```sql
CREATE SECRET secret1 (
    TYPE AZURE,
    PROVIDER CREDENTIAL_CHAIN,
    ACCOUNT_NAME '⟨storage account name⟩'
);
```

Then to query a file on azure:

```sql
SELECT count(*) FROM 'az://<my_container>/<my_file>.<parquet_or_csv>';
```

Globbing is also supported:

```sql
SELECT count(*) FROM 'az://dummy_container/*.csv';
```

## Other authentication methods

Other authentication options available:

### Connection string

```sql
CREATE SECRET secret2 (
    TYPE AZURE,
    CONNECTION_STRING '<value>'
);
```

### Service Principal

(replace `CLIENT_SECRET` with `CLIENT_CERTIFICATE_PATH` to use a client certificate)

```sql
CREATE SECRET azure3 (
    TYPE AZURE,
    PROVIDER SERVICE_PRINCIPAL,
    TENANT_ID '⟨tenant id⟩',
    CLIENT_ID '⟨client id⟩',
    CLIENT_SECRET '⟨client secret⟩',
    ACCOUNT_NAME '⟨storage account name⟩'
);
```

### Access token

(its audience needs to be `https://storage.azure.com`)

```sql
CREATE SECRET secret4 (
    TYPE AZURE,
    PROVIDER ACCESS_TOKEN,
    ACCESS_TOKEN '⟨value⟩'
    ACCOUNT_NAME '⟨storage account name⟩'
);
```

### Anonymous

```sql
CREATE SECRET secret5 (
    TYPE AZURE,
    PROVIDER CONFIG,
    ACCOUNT_NAME '⟨storage account name⟩'
);
```

### Managed Identity with User-assigned ID (UAMI)

```sql
CREATE SECRET secret1 (
    TYPE AZURE,
    PROVIDER MANAGED_IDENTITY,
    ACCOUNT_NAME '⟨storage account name⟩',
    CLIENT_ID '⟨used-assigned managed identity client id⟩'
);
```

`CLIENT_ID` is optional; if not specified, the Azure SDK will attempt to find and use either a
System-assigned Managed Identity (SAMI) or User-assigned Managed Identity (UAMI). If both are
defined, or more than 1 UAMI is available, order and behavior is undefined.

Alternatively, `OBJECT_ID` or `RESOURCE_ID` may be used instead of `CLIENT_ID`. Only 1 of these
IDs may be specified.

See also [Azure Identity Managed Identity Support](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#managed-identity-support)

## Writing

Writing to Azure Storage stages blocks during upload; staged blocks are not
visible until committed which occurs during a sync or close call. Thus large
uploads may not be visible to standard Azure tooling until whole file has been
written.

The Blob writer (`az://` or `azure://`) uses block-list uploads, which cannot
overwrite files created or overwritten through DFS (`abfs://` or `abfss://`).
Use DFS for subsequent writes to those files to avoid `BlobOperationNotSupported`.
They remain readable through either endpoint, including on OneLake.

### File size limit

Azure [hard-limits a blob to **50,000
blocks**](https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets).
With the default block size of 8 MiB this caps blob size at roughly **390
GiB**. To write larger files, increase the block size:

```sql
SET azure_write_block_size = '128MiB';  -- raises the cap to ~6.1 TiB
SET azure_write_block_size = '4000MiB'; -- maximum block size, cap ~190 TiB
```

The block size can be any positive value up to 4,000 MiB (Azure's per-request limit). Set to `0` to restore the default (8 MiB).

### Write settings

| Setting | Default | Description |
|---|---|---|
| `azure_write_block_size` | `8MiB` | Size of each block for Blob/DFS writes. `0` restores the default. Max 4,000 MiB. Increase to raise the file size ceiling. |
| `azure_write_staged_blocks_per_commit` | `0` | Blocks staged before an intermediate commit. `0` disables intermediate commits; partial writes are not visible until the file is closed. |

## Metadata caching

Azure file metadata is reused within a query by default. Set
`enable_http_metadata_cache = true` to also reuse it across queries and
connections in the same database. Blob and DFS share this cache, keyed by the
resolved object location rather than the spelling of its DuckDB URI.

Cache keys have separate account/OneLake authority, full path (including the
container or OneLake workspace), snapshot, and version fields. Blob and DFS
spellings of an authority share an identity, including OneLake global, regional,
and workspace-private endpoints. Different authorities and object paths remain
isolated; authentication query parameters are not part of the key.

Shared caching requires an account resolved from the URI or a matching secret.
Legacy settings without a resolved account fall back to query-local caching.
Writes invalidate both caches at open, sync, and close, including when the
writer has shared caching disabled. Deletes and directory creation also
invalidate cached metadata. Changes made outside this extension are not tracked.

## Supported architectures

The extension is tested & distributed for Linux (x64, arm64), MacOS (x64, arm64) and Windows (x64)

## Documentation

See the [Azure page in the DuckDB documentation](https://duckdb.org/docs/extensions/azure).

Check out the tests in `test/sql` for more examples.

## Building

For development, this extension requires [CMake](https://cmake.org), Python3, a `C++11` compliant compiler, and the Azure C++ SDK. Run `make` in the root directory to compile the sources. Run `make debug` to build a non-optimized debug version. Run `make test` to verify that your version works properly after making changes. Install the Azure C++ SDK using [vcpkg](https://vcpkg.io/en/getting-started.html) and set the `VCPKG_TOOLCHAIN_PATH` environment variable when building.

```shell
sudo apt-get update && sudo apt-get install -y git g++ cmake ninja-build libssl-dev
git clone --recursive https://github.com/duckdb/duckdb_azure
git clone https://github.com/microsoft/vcpkg
./vcpkg/bootstrap-vcpkg.sh
cd duckdb_azure
GEN=ninja VCPKG_TOOLCHAIN_PATH=$PWD/../vcpkg/scripts/buildsystems/vcpkg.cmake make
```

Please also refer to our [Build Guide](https://duckdb.org/dev/building) and [Contribution Guide](https://github.com/duckdb/duckdb/blob/main/CONTRIBUTING.md).
