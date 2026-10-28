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

Connection strings support shared keys, SAS tokens, explicit service endpoints,
sovereign-cloud endpoint suffixes, and the Azurite shortcut
`UseDevelopmentStorage=true`. Setting names are case-insensitive. `AccountName`
is required for shared-key authentication and for generating default endpoints,
but not when an explicit endpoint is used with a SAS token or anonymous access:

```sql
CREATE SECRET sas_secret (
    TYPE AZURE,
    CONNECTION_STRING 'BlobEndpoint=https://myaccount.blob.core.windows.net;SharedAccessSignature=<sas-token>'
);
```

For Data Lake storage, specify `DfsEndpoint` or an `AccountName` from which the
SDK can generate the DFS endpoint. A fully qualified DuckDB path must match the
connection string's account name, or its service endpoint when `AccountName` is
omitted. Connection-string endpoints take precedence over endpoints in DuckDB
paths. The legacy `azure_storage_connection_string` setting supports the same
formats for Blob storage; use secrets for Data Lake connection strings.

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

## HTTP statistics

Set `azure_http_stats = true` to include Azure HTTP statistics in `EXPLAIN ANALYZE`.
Downloaded bytes count response bodies, not the object sizes advertised by HEAD
responses. HEAD requests still contribute to the request count.

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
