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

### Token Credential Caching

By default, credential objects are reused across SQL statements so the Azure SDK's internal token
cache survives query boundaries and avoids redundant authentication round-trips. To disable this,
set `CACHE_TOKEN_CREDENTIAL = false`:

```sql
CREATE SECRET secret_cli (
    TYPE AZURE,
    PROVIDER CREDENTIAL_CHAIN,
    CHAIN 'cli',
    ACCOUNT_NAME '⟨storage account name⟩',
    CACHE_TOKEN_CREDENTIAL false
);
```

Updating or replacing an Azure secret invalidates its cached credential; the next query
will create a fresh one. However, out-of-band credential changes — such as `az account logout` —
cannot be detected and will not invalidate the cache. Setting `CACHE_TOKEN_CREDENTIAL = false`
disables caching for that secret, so a new credential is created from scratch for each query,
ensuring any behind-the-scenes credential changes are picked up promptly.

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

Please also refer to our [Build Guide](https://duckdb.org/dev/building) and [Contribution Guide](<[CONTRIBUTING.md](https://github.com/duckdb/duckdb/blob/main/CONTRIBUTING.md)>).
