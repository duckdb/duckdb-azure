#pragma once

#include <string>

#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/storage/files/datalake/datalake_service_client.hpp>

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "azure_parsed_url.hpp"

namespace duckdb {

Azure::Storage::Blobs::BlobServiceClient ConnectToBlobStorageAccount(optional_ptr<FileOpener> opener,
                                                                     const std::string &path,
                                                                     const AzureParsedUrl &azure_parsed_url);

Azure::Storage::Files::DataLake::DataLakeServiceClient
ConnectToDfsStorageAccount(optional_ptr<FileOpener> opener, const std::string &path,
                           const AzureParsedUrl &azure_parsed_url);

// Scopes match by prefix, and the path is used as given: a directory operation passes its path with a trailing '/',
// so that a secret scoped to '<dir>/' covers the directory itself, while a file operation on '<dir>' looks up '<dir>'.
const SecretMatch LookupSecret(optional_ptr<FileOpener> opener, const std::string &path);
} // namespace duckdb
