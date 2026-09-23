#pragma once

#include <string>

#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/storage/files/datalake/datalake_service_client.hpp>

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "azure_parsed_url.hpp"

namespace duckdb {

class ClientContext;

Azure::Storage::Blobs::BlobServiceClient ConnectToBlobStorageAccount(optional_ptr<FileOpener> opener,
                                                                     const std::string &path,
                                                                     const AzureParsedUrl &azure_parsed_url);

Azure::Storage::Files::DataLake::DataLakeServiceClient
ConnectToDfsStorageAccount(optional_ptr<FileOpener> opener, const std::string &path,
                           const AzureParsedUrl &azure_parsed_url);

const SecretMatch LookupSecret(optional_ptr<FileOpener> opener, const std::string &path);

struct AzureAccessToken {
	std::string token;
	int64_t expiration_epoch_ms;
};

//! Mint a bearer token for `token_scope` from the credential chain configured in a credential_chain secret
AzureAccessToken FetchAzureAccessToken(ClientContext &context, const KeyValueSecret &secret,
                                       const std::string &token_scope);
} // namespace duckdb
