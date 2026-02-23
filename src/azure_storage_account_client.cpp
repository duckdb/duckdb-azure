#include "azure_storage_account_client.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "http_logging_policy.hpp"
#include "http_state_policy.hpp"
#include <unordered_set>

#include <azure/core/credentials/token_credential_options.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/core/resource_identifier.hpp>
#include <azure/identity/azure_cli_credential.hpp>
#include <azure/identity/chained_token_credential.hpp>
#include <azure/identity/client_certificate_credential.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <azure/identity/environment_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>
#include <azure/storage/files/datalake/datalake_service_client.hpp>
#include <cstdlib>
#include <memory>
#include <string>
#include <type_traits>

namespace duckdb {
const static std::string DEFAULT_BLOB_ENDPOINT = "blob.core.windows.net";
const static std::string DEFAULT_DFS_ENDPOINT = "dfs.core.windows.net";

static std::string TryGetCurrentSetting(optional_ptr<FileOpener> opener, const std::string &name) {
	Value val;
	if (FileOpener::TryGetCurrentSetting(opener, name, val)) {
		return val.ToString();
	}
	return "";
}

static bool ConnectionStringMatchStorageAccountName(const std::string &connection_string,
                                                    const std::string &provided_storage_account) {
	auto storage_account_name_pos = connection_string.find("AccountName=");
	if (storage_account_name_pos == std::string::npos) {
		throw InvalidInputException("A invalid connection string has been provided.");
	}
	return 0 == connection_string.compare(storage_account_name_pos + 12, provided_storage_account.size(),
	                                      provided_storage_account);
}

static std::string AccountUrl(const std::string &storage_account, const std::string &endpoint) {
	return "https://" + storage_account + '.' + endpoint;
}

static std::string AccountUrl(const KeyValueSecret &secret, const std::string &defaultEndpoint) {
	std::string endpoint;
	auto endpoint_value = secret.TryGetValue("endpoint");
	if (endpoint_value.IsNull()) {
		endpoint = defaultEndpoint;
	} else {
		endpoint = endpoint_value.ToString();
	}

	return AccountUrl(secret.TryGetValue("account_name", true).ToString(), endpoint);
}

static std::string AccountUrl(const AzureParsedUrl &azure_parsed_url) {
	return AccountUrl(azure_parsed_url.storage_account_name, azure_parsed_url.endpoint);
}

template <typename T>
static T ToClientOptions(const Azure::Core::Http::Policies::TransportOptions &transport_options,
                         shared_ptr<AzureHTTPState> http_state, optional_ptr<FileOpener> opener) {
	static_assert(std::is_base_of<Azure::Core::_internal::ClientOptions, T>::value,
	              "type parameter must be an Azure ClientOptions");
	T options;
	auto db = FileOpener::TryGetDatabase(opener);
	if (db) {
		options.Telemetry.ApplicationId = StringUtil::Format("%s %s", db->config.UserAgent(), DuckDB::SourceID());
	}
	options.Transport = transport_options;
	if (http_state != nullptr) {
		// Because we mainly want to have stats on what has been needed and not on
		// what has been used on the network, we register the policy on `PerOperationPolicies`
		// part and not the `PerRetryPolicies`. Network issues will result in retry that can
		// increase the input/output but will not be displayed in the EXPLAIN summary.
		options.PerOperationPolicies.emplace_back(new HttpStatePolicy(std::move(http_state)));
	}
	// Add HTTP logging policy (per-retry, so user-agent is already set by the telemetry policy)
	Value enable_http_logging_value;
	bool enable_http_logging = false;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_http_logging", enable_http_logging_value)) {
		enable_http_logging = enable_http_logging_value.GetValue<bool>();
	}
	if (enable_http_logging) {
		auto client_context = FileOpener::TryGetClientContext(opener);
		if (client_context && client_context->logger) {
			// Read redaction config options
			std::unordered_set<std::string> redact_query_params;
			std::unordered_set<std::string> redact_headers;

			Value redact_query_params_value;
			if (FileOpener::TryGetCurrentSetting(opener, "azure_http_logging_redact_query_params",
			                                     redact_query_params_value) &&
			    !redact_query_params_value.IsNull()) {
				for (auto &param : StringUtil::Split(redact_query_params_value.GetValue<std::string>(), ';')) {
					auto trimmed = param;
						StringUtil::Trim(trimmed);
					if (!trimmed.empty()) {
						redact_query_params.insert(trimmed);
					}
				}
			}

			Value redact_headers_value;
			if (FileOpener::TryGetCurrentSetting(opener, "azure_http_logging_redact_headers",
			                                     redact_headers_value) &&
			    !redact_headers_value.IsNull()) {
				for (auto &hdr : StringUtil::Split(redact_headers_value.GetValue<std::string>(), ';')) {
					auto trimmed = hdr;
						StringUtil::Trim(trimmed);
						trimmed = StringUtil::Lower(trimmed);
					if (!trimmed.empty()) {
						redact_headers.insert(trimmed);
					}
				}
			}

			options.PerRetryPolicies.emplace_back(
			    new HttpLoggingPolicy(client_context->logger, std::move(redact_query_params),
			                         std::move(redact_headers)));
		}
	}
	return options;
}

static Azure::Storage::Blobs::BlobClientOptions
ToBlobClientOptions(const Azure::Core::Http::Policies::TransportOptions &transport_options,
                    shared_ptr<AzureHTTPState> http_state, optional_ptr<FileOpener> opener) {
	return ToClientOptions<Azure::Storage::Blobs::BlobClientOptions>(transport_options, std::move(http_state), opener);
}

static Azure::Storage::Files::DataLake::DataLakeClientOptions
ToDfsClientOptions(const Azure::Core::Http::Policies::TransportOptions &transport_options,
                   shared_ptr<AzureHTTPState> http_state, optional_ptr<FileOpener> opener) {
	return ToClientOptions<Azure::Storage::Files::DataLake::DataLakeClientOptions>(transport_options,
	                                                                               std::move(http_state), opener);
}

static Azure::Core::Credentials::TokenCredentialOptions
ToTokenCredentialOptions(const Azure::Core::Http::Policies::TransportOptions &transport_options) {
	Azure::Core::Credentials::TokenCredentialOptions options;
	options.Transport = transport_options;
	return options;
}

static shared_ptr<AzureHTTPState> GetHttpState(optional_ptr<FileOpener> opener) {
	Value value;
	bool enable_http_stats = false;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_http_stats", value)) {
		enable_http_stats = value.GetValue<bool>();
	}

	shared_ptr<AzureHTTPState> http_state;
	if (enable_http_stats) {
		http_state = AzureHTTPState::TryGetState(opener);
	}

	return http_state;
}

static std::shared_ptr<Azure::Core::Credentials::TokenCredential>
CreateChainedTokenCredential(const std::string &chain,
                             const Azure::Core::Http::Policies::TransportOptions &transport_options) {
	auto credential_options = ToTokenCredentialOptions(transport_options);

	// Create credential chain
	auto chain_list = StringUtil::Split(chain, ';');
	Azure::Identity::ChainedTokenCredential::Sources sources;
	for (const auto &item : chain_list) {
		if (item == "cli") {
			sources.push_back(std::make_shared<Azure::Identity::AzureCliCredential>(credential_options));
		} else if (item == "workload_identity") {
			sources.push_back(std::make_shared<Azure::Identity::WorkloadIdentityCredential>(credential_options));
		} else if (item == "managed_identity") {
			sources.push_back(std::make_shared<Azure::Identity::ManagedIdentityCredential>(credential_options));
		} else if (item == "env") {
			sources.push_back(std::make_shared<Azure::Identity::EnvironmentCredential>(credential_options));
		} else if (item == "default") {
			sources.push_back(std::make_shared<Azure::Identity::DefaultAzureCredential>(credential_options));
		} else {
			throw InvalidInputException("Unknown credential provider found: " + item);
		}
	}
	return std::make_shared<Azure::Identity::ChainedTokenCredential>(sources);
}

static std::shared_ptr<Azure::Core::Credentials::TokenCredential>
CreateChainedTokenCredential(const KeyValueSecret &secret,
                             const Azure::Core::Http::Policies::TransportOptions &transport_options) {
	std::string chain = "default";
	auto chain_value = secret.TryGetValue("chain");
	if (!chain_value.IsNull()) {
		chain = chain_value.ToString();
	}

	return CreateChainedTokenCredential(chain, transport_options);
}

static std::shared_ptr<Azure::Core::Credentials::TokenCredential>
CreateClientCredential(const std::string &tenant_id, const std::string &client_id, const std::string &client_secret,
                       const std::string &client_certificate_path,
                       const Azure::Core::Http::Policies::TransportOptions &transport_options) {
	auto credential_options = ToTokenCredentialOptions(transport_options);
	if (!client_secret.empty()) {
		return std::make_shared<Azure::Identity::ClientSecretCredential>(tenant_id, client_id, client_secret,
		                                                                 credential_options);
	} else if (!client_certificate_path.empty()) {
		return std::make_shared<Azure::Identity::ClientCertificateCredential>(
		    tenant_id, client_id, client_certificate_path, credential_options);
	}

	throw InvalidInputException("Failed to fetch key 'client_secret' or 'client_certificate_path' from secret "
	                            "'service_principal' of type 'azure'");
}

static std::shared_ptr<Azure::Core::Credentials::TokenCredential>
CreateClientCredential(const KeyValueSecret &secret,
                       const Azure::Core::Http::Policies::TransportOptions &transport_options) {
	constexpr bool error_on_missing = true;
	auto tenant_id = secret.TryGetValue("tenant_id", error_on_missing);
	auto client_id = secret.TryGetValue("client_id", error_on_missing);
	auto client_secret_val = secret.TryGetValue("client_secret");
	auto client_certificate_path_val = secret.TryGetValue("client_certificate_path");

	std::string client_secret = client_secret_val.IsNull() ? "" : client_secret_val.ToString();
	std::string client_certificate_path =
	    client_certificate_path_val.IsNull() ? "" : client_certificate_path_val.ToString();

	return CreateClientCredential(tenant_id.ToString(), client_id.ToString(), client_secret, client_certificate_path,
	                              transport_options);
}

class AccessTokenCredential : public Azure::Core::Credentials::TokenCredential {
public:
	AccessTokenCredential(const std::string &token)
	    : Azure::Core::Credentials::TokenCredential("AccessTokenCredential") {
		access_token.Token = token;
		access_token.ExpiresOn =
		    Azure::DateTime::max(); // Refreshing tokens is not supported, so setting expiry time to infinity
	}

	Azure::Core::Credentials::AccessToken
	GetToken(Azure::Core::Credentials::TokenRequestContext const &tokenRequestContext,
	         Azure::Core::Context const &context) const override {
		return access_token;
	};

private:
	Azure::Core::Credentials::AccessToken access_token;
};

static std::shared_ptr<Azure::Core::Credentials::TokenCredential>
CreateAccessTokenCredential(const KeyValueSecret &secret) {
	constexpr bool error_on_missing = true;
	auto access_token_val = secret.TryGetValue("access_token", error_on_missing);

	std::string access_token = access_token_val.IsNull() ? "" : access_token_val.ToString();

	return std::make_shared<AccessTokenCredential>(access_token);
}

static std::shared_ptr<Azure::Core::Http::HttpTransport>
CreateCurlTransport(const std::string &proxy, const std::string &proxy_username, const std::string &proxy_password) {
	Azure::Core::Http::CurlTransportOptions curl_transport_options;

	if (!proxy.empty()) {
		curl_transport_options.Proxy = proxy;
	}

	if (!proxy_username.empty()) {
		curl_transport_options.ProxyUsername = proxy_username;
	}

	if (!proxy_password.empty()) {
		curl_transport_options.ProxyPassword = proxy_password;
	}

	const char *ca_info = std::getenv("CURL_CA_INFO");
#if !defined(_WIN32) && !defined(__APPLE__)
	if (!ca_info) {
		// https://github.com/Azure/azure-sdk-for-cpp/issues/4983
		// https://github.com/Azure/azure-sdk-for-cpp/issues/4738
		for (const auto *path : {
		         "/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
		         "/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
		         "/etc/ssl/ca-bundle.pem",                            // OpenSUSE
		         "/etc/pki/tls/cacert.pem",                           // OpenELEC
		         "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
		         "/etc/ssl/cert.pem"                                  // Alpine Linux
		     }) {
			if (FILE *f = fopen(path, "r")) {
				fclose(f);
				ca_info = path;
				break;
			}
		}
	}
#endif
	if (ca_info) {
		curl_transport_options.CAInfo = ca_info;
	}

	const char *ca_path = std::getenv("CURL_CA_PATH");
	if (ca_path) {
		curl_transport_options.CAPath = ca_path;
	}

	return std::make_shared<Azure::Core::Http::CurlTransport>(curl_transport_options);
}

static Azure::Core::Http::Policies::TransportOptions GetTransportOptions(const std::string &transport_option_type,
                                                                         const std::string &proxy,
                                                                         const std::string &proxy_username,
                                                                         const std::string &proxy_password) {
	Azure::Core::Http::Policies::TransportOptions transport_options;
	if (transport_option_type == "default") {
		if (!proxy.empty()) {
			transport_options.HttpProxy = proxy;
		}

		if (!proxy_username.empty()) {
			transport_options.ProxyUserName = proxy_username;
		}

		if (!proxy_password.empty()) {
			transport_options.ProxyPassword = proxy_password;
		}
	} else if (transport_option_type == "curl") {
		transport_options.Transport = CreateCurlTransport(proxy, proxy_username, proxy_password);
	} else {
		throw InvalidInputException("transport_option_type cannot take value '%s'", transport_option_type);
	}

	return transport_options;
}

static Azure::Core::Http::Policies::TransportOptions GetTransportOptions(optional_ptr<FileOpener> opener,
                                                                         const KeyValueSecret &secret) {
	auto transport_option_type = TryGetCurrentSetting(opener, "azure_transport_option_type");

	std::string http_proxy;
	auto http_proxy_val = secret.TryGetValue("http_proxy");
	if (!http_proxy_val.IsNull()) {
		http_proxy = http_proxy_val.ToString();
	} else {
		// Keep honoring the env variable if present
		auto *http_proxy_env = std::getenv("HTTP_PROXY");
		if (http_proxy_env != nullptr) {
			http_proxy = http_proxy_env;
		}
	}

	std::string http_proxy_username;
	auto http_proxy_username_val = secret.TryGetValue("proxy_user_name");
	if (!http_proxy_username_val.IsNull()) {
		http_proxy_username = http_proxy_username_val.ToString();
	}

	std::string http_proxy_password;
	auto http_proxy_password_val = secret.TryGetValue("proxy_password");
	if (!http_proxy_password_val.IsNull()) {
		http_proxy_password = http_proxy_password_val.ToString();
	}

	return GetTransportOptions(transport_option_type, http_proxy, http_proxy_username, http_proxy_password);
}

static Azure::Storage::Blobs::BlobServiceClient
GetBlobStorageAccountClientFromConfigProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                              const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);

	// If connection string, we're done heres
	auto connection_string_val = secret.TryGetValue("connection_string");
	if (!connection_string_val.IsNull()) {
		auto connection_string = connection_string_val.ToString();
		if (azure_parsed_url.is_fully_qualified &&
		    !ConnectionStringMatchStorageAccountName(connection_string, azure_parsed_url.storage_account_name)) {
			throw InvalidInputException("The provided connection string does not match the storage account named %s",
			                            azure_parsed_url.storage_account_name);
		}

		auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);
		return Azure::Storage::Blobs::BlobServiceClient::CreateFromConnectionString(connection_string, blob_options);
	}

	// Default provider (config) with no connection string => public storage account
	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_BLOB_ENDPOINT);
	auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Blobs::BlobServiceClient(account_url, blob_options);
}

static Azure::Storage::Files::DataLake::DataLakeServiceClient
GetDfsStorageAccountClientFromConfigProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                             const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);

	// If connection string, we're done heres
	auto connection_string_val = secret.TryGetValue("connection_string");
	if (!connection_string_val.IsNull()) {
		auto connection_string = connection_string_val.ToString();
		if (azure_parsed_url.is_fully_qualified &&
		    !ConnectionStringMatchStorageAccountName(connection_string, azure_parsed_url.storage_account_name)) {
			throw InvalidInputException("The provided connection string does not match the storage account named %s",
			                            azure_parsed_url.storage_account_name);
		}

		auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
		return Azure::Storage::Files::DataLake::DataLakeServiceClient::CreateFromConnectionString(connection_string,
		                                                                                          dfs_options);
	}

	// Default provider (config) with no connection string => public storage account
	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_DFS_ENDPOINT);
	auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Files::DataLake::DataLakeServiceClient(account_url, dfs_options);
}

static Azure::Storage::Blobs::BlobServiceClient
GetBlobStorageAccountClientFromCredentialChainProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                       const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	// Create credential chain
	auto credential = CreateChainedTokenCredential(secret, transport_options);

	// Connect to storage account
	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_BLOB_ENDPOINT);
	auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Blobs::BlobServiceClient(account_url, std::move(credential), blob_options);
}

static Azure::Storage::Files::DataLake::DataLakeServiceClient
GetDfsStorageAccountClientFromCredentialChainProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                      const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	// Create credential chain
	auto credential = CreateChainedTokenCredential(secret, transport_options);

	// Connect to storage account
	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_DFS_ENDPOINT);
	auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Files::DataLake::DataLakeServiceClient(account_url, std::move(credential), dfs_options);
}

static std::shared_ptr<Azure::Identity::ManagedIdentityCredential>
GetManagedIdentityCredential(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                             const Azure::Core::Http::Policies::TransportOptions &tp_opts) {
	using namespace Azure::Identity;

	ManagedIdentityCredentialOptions mi_cred_options;

	auto client_id = secret.TryGetValue("client_id");
	auto object_id = secret.TryGetValue("object_id");
	auto resource_id = secret.TryGetValue("resource_id");

	// check that max 1 of client, object or resource ID is specified
	auto id_count = int(!client_id.IsNull()) + int(!object_id.IsNull()) + int(!resource_id.IsNull());
	if (id_count > 1) {
		throw InvalidInputException(
		    "Only 1 of client_id, object_id, or resource_id, may be specified with provider managed_identity");
	}

	// https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#managed-identity-support
	if (!client_id.IsNull()) {
		mi_cred_options.IdentityId = ManagedIdentityId::FromUserAssignedClientId(client_id.ToString());
	}
	if (!object_id.IsNull()) {
		mi_cred_options.IdentityId = ManagedIdentityId::FromUserAssignedObjectId(object_id.ToString());
	}
	if (!resource_id.IsNull()) {
		mi_cred_options.IdentityId =
		    ManagedIdentityId::FromUserAssignedResourceId(Azure::Core::ResourceIdentifier(resource_id.ToString()));
	}

	return std::make_shared<ManagedIdentityCredential>(mi_cred_options);
}

static Azure::Storage::Blobs::BlobServiceClient
GetBlobStorageAccountClientFromManagedIdentityProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                       const AzureParsedUrl &azure_parsed_url) {
	// Connect to (blob) storage account
	auto transport_options = GetTransportOptions(opener, secret);
	auto mi_cred = GetManagedIdentityCredential(opener, secret, transport_options);
	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_BLOB_ENDPOINT);
	auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Blobs::BlobServiceClient(account_url, mi_cred, blob_options);
}

static Azure::Storage::Files::DataLake::DataLakeServiceClient
GetDfsStorageAccountClientFromManagedIdentityProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                      const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	auto mi_cred = GetManagedIdentityCredential(opener, secret, transport_options);

	// Connect to ADLS storage account
	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_DFS_ENDPOINT);
	auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Files::DataLake::DataLakeServiceClient(account_url, mi_cred, dfs_options);
}

static Azure::Storage::Blobs::BlobServiceClient
GetBlobStorageAccountClientFromServicePrincipalProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                        const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	auto token_credential = CreateClientCredential(secret, transport_options);

	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_BLOB_ENDPOINT);
	;
	auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Blobs::BlobServiceClient(account_url, token_credential, blob_options);
}

static Azure::Storage::Files::DataLake::DataLakeServiceClient
GetDfsStorageAccountClientFromServicePrincipalProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                       const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	auto token_credential = CreateClientCredential(secret, transport_options);

	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_DFS_ENDPOINT);
	;
	auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Files::DataLake::DataLakeServiceClient(account_url, token_credential, dfs_options);
}

static Azure::Storage::Blobs::BlobServiceClient
GetBlobStorageAccountClientFromAccessTokenProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                   const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	auto token_credential = CreateAccessTokenCredential(secret);

	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_BLOB_ENDPOINT);
	;
	auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Blobs::BlobServiceClient(account_url, token_credential, blob_options);
}

static Azure::Storage::Files::DataLake::DataLakeServiceClient
GetDfsStorageAccountClientFromAccessTokenProvider(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                                                  const AzureParsedUrl &azure_parsed_url) {
	auto transport_options = GetTransportOptions(opener, secret);
	auto token_credential = CreateAccessTokenCredential(secret);

	auto account_url =
	    azure_parsed_url.is_fully_qualified ? AccountUrl(azure_parsed_url) : AccountUrl(secret, DEFAULT_DFS_ENDPOINT);
	;
	auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Files::DataLake::DataLakeServiceClient(account_url, token_credential, dfs_options);
}

static Azure::Storage::Blobs::BlobServiceClient GetBlobStorageAccountClient(optional_ptr<FileOpener> opener,
                                                                            const KeyValueSecret &secret,
                                                                            const AzureParsedUrl &azure_parsed_url) {
	auto &provider = secret.GetProvider();
	// default provider
	if (provider == "config") {
		return GetBlobStorageAccountClientFromConfigProvider(opener, secret, azure_parsed_url);
	} else if (provider == "credential_chain") {
		return GetBlobStorageAccountClientFromCredentialChainProvider(opener, secret, azure_parsed_url);
	} else if (provider == "managed_identity") {
		return GetBlobStorageAccountClientFromManagedIdentityProvider(opener, secret, azure_parsed_url);
	} else if (provider == "service_principal") {
		return GetBlobStorageAccountClientFromServicePrincipalProvider(opener, secret, azure_parsed_url);
	} else if (provider == "access_token") {
		return GetBlobStorageAccountClientFromAccessTokenProvider(opener, secret, azure_parsed_url);
	}

	throw InvalidInputException("Unsupported provider type %s for azure", provider);
}

static Azure::Storage::Files::DataLake::DataLakeServiceClient
GetDfsStorageAccountClient(optional_ptr<FileOpener> opener, const KeyValueSecret &secret,
                           const AzureParsedUrl &azure_parsed_url) {
	auto &provider = secret.GetProvider();
	// default provider
	if (provider == "config") {
		return GetDfsStorageAccountClientFromConfigProvider(opener, secret, azure_parsed_url);
	} else if (provider == "credential_chain") {
		return GetDfsStorageAccountClientFromCredentialChainProvider(opener, secret, azure_parsed_url);
	} else if (provider == "managed_identity") {
		return GetDfsStorageAccountClientFromManagedIdentityProvider(opener, secret, azure_parsed_url);
	} else if (provider == "service_principal") {
		return GetDfsStorageAccountClientFromServicePrincipalProvider(opener, secret, azure_parsed_url);
	} else if (provider == "access_token") {
		return GetDfsStorageAccountClientFromAccessTokenProvider(opener, secret, azure_parsed_url);
	}

	throw InvalidInputException("Unsupported provider type %s for azure", provider);
}

static Azure::Core::Http::Policies::TransportOptions GetTransportOptions(optional_ptr<FileOpener> opener) {
	auto azure_transport_option_type = TryGetCurrentSetting(opener, "azure_transport_option_type");

	// Load proxy options
	auto http_proxy = TryGetCurrentSetting(opener, "azure_http_proxy");
	auto http_proxy_user_name = TryGetCurrentSetting(opener, "azure_proxy_user_name");
	auto http_proxy_password = TryGetCurrentSetting(opener, "azure_proxy_password");

	return GetTransportOptions(azure_transport_option_type, http_proxy, http_proxy_user_name, http_proxy_password);
}

static Azure::Storage::Blobs::BlobServiceClient GetBlobStorageAccountClient(optional_ptr<FileOpener> opener,
                                                                            const std::string &provided_storage_account,
                                                                            const std::string &provided_endpoint) {
	auto transport_options = GetTransportOptions(opener);
	auto blob_options = ToBlobClientOptions(transport_options, GetHttpState(opener), opener);

	auto connection_string = TryGetCurrentSetting(opener, "azure_storage_connection_string");
	if (!connection_string.empty() &&
	    ConnectionStringMatchStorageAccountName(connection_string, provided_storage_account)) {
		return Azure::Storage::Blobs::BlobServiceClient::CreateFromConnectionString(connection_string, blob_options);
	}

	std::string endpoint;
	if (provided_endpoint.empty()) {
		endpoint = TryGetCurrentSetting(opener, "azure_endpoint");
		if (endpoint.empty()) {
			endpoint = DEFAULT_BLOB_ENDPOINT;
		}
	} else {
		endpoint = provided_endpoint;
	}

	std::string azure_account_name;
	if (provided_storage_account.empty()) {
		azure_account_name = TryGetCurrentSetting(opener, "azure_account_name");
	} else {
		azure_account_name = provided_storage_account;
	}
	if (azure_account_name.empty()) {
		throw InvalidInputException("No valid Azure credentials found!");
	}

	auto account_url = AccountUrl(azure_account_name, endpoint);
	// Credential chain secret equivalent
	auto credential_chain = TryGetCurrentSetting(opener, "azure_credential_chain");
	if (!credential_chain.empty()) {
		auto credential = CreateChainedTokenCredential(credential_chain, transport_options);

		return Azure::Storage::Blobs::BlobServiceClient(account_url, std::move(credential), blob_options);
	}

	// Anonymous
	return Azure::Storage::Blobs::BlobServiceClient {account_url, blob_options};
}

const SecretMatch LookupSecret(optional_ptr<FileOpener> opener, const std::string &path) {
	auto secret_manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);

	if (secret_manager && transaction) {
		return secret_manager->LookupSecret(*transaction, path, "azure");
	}

	return {};
}

Azure::Storage::Blobs::BlobServiceClient ConnectToBlobStorageAccount(optional_ptr<FileOpener> opener,
                                                                     const std::string &path,
                                                                     const AzureParsedUrl &azure_parsed_url) {
	auto secret_match = LookupSecret(opener, path);
	if (secret_match.HasMatch()) {
		const auto &base_secret = secret_match.GetSecret();
		return GetBlobStorageAccountClient(opener, dynamic_cast<const KeyValueSecret &>(base_secret), azure_parsed_url);
	}

	// No secret found try to connect with variables
	return GetBlobStorageAccountClient(opener, azure_parsed_url.storage_account_name, azure_parsed_url.endpoint);
}

Azure::Storage::Files::DataLake::DataLakeServiceClient
ConnectToDfsStorageAccount(optional_ptr<FileOpener> opener, const std::string &path,
                           const AzureParsedUrl &azure_parsed_url) {
	auto secret_match = LookupSecret(opener, path);
	if (secret_match.HasMatch()) {
		const auto &base_secret = secret_match.GetSecret();
		return GetDfsStorageAccountClient(opener, dynamic_cast<const KeyValueSecret &>(base_secret), azure_parsed_url);
	}

	if (!azure_parsed_url.is_fully_qualified) {
		throw InvalidInputException(
		    "Cannot identify the storage account from path '%s'. To connect anonymously to a "
		    "storage account easier a fully qualified path has to be provided or secret must be created.",
		    path);
	}

	// No secret but FQDN has been provided, connect to a public storage account
	auto transport_options = GetTransportOptions(opener);
	auto account_url = "https://" + azure_parsed_url.storage_account_name + '.' + azure_parsed_url.endpoint;
	auto dfs_options = ToDfsClientOptions(transport_options, GetHttpState(opener), opener);
	return Azure::Storage::Files::DataLake::DataLakeServiceClient(account_url, dfs_options);
}

} // namespace duckdb
