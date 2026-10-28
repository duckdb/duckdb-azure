#include "azure_secret.hpp"

#include "azure_dfs_filesystem.hpp"
#include "azure_storage_account_client.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"

#include <azure/identity/azure_cli_credential.hpp>
#include <azure/identity/chained_token_credential.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <azure/identity/environment_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#include <azure/storage/blobs.hpp>

namespace duckdb {
constexpr auto COMMON_OPTIONS = {
    // Proxy option
    "http_proxy", "proxy_user_name", "proxy_password",
    // Storage account option
    "account_name", "endpoint"};

//! Audience of Azure Storage, which is also what OneLake's Iceberg REST catalog accepts
constexpr auto DEFAULT_TOKEN_SCOPE = "https://storage.azure.com/.default";

static void CopySecret(const std::string &key, const CreateSecretInput &input, KeyValueSecret &result) {
	auto val = input.options.find(key);

	if (val != input.options.end()) {
		result.secret_map[Identifier(key)] = val->second;
	}
}

static void RedactCommonKeys(KeyValueSecret &result) {
	result.redact_keys.insert("proxy_password");
}

static unique_ptr<BaseSecret> CreateAzureSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("azure://");
		scope.push_back("az://");
		scope.push_back(AzureDfsStorageFileSystem::PATH_PREFIX);
		scope.push_back(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX);
	}

	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Manage common option that all secret type share
	for (const auto *key : COMMON_OPTIONS) {
		CopySecret(key, input, *result);
	}

	// Manage specific secret option
	CopySecret("connection_string", input, *result);

	// Redact sensible keys
	RedactCommonKeys(*result);
	result->redact_keys.insert("connection_string");

	return std::move(result);
}

static unique_ptr<BaseSecret> CreateAzureSecretFromCredentialChain(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("azure://");
		scope.push_back("az://");
		scope.push_back(AzureDfsStorageFileSystem::PATH_PREFIX);
		scope.push_back(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX);
	}

	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Manage common option that all secret type share
	for (const auto *key : COMMON_OPTIONS) {
		CopySecret(key, input, *result);
	}

	// Manage specific secret option
	CopySecret("chain", input, *result);
	CopySecret("token_scope", input, *result);

	// Redact sensible keys
	RedactCommonKeys(*result);

	// With refresh 'auto' the secret carries a bearer token for consumers outside this extension (e.g. duckdb-iceberg
	// authenticating to a REST catalog). Same contract as duckdb-aws: replaying 'refresh_info' as the options of a new
	// CREATE SECRET re-runs the chain, and 'expiration_epoch_ms' tells the consumer when to do so.
	auto refresh_entry = input.options.find("refresh");
	if (refresh_entry != input.options.end()) {
		auto refresh = StringUtil::Lower(refresh_entry->second.ToString());
		if (refresh != "auto") {
			throw InvalidInputException("Unsupported value '%s' for 'refresh', only 'auto' is supported", refresh);
		}

		child_list_t<Value> refresh_info;
		for (const auto &named_param : input.options) {
			refresh_info.emplace_back(Identifier(StringUtil::Lower(named_param.first)), named_param.second);
		}
		result->secret_map["refresh_info"] = Value::STRUCT(std::move(refresh_info));

		std::string token_scope = DEFAULT_TOKEN_SCOPE;
		auto token_scope_value = result->TryGetValue("token_scope");
		if (!token_scope_value.IsNull()) {
			token_scope = token_scope_value.ToString();
		}
		auto access_token = FetchAzureAccessToken(context, *result, token_scope);
		result->secret_map["token"] = Value(access_token.token);
		result->secret_map["expiration_epoch_ms"] = Value::BIGINT(access_token.expiration_epoch_ms);
		result->redact_keys.insert("token");
		// May contain proxy_password
		result->redact_keys.insert("refresh_info");
	}

	return std::move(result);
}

static unique_ptr<BaseSecret> CreateAzureSecretFromManagedIdentity(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("azure://");
		scope.push_back("az://");
		scope.push_back(AzureDfsStorageFileSystem::PATH_PREFIX);
		scope.push_back(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX);
	}

	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Manage common option that all secret type share
	for (const auto *key : COMMON_OPTIONS) {
		CopySecret(key, input, *result);
	}

	// Manage specific secret options
	CopySecret("client_id", input, *result);
	CopySecret("object_id", input, *result);
	CopySecret("resource_id", input, *result);

	// Redact sensible keys
	RedactCommonKeys(*result);

	return std::move(result);
}

static unique_ptr<BaseSecret> CreateAzureSecretFromServicePrincipal(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("azure://");
		scope.push_back("az://");
		scope.push_back(AzureDfsStorageFileSystem::PATH_PREFIX);
		scope.push_back(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX);
	}

	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Manage common option that all secret type share
	for (const auto *key : COMMON_OPTIONS) {
		CopySecret(key, input, *result);
	}

	// Manage specific secret option
	CopySecret("tenant_id", input, *result);
	CopySecret("client_id", input, *result);
	CopySecret("client_secret", input, *result);
	CopySecret("client_certificate_path", input, *result);

	// Redact sensible keys
	RedactCommonKeys(*result);
	result->redact_keys.insert("client_secret");
	result->redact_keys.insert("client_certificate_path");

	return std::move(result);
}

static unique_ptr<BaseSecret> CreateAzureSecretFromAccessToken(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("azure://");
		scope.push_back("az://");
		scope.push_back(AzureDfsStorageFileSystem::PATH_PREFIX);
		scope.push_back(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX);
	}

	auto result = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	// Manage common option that all secret type share
	for (const auto *key : COMMON_OPTIONS) {
		CopySecret(key, input, *result);
	}

	// Manage specific secret option
	CopySecret("access_token", input, *result);

	// Redact sensible keys
	RedactCommonKeys(*result);
	result->redact_keys.insert("access_token");

	return std::move(result);
}

static void RegisterCommonSecretParameters(CreateSecretFunction &function) {
	// Register azure common parameters
	function.named_parameters["account_name"] = LogicalType::VARCHAR;
	function.named_parameters["endpoint"] = LogicalType::VARCHAR;

	// Register proxy parameters
	function.named_parameters["http_proxy"] = LogicalType::VARCHAR;
	function.named_parameters["proxy_user_name"] = LogicalType::VARCHAR;
	function.named_parameters["proxy_password"] = LogicalType::VARCHAR;
}

void CreateAzureSecretFunctions::Register(ExtensionLoader &loader) {
	string type = "azure";

	// Register the new type
	SecretType secret_type;
	secret_type.name = Identifier(type);
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);

	// Register the connection string secret provider
	CreateSecretFunction connection_string_function = {type, "config", CreateAzureSecretFromConfig};
	connection_string_function.named_parameters["connection_string"] = LogicalType::VARCHAR;
	RegisterCommonSecretParameters(connection_string_function);
	loader.RegisterFunction(connection_string_function);

	// Register the credential_chain secret provider
	CreateSecretFunction cred_chain_function = {type, "credential_chain", CreateAzureSecretFromCredentialChain};
	cred_chain_function.named_parameters["chain"] = LogicalType::VARCHAR;
	cred_chain_function.named_parameters["refresh"] = LogicalType::VARCHAR;
	cred_chain_function.named_parameters["token_scope"] = LogicalType::VARCHAR;
	RegisterCommonSecretParameters(cred_chain_function);
	loader.RegisterFunction(cred_chain_function);

	// Register the managed identity secret provider
	CreateSecretFunction managed_identity_function = {type, "managed_identity", CreateAzureSecretFromManagedIdentity};
	managed_identity_function.named_parameters["client_id"] = LogicalType::VARCHAR;
	managed_identity_function.named_parameters["object_id"] = LogicalType::VARCHAR;
	managed_identity_function.named_parameters["resource_id"] = LogicalType::VARCHAR;
	RegisterCommonSecretParameters(managed_identity_function);
	loader.RegisterFunction(managed_identity_function);

	// Register the service_principal secret provider
	CreateSecretFunction service_principal_function = {type, "service_principal",
	                                                   CreateAzureSecretFromServicePrincipal};
	service_principal_function.named_parameters["tenant_id"] = LogicalType::VARCHAR;
	service_principal_function.named_parameters["client_id"] = LogicalType::VARCHAR;
	service_principal_function.named_parameters["client_secret"] = LogicalType::VARCHAR;
	service_principal_function.named_parameters["client_certificate_path"] = LogicalType::VARCHAR;
	RegisterCommonSecretParameters(service_principal_function);
	loader.RegisterFunction(service_principal_function);

	// Register the access_token secret provider
	CreateSecretFunction access_token_function = {type, "access_token", CreateAzureSecretFromAccessToken};
	access_token_function.named_parameters["access_token"] = LogicalType::VARCHAR;
	RegisterCommonSecretParameters(access_token_function);
	loader.RegisterFunction(access_token_function);
}

} // namespace duckdb
