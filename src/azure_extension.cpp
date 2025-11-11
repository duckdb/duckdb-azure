#include "azure_extension.hpp"
#include "azure_blob_filesystem.hpp"
#include "azure_dfs_filesystem.hpp"
#include "azure_secret.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Load filesystem
	auto &instance = loader.GetDatabaseInstance();
	auto &fs = instance.GetFileSystem();
	fs.RegisterSubSystem(make_uniq<AzureBlobStorageFileSystem>());
	fs.RegisterSubSystem(make_uniq<AzureDfsStorageFileSystem>());

	// Load Secret functions
	CreateAzureSecretFunctions::Register(loader);

	// Load extension config
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("azure_storage_connection_string",
	                          "Azure connection string, used for authenticating and configuring azure requests",
	                          LogicalType::VARCHAR);
	config.AddExtensionOption(
	    "azure_account_name",
	    "Azure account name, when set, the extension will attempt to automatically detect credentials",
	    LogicalType::VARCHAR);
	config.AddExtensionOption("azure_credential_chain",
	                          "Ordered list of Azure credential providers, in string format separated by ';'. E.g. "
	                          "'cli;workload_identity;managed_identity;env'",
	                          LogicalType::VARCHAR, nullptr);
	config.AddExtensionOption("azure_endpoint",
	                          "Override the azure endpoint for when the Azure credential providers are used.",
	                          LogicalType::VARCHAR, "blob.core.windows.net");
	config.AddExtensionOption("azure_http_stats",
	                          "Include http info from the Azure Storage in the explain analyze statement.",
	                          LogicalType::BOOLEAN, false);
	config.AddExtensionOption("azure_context_caching",
	                          "Enable/disable the caching of some context when performing queries. "
	                          "This cache is by default enable, and will for a given connection keep a local context "
	                          "when performing a query. "
	                          "If you suspect that the caching is causing some side effect you can try to disable it "
	                          "by setting this option to false.",
	                          LogicalType::BOOLEAN, true);
	config.AddExtensionOption("azure_transport_option_type",
	                          "Underlying adapter to use with the Azure SDK. Read more about the adapter at "
	                          "https://github.com/Azure/azure-sdk-for-cpp/blob/main/doc/HttpTransportAdapter.md. Valid "
	                          "values are: default, curl",
	                          LogicalType::VARCHAR, "default");

	AzureReadOptions default_read_options;
	config.AddExtensionOption("azure_read_transfer_concurrency",
	                          "Maximum number of threads the Azure client can use for a single parallel read. "
	                          "If azure_read_transfer_chunk_size is less than azure_read_buffer_size then setting "
	                          "this > 1 will allow the Azure client to do concurrent requests to fill the buffer.",
	                          LogicalType::INTEGER, Value::INTEGER(default_read_options.transfer_concurrency));

	config.AddExtensionOption("azure_read_transfer_chunk_size",
	                          "Maximum size in bytes that the Azure client will read in a single request. "
	                          "It is recommended that this is a factor of azure_read_buffer_size.",
	                          LogicalType::BIGINT, Value::BIGINT(default_read_options.transfer_chunk_size));

	config.AddExtensionOption("azure_read_buffer_size",
	                          "Size of the read buffer.  It is recommended that this is evenly divisible by "
	                          "azure_read_transfer_chunk_size.",
	                          LogicalType::UBIGINT, Value::UBIGINT(default_read_options.buffer_size));

	auto *http_proxy = std::getenv("HTTP_PROXY");
	Value default_http_value = http_proxy ? Value(http_proxy) : Value(nullptr);
	config.AddExtensionOption("azure_http_proxy",
	                          "Proxy to use when login & performing request to azure. "
	                          "By default it will use the HTTP_PROXY environment variable if set.",
	                          LogicalType::VARCHAR, default_http_value);
	config.AddExtensionOption("azure_proxy_user_name", "Http proxy user name if needed.", LogicalType::VARCHAR,
	                          Value(nullptr));
	config.AddExtensionOption("azure_proxy_password", "Http proxy password if needed.", LogicalType::VARCHAR,
	                          Value(nullptr));
}

void AzureExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string AzureExtension::Name() {
	return "azure";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(azure, loader) {
	duckdb::LoadInternal(loader);
}
}
