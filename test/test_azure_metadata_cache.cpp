#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "azure_blob_filesystem.hpp"
#include "azure_dfs_filesystem.hpp"
#include "azure_extension.hpp"
#include "duckdb/main/client_context_file_opener.hpp"

#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <functional>

using namespace duckdb;

static AzureOptions TestOptions() {
	AzureOptions options;
	options.read_buffer_size = 128;
	options.write_block_size = 4;
	return options;
}

static void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	INFO(result->ToString());
	REQUIRE(!result->HasError());
}

struct CacheTestContext {
	DuckDB db;
	Connection con;
	ClientContextFileOpener opener;

	CacheTestContext() : con(db), opener(*con.context) {
		db.LoadStaticExtension<AzureExtension>();
		Execute(con, "SET allow_persistent_secrets = false");
	}
};

template <class FILE_SYSTEM>
static AzureMetadataCacheHandle GetCache(FILE_SYSTEM &fs, FileOpener &opener, const string &path,
                                         const string &resolved_url) {
	AzureMetadataCacheHandle cache;
	auto context = opener.TryGetClientContext();
	REQUIRE(context);
	context->RunFunctionInTransaction(
	    [&]() { cache = fs.GetMetadataCache(&opener, path, ParseUrl(path), resolved_url); });
	return cache;
}

class TestAzureFileHandle : public AzureFileHandle {
public:
	TestAzureFileHandle(AzureStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
	                    AzureMetadataCacheHandle cache = {})
	    : AzureFileHandle(fs, info, flags, FileType::FILE_TYPE_INVALID, TestOptions(), std::move(cache)) {
	}

	void Close() override {
		InvalidateMetadata();
	}
};

class TestAzureFileSystem : public AzureStorageFileSystem {
public:
	using AzureStorageFileSystem::GetMetadataCache;

	explicit TestAzureFileSystem(shared_ptr<AzureMetadataCache> cache) : AzureStorageFileSystem(std::move(cache)) {
	}

	AzureMetadataCacheHandle CacheFor(FileOpener &opener, const string &path, const string &resolved_url) {
		return GetCache(*this, opener, path, resolved_url);
	}

	string GetName() const override {
		return "TestAzureFileSystem";
	}

	bool CanHandleFile(const string &path) override {
		return true;
	}

	idx_t remote_loads = 0;
	idx_t remote_length = 100;

protected:
	unique_ptr<AzureFileHandle> CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
	                                         optional_ptr<FileOpener> opener) override {
		throw InternalException("Unexpected CreateHandle in metadata cache test");
	}

	void ReadRange(AzureFileHandle &handle, idx_t offset, char *buffer, idx_t length) override {
		throw InternalException("Unexpected ReadRange in metadata cache test");
	}

	const string &GetContextPrefix() const override {
		static const string prefix = "az://";
		return prefix;
	}

	shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                   const AzureParsedUrl &parsed_url) override {
		throw InternalException("Unexpected CreateStorageContext in metadata cache test");
	}

	void LoadRemoteFileInfo(AzureFileHandle &handle) override {
		remote_loads++;
		if (handle.flags.ExclusiveCreate()) {
			throw IOException("ExclusiveCreate specified while file already exists");
		}
		auto length = handle.flags.OpenForWriting() && handle.flags.OverwriteExistingFile() ? 0 : remote_length;
		handle.SetFileInfo(FileType::FILE_TYPE_REGULAR, length, timestamp_t(123), "remote-etag");
	}
};

class TestBlobFileSystem : public AzureBlobStorageFileSystem {
public:
	using AzureBlobStorageFileSystem::AzureBlobStorageFileSystem;
	using AzureStorageFileSystem::GetMetadataCache;
};

class TestDfsFileSystem : public AzureDfsStorageFileSystem {
public:
	using AzureDfsStorageFileSystem::AzureDfsStorageFileSystem;
	using AzureStorageFileSystem::GetMetadataCache;
};

static OpenFileInfo PrefilledInfo(const string &path) {
	OpenFileInfo info(path);
	info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	auto &options = info.extended_info->options;
	options.emplace("file_size", Value::UBIGINT(42));
	options.emplace("last_modified", Value::TIMESTAMP(timestamp_t(0)));
	options.emplace("etag", Value(""));
	return info;
}

TEST_CASE("Azure accepts caller metadata without a type or nonempty ETag", "[azure_metadata_cache]") {
	TestAzureFileSystem fs(make_shared_ptr<AzureMetadataCache>(false));
	for (const auto &scheme : {"az://", "abfss://"}) {
		auto info = PrefilledInfo(string(scheme) + "container/file");
		TestAzureFileHandle handle(fs, info, FileFlags::FILE_FLAGS_READ);
		REQUIRE(handle.IsRemoteLoaded());
		REQUIRE(handle.PostConstruct());
		CHECK(handle.GetType() == FileType::FILE_TYPE_REGULAR);
		CHECK(handle.length == 42);
		CHECK(handle.last_modified == timestamp_t(0));
		CHECK(handle.etag.empty());
	}
	CHECK(fs.remote_loads == 0);
}

TEST_CASE("Azure metadata prefill respects types and open flags", "[azure_metadata_cache]") {
	TestAzureFileSystem fs(make_shared_ptr<AzureMetadataCache>(false));
	auto info = PrefilledInfo("az://container/file");

	SECTION("directory metadata does not describe a regular file") {
		info.extended_info->options["type"] = Value("directory");
		info.extended_info->options.erase("file_size");
		info.extended_info->options.erase("etag");
		TestAzureFileHandle handle(fs, info, FileFlags::FILE_FLAGS_READ);
		REQUIRE(handle.PostConstruct());
		CHECK(handle.GetType() == FileType::FILE_TYPE_DIR);
		CHECK(handle.length == 0);
		CHECK(fs.remote_loads == 0);
	}
	SECTION("missing metadata still loads remote properties") {
		info.extended_info->options.erase("etag");
		TestAzureFileHandle handle(fs, info, FileFlags::FILE_FLAGS_READ);
		CHECK(!handle.IsRemoteLoaded());
		REQUIRE(handle.PostConstruct());
		CHECK(fs.remote_loads == 1);
		CHECK(handle.length == fs.remote_length);
	}
	SECTION("write opens still create or truncate remotely") {
		info.extended_info->options["type"] = Value("file");
		info.extended_info->options["etag"] = Value("listing-etag");
		TestAzureFileHandle handle(fs, info, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		CHECK(!handle.IsRemoteLoaded());
		REQUIRE(handle.PostConstruct());
		CHECK(fs.remote_loads == 1);
		CHECK(handle.length == 0);
	}
	SECTION("exclusive create is not bypassed by caller metadata") {
		info.extended_info->options["type"] = Value("file");
		info.extended_info->options["etag"] = Value("listing-etag");
		TestAzureFileHandle handle(fs, info, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
		CHECK(!handle.IsRemoteLoaded());
		REQUIRE_THROWS_WITH(handle.PostConstruct(), Catch::Contains("ExclusiveCreate"));
		CHECK(fs.remote_loads == 1);
	}
	SECTION("null-if-exists also applies to prefilled metadata") {
		TestAzureFileHandle handle(fs, info, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_EXISTS);
		CHECK(!handle.PostConstruct());
		CHECK(fs.remote_loads == 0);
	}
}

TEST_CASE("Azure metadata keys identify resolved objects across both filesystems", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestBlobFileSystem blob_fs(global_cache);
	TestDfsFileSystem dfs_fs(global_cache);

	const string blob_url = "https://account.blob.core.windows.net/container/dir/file";
	const string dfs_url = "https://account.dfs.core.windows.net/container/dir/file";
	auto blob = GetCache(blob_fs, context.opener, "az://container/dir/file", blob_url);
	REQUIRE(blob.cache == global_cache);
	CHECK(blob.key.endpoint_identity == "account.core.windows.net");
	CHECK(blob.key.path == "container/dir/file");
	CHECK(blob.key.snapshot.empty());
	CHECK(blob.key.version.empty());
	for (const auto &path : {"azure://container/dir/file", "az://account.blob.core.windows.net/container/dir/file"}) {
		auto alias = GetCache(blob_fs, context.opener, path, blob_url);
		CHECK(alias.key == blob.key);
		CHECK(alias.cache == blob.cache);
	}
	for (const auto &path : {"abfss://container/dir/file", "abfs://account.dfs.core.windows.net/container/dir/file",
	                         "abfss://container@account.dfs.core.windows.net/dir/file"}) {
		auto alias = GetCache(dfs_fs, context.opener, path, dfs_url);
		CHECK(alias.key == blob.key);
		CHECK(alias.cache == blob.cache);
	}

	AzureFileInfo info;
	info.length = 42;
	blob.cache->Insert(blob.key, info, blob.cache->GetGeneration());
	auto dfs = GetCache(dfs_fs, context.opener, "abfss://container/dir/file", dfs_url);
	AzureFileInfo cached;
	REQUIRE(dfs.cache->Find(dfs.key, cached));
	CHECK(cached.length == 42);
	dfs.Invalidate();
	CHECK(!blob.cache->Find(blob.key, cached));

	auto signed_url = GetCache(blob_fs, context.opener, "az://container/dir/file", blob_url + "?sig=test-signature");
	CHECK(signed_url.key == blob.key);
	auto snapshot = GetCache(blob_fs, context.opener, "az://container/dir/file", blob_url + "?snapshot=old");
	auto version = GetCache(blob_fs, context.opener, "az://container/dir/file", blob_url + "?versionid=old");
	CHECK(snapshot.key.snapshot == "old");
	CHECK(snapshot.key.version.empty());
	CHECK(version.key.snapshot.empty());
	CHECK(version.key.version == "old");
	CHECK(snapshot.key != blob.key);
	CHECK(version.key != blob.key);
	CHECK(snapshot.key != version.key);
	auto literal_query = GetCache(blob_fs, context.opener, "az://container/dir/file", blob_url + "%3Fsnapshot=old");
	CHECK(literal_query.key != snapshot.key);
	auto other_endpoint = GetCache(blob_fs, context.opener, "az://container/dir/file",
	                               "https://account.blob.core.chinacloudapi.cn/container/dir/file");
	CHECK(other_endpoint.key != blob.key);
}

TEST_CASE("Azure structured cache keys compare and hash every identity field", "[azure_metadata_cache]") {
	AzureMetadataCache cache(false);
	const vector<AzureMetadataCacheKey> keys {{"account.core.windows.net", "container/file", "", ""},
	                                          {"other.core.windows.net", "container/file", "", ""},
	                                          {"account.core.windows.net", "other-container/file", "", ""},
	                                          {"account.core.windows.net", "container/file", "old", ""},
	                                          {"account.core.windows.net", "container/file", "", "old"},
	                                          {"account.core.windows.net", "container/file", "old", "new"},
	                                          {"account.core.windows.net", "container/file\nsnapshot:old", "", ""}};
	for (idx_t i = 0; i < keys.size(); i++) {
		AzureFileInfo info;
		info.length = i + 1;
		cache.Insert(keys[i], info, cache.GetGeneration());
	}
	for (idx_t i = 0; i < keys.size(); i++) {
		AzureFileInfo info;
		auto copy = keys[i];
		CHECK(copy == keys[i]);
		CHECK(AzureMetadataCacheKeyHash()(copy) == AzureMetadataCacheKeyHash()(keys[i]));
		REQUIRE(cache.Find(copy, info));
		CHECK(info.length == i + 1);
	}
	cache.Erase(keys[3]);
	AzureFileInfo info;
	CHECK(!cache.Find(keys[3], info));
	REQUIRE(cache.Find(keys[4], info));
	CHECK(info.length == 5);
}

TEST_CASE("OneLake Blob and DFS URLs share structured workspace and item identities", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestDfsFileSystem dfs_fs(global_cache);
	TestBlobFileSystem blob_fs(global_cache);
	const string workspace = "11111111-1111-1111-1111-111111111111";
	const string item_path = "22222222-2222-2222-2222-222222222222/Files/data.parquet";
	const string path = "abfss://" + workspace + "@onelake.dfs.fabric.microsoft.com/" + item_path;
	Azure::Storage::Files::DataLake::DataLakeServiceClient service("https://onelake.dfs.fabric.microsoft.com");
	auto client = service.GetFileSystemClient(workspace).GetFileClient(item_path);
	auto cache = GetCache(dfs_fs, context.opener, path, client.GetUrl());
	REQUIRE(cache.cache == global_cache);
	CHECK(cache.key.endpoint_identity == "onelake.fabric.microsoft.com");
	CHECK(cache.key.path == workspace + "/" + item_path);
	CHECK(cache.key.snapshot.empty());
	CHECK(cache.key.version.empty());

	for (const auto &scheme : {"abfs://", "abfss://"}) {
		auto qualified_path = string(scheme) + "onelake.dfs.fabric.microsoft.com/" + workspace + "/" + item_path;
		auto alias = GetCache(dfs_fs, context.opener, qualified_path, client.GetUrl());
		CHECK(alias.key == cache.key);
	}
	auto blob_path = "az://onelake.blob.fabric.microsoft.com/" + workspace + "/" + item_path;
	Azure::Storage::Blobs::BlobServiceClient blob_service("https://onelake.blob.fabric.microsoft.com");
	auto blob_client = blob_service.GetBlobContainerClient(workspace).GetBlockBlobClient(item_path);
	auto blob = GetCache(blob_fs, context.opener, blob_path, blob_client.GetUrl());
	CHECK(blob.key == cache.key);
	CHECK(blob.cache == cache.cache);
	for (const auto &scheme : {"az://", "azure://"}) {
		auto alias = GetCache(blob_fs, context.opener,
		                      string(scheme) + "onelake.blob.fabric.microsoft.com/" + workspace + "/" + item_path,
		                      blob_client.GetUrl());
		CHECK(alias.key == cache.key);
	}
	auto explicit_port = GetCache(dfs_fs, context.opener, path,
	                              "https://onelake.dfs.fabric.microsoft.com:443/" + workspace + "/" + item_path);
	CHECK(explicit_port.key == cache.key);

	AzureFileInfo info;
	info.length = 42;
	cache.cache->Insert(cache.key, info, cache.cache->GetGeneration());
	AzureFileInfo cached;
	REQUIRE(blob.cache->Find(blob.key, cached));
	CHECK(cached.length == 42);
	blob.Invalidate();
	CHECK(!cache.cache->Find(cache.key, cached));
	info.length = 84;
	blob.cache->Insert(blob.key, info, blob.cache->GetGeneration());
	REQUIRE(cache.cache->Find(cache.key, cached));
	CHECK(cached.length == 84);
	cache.Invalidate();
	CHECK(!blob.cache->Find(blob.key, cached));

	auto other = service.GetFileSystemClient("other-workspace").GetFileClient(item_path);
	auto other_cache =
	    GetCache(dfs_fs, context.opener, "abfss://other-workspace@onelake.dfs.fabric.microsoft.com/" + item_path,
	             other.GetUrl());
	CHECK(other_cache.key.endpoint_identity == cache.key.endpoint_identity);
	CHECK(other_cache.key != cache.key);
	CHECK(other_cache.key.path == "other-workspace/" + item_path);
	auto named = service.GetFileSystemClient("Workspace").GetFileClient("Lakehouse.Lakehouse/Files/file name.parquet");
	auto named_cache =
	    GetCache(dfs_fs, context.opener,
	             "abfss://Workspace@onelake.dfs.fabric.microsoft.com/Lakehouse.Lakehouse/Files/file name.parquet",
	             named.GetUrl());
	CHECK(named_cache.key.path == "Workspace/Lakehouse.Lakehouse/Files/file name.parquet");
	Execute(context.con, "SET enable_http_metadata_cache = false");
	auto local = GetCache(dfs_fs, context.opener, path, client.GetUrl());
	REQUIRE(local.cache != global_cache);
	CHECK(local.key == cache.key);
	local.cache->Insert(local.key, info, local.cache->GetGeneration());
	global_cache->Insert(cache.key, info, global_cache->GetGeneration());
	local.Invalidate();
	CHECK(!local.cache->Find(local.key, cached));
	CHECK(!global_cache->Find(cache.key, cached));
}

TEST_CASE("OneLake regional and private endpoints canonicalize Blob and DFS services", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestDfsFileSystem dfs_fs(global_cache);
	TestBlobFileSystem blob_fs(global_cache);
	const string object_path = "workspace/Lakehouse.Lakehouse/Files/data.parquet";
	for (const auto &authority : {"westus-onelake", "11111111111111111111111111111111.z11"}) {
		const auto dfs_host = string(authority) + ".dfs.fabric.microsoft.com";
		const auto blob_host = string(authority) + ".blob.fabric.microsoft.com";
		const auto dfs_path = "abfss://" + dfs_host + "/" + object_path;
		const auto blob_path = "az://" + blob_host + "/" + object_path;
		auto dfs = GetCache(dfs_fs, context.opener, dfs_path, "https://" + dfs_host + "/" + object_path);
		auto blob = GetCache(blob_fs, context.opener, blob_path, "https://" + blob_host + "/" + object_path);
		CHECK(dfs.key == blob.key);
		CHECK(dfs.key.endpoint_identity == string(authority) + ".fabric.microsoft.com");
		CHECK(dfs.key.path == object_path);
		auto azure_account = GetCache(blob_fs, context.opener, "az://onelake.blob.core.windows.net/" + object_path,
		                              "https://onelake.blob.core.windows.net/" + object_path);
		CHECK(azure_account.key != dfs.key);
	}
}

TEST_CASE("OneLake keys keep optional selectors independent of paths and credentials", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestDfsFileSystem dfs_fs(global_cache);
	const string path = "abfss://workspace@onelake.dfs.fabric.microsoft.com/Lakehouse.Lakehouse/Files/file";
	const string url = "https://onelake.dfs.fabric.microsoft.com/workspace/Lakehouse.Lakehouse/Files/file";
	auto first =
	    GetCache(dfs_fs, context.opener, path, url + "?snapshot=2026-10-07T12%3A00%3A00Z&versionid=v%2B1&sig=first");
	auto second =
	    GetCache(dfs_fs, context.opener, path, url + "?sig=second&versionid=v%2B1&snapshot=2026-10-07T12:00:00Z");
	CHECK(first.key == second.key);
	CHECK(first.key.endpoint_identity == "onelake.fabric.microsoft.com");
	CHECK(first.key.path == "workspace/Lakehouse.Lakehouse/Files/file");
	CHECK(first.key.snapshot == "2026-10-07T12:00:00Z");
	CHECK(first.key.version == "v+1");
	auto literal = GetCache(dfs_fs, context.opener, path, url + "%3Fsnapshot=2026-10-07T12:00:00Z");
	CHECK(literal.key != first.key);
}

TEST_CASE("Azure global metadata does not collide across accounts or custom endpoints", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'first')");
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestAzureFileSystem fs(global_cache);
	const string path = "az://container/file";
	auto first = fs.CacheFor(context.opener, path, "https://first.blob.core.windows.net/container/file");
	TestAzureFileHandle first_handle(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, first);
	REQUIRE(first_handle.PostConstruct());
	CHECK(first_handle.length == 100);
	Connection second_con(context.db);
	Execute(second_con, "SET enable_http_metadata_cache = true");
	Execute(second_con, "CREATE OR REPLACE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'second')");
	ClientContextFileOpener second_opener(*second_con.context);
	auto second = fs.CacheFor(second_opener, path, "https://second.blob.core.windows.net/container/file");
	REQUIRE(first.cache == second.cache);
	REQUIRE(first.key != second.key);
	fs.remote_length = 200;
	TestAzureFileHandle second_handle(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, second);
	REQUIRE(second_handle.PostConstruct());
	CHECK(second_handle.length == 200);
	CHECK(fs.remote_loads == 2);
	auto custom_first = fs.CacheFor(context.opener, path, "http://127.0.0.1:10000/account/container/file");
	auto custom_second = fs.CacheFor(context.opener, path, "http://127.0.0.1:11000/account/container/file");
	CHECK(custom_first.key != custom_second.key);
	auto http_default = fs.CacheFor(context.opener, path, "http://127.0.0.1:80/account/container/file");
	auto https_default = fs.CacheFor(context.opener, path, "https://127.0.0.1:443/account/container/file");
	CHECK(http_default.key != https_default.key);
	auto custom_blob = fs.CacheFor(context.opener, path, "https://account.blob.example.com/container/file");
	auto custom_dfs = fs.CacheFor(context.opener, path, "https://account.dfs.example.com/container/file");
	CHECK(custom_blob.key != custom_dfs.key);
}

TEST_CASE("Azure query-local metadata expires at query end including the legacy fallback", "[azure_metadata_cache]") {
	CacheTestContext context;
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestAzureFileSystem fs(global_cache);
	const string path = "az://container/file";
	const string url = "https://account.blob.core.windows.net/container/file";

	for (bool global_enabled : {false, true}) {
		Execute(context.con,
		        global_enabled ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
		auto cache = fs.CacheFor(context.opener, path, url);
		REQUIRE(cache.cache);
		CHECK(cache.cache != global_cache);
		TestAzureFileHandle first(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
		REQUIRE(first.PostConstruct());
		auto loads = fs.remote_loads;
		TestAzureFileHandle second(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
		REQUIRE(second.PostConstruct());
		CHECK(fs.remote_loads == loads);
		Execute(context.con, "SELECT 42");
		TestAzureFileHandle next_query(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ,
		                               fs.CacheFor(context.opener, path, url));
		REQUIRE(next_query.PostConstruct());
		CHECK(fs.remote_loads == loads + 1);
		AzureFileInfo unused;
		CHECK(!global_cache->Find(cache.key, unused));
	}
	Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ENDPOINT 'blob.core.windows.net')");
	auto endpoint_only = fs.CacheFor(context.opener, path, url);
	CHECK(endpoint_only.cache != global_cache);
}

class CommitTransport : public Azure::Core::Http::HttpTransport {
public:
	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     const Azure::Core::Context &context) override {
		using Azure::Core::Http::HttpStatusCode;
		auto query = request.GetUrl().GetQueryParameters();
		HttpStatusCode status;
		if (query["comp"] == "block") {
			status = HttpStatusCode::Created;
			stages++;
		} else if (query["comp"] == "blocklist") {
			status = HttpStatusCode::Created;
			commits++;
		} else if (query["action"] == "append") {
			status = HttpStatusCode::Accepted;
			stages++;
		} else if (query["action"] == "flush") {
			status = HttpStatusCode::Ok;
			commits++;
		} else {
			throw InternalException("Unexpected Azure request in metadata cache test");
		}
		auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, status, "OK");
		response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(nullptr, 0));
		response->SetHeader("content-length", query["action"] == "flush" ? query["position"] : "0");
		response->SetHeader("etag", "\"committed-etag\"");
		response->SetHeader("last-modified", "Wed, 07 Oct 2026 12:00:00 GMT");
		response->SetHeader("x-ms-request-id", "metadata-cache-test");
		response->SetHeader("x-ms-request-server-encrypted", "true");
		return response;
	}

	idx_t stages = 0;
	idx_t commits = 0;
};

TEST_CASE("Azure writes invalidate metadata at sync and close even without shared caching", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	auto transport = std::make_shared<CommitTransport>();
	AzureFileInfo info;
	info.length = 0;
	AzureFileInfo cached;

	SECTION("Blob") {
		TestBlobFileSystem fs(global_cache);
		Azure::Storage::Blobs::BlobClientOptions options;
		options.Transport.Transport = transport;
		Azure::Storage::Blobs::BlockBlobClient client("https://account.blob.core.windows.net/container/file", options);
		const string path = "az://container/file";
		auto cache = GetCache(fs, context.opener, path, client.GetUrl());
		REQUIRE(cache.cache != global_cache);
		AzureBlobStorageFileHandle handle(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_WRITE, TestOptions(), cache,
		                                  client);
		handle.write_buffer = make_uniq_array<data_t>(4);
		memset(handle.write_buffer.get(), 'x', 4);
		handle.write_buffer_offset = 4;
		global_cache->Insert(cache.key, info, global_cache->GetGeneration());
		cache.cache->Insert(cache.key, info, cache.cache->GetGeneration());
		handle.Sync();
		CHECK(transport->stages == 1);
		CHECK(transport->commits == 1);
		CHECK(!global_cache->Find(cache.key, cached));
		CHECK(!cache.cache->Find(cache.key, cached));
		global_cache->Insert(cache.key, info, global_cache->GetGeneration());
		handle.Close();
		CHECK(!global_cache->Find(cache.key, cached));
		AzureBlobStorageFileHandle reader(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, TestOptions(), cache,
		                                  client);
		global_cache->Insert(cache.key, info, global_cache->GetGeneration());
		reader.Close();
		CHECK(global_cache->Find(cache.key, cached));
	}
	SECTION("DFS") {
		TestDfsFileSystem fs(global_cache);
		Azure::Storage::Files::DataLake::DataLakeClientOptions options;
		options.Transport.Transport = transport;
		Azure::Storage::Files::DataLake::DataLakeFileClient client(
		    "https://account.dfs.core.windows.net/container/file", options);
		const string path = "abfss://container/file";
		auto cache = GetCache(fs, context.opener, path, client.GetUrl());
		REQUIRE(cache.cache != global_cache);
		AzureDfsStorageFileHandle handle(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_WRITE, TestOptions(), cache,
		                                 client);
		handle.write_buffer = make_uniq_array<data_t>(4);
		memset(handle.write_buffer.get(), 'x', 4);
		handle.write_buffer_offset = 4;
		global_cache->Insert(cache.key, info, global_cache->GetGeneration());
		cache.cache->Insert(cache.key, info, cache.cache->GetGeneration());
		handle.Sync();
		CHECK(transport->stages == 1);
		CHECK(transport->commits == 1);
		CHECK(!global_cache->Find(cache.key, cached));
		CHECK(!cache.cache->Find(cache.key, cached));
		global_cache->Insert(cache.key, info, global_cache->GetGeneration());
		handle.Close();
		CHECK(transport->commits == 2);
		CHECK(!global_cache->Find(cache.key, cached));
	}
}

TEST_CASE("Azure metadata cache references can outlive their client context", "[azure_metadata_cache]") {
	auto global_cache = make_shared_ptr<AzureMetadataCache>(false);
	TestAzureFileSystem fs(global_cache);
	unique_ptr<TestAzureFileHandle> handle;
	AzureMetadataCacheHandle cache;
	{
		CacheTestContext context;
		cache =
		    fs.CacheFor(context.opener, "az://container/file", "https://account.blob.core.windows.net/container/file");
		handle =
		    make_uniq<TestAzureFileHandle>(fs, OpenFileInfo("az://container/file"), FileFlags::FILE_FLAGS_WRITE, cache);
	}
	AzureFileInfo info;
	cache.cache->Insert(cache.key, info, cache.cache->GetGeneration());
	global_cache->Insert(cache.key, info, global_cache->GetGeneration());
	handle->Close();
	AzureFileInfo cached;
	CHECK(!cache.cache->Find(cache.key, cached));
	CHECK(!global_cache->Find(cache.key, cached));
}

class InterleavedTestFileSystem : public TestAzureFileSystem {
public:
	using TestAzureFileSystem::TestAzureFileSystem;
	std::function<void()> after_properties;

protected:
	void LoadRemoteFileInfo(AzureFileHandle &handle) override {
		TestAzureFileSystem::LoadRemoteFileInfo(handle);
		if (after_properties) {
			auto callback = std::move(after_properties);
			// A moved-from std::function can remain callable, including with libc++ inline targets.
			after_properties = nullptr;
			callback();
		}
	}
};

TEST_CASE("Azure in-flight metadata is not cached after write invalidation", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		for (bool dfs_writer : {false, true}) {
			INFO("shared cache: " << shared_cache << ", DFS writer: " << dfs_writer);
			CacheTestContext context;
			Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
			Execute(context.con,
			        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
			auto global = make_shared_ptr<AzureMetadataCache>(false);
			InterleavedTestFileSystem reader_fs(global);
			TestBlobFileSystem blob_fs(global);
			TestDfsFileSystem dfs_fs(global);
			const string path = "az://container/file";
			const string url = "https://account.blob.core.windows.net/container/file";
			auto cache = GetCache(reader_fs, context.opener, path, url);
			auto transport = std::make_shared<CommitTransport>();
			auto options = TestOptions();
			options.write_block_size = 256;
			unique_ptr<AzureFileHandle> writer;
			if (dfs_writer) {
				Azure::Storage::Files::DataLake::DataLakeClientOptions client_options;
				client_options.Transport.Transport = transport;
				Azure::Storage::Files::DataLake::DataLakeFileClient client(
				    "https://account.dfs.core.windows.net/container/file", client_options);
				const string writer_path = "abfss://container/file";
				writer = make_uniq<AzureDfsStorageFileHandle>(
				    dfs_fs, OpenFileInfo(writer_path), FileFlags::FILE_FLAGS_WRITE, options,
				    GetCache(dfs_fs, context.opener, writer_path, client.GetUrl()), client);
			} else {
				Azure::Storage::Blobs::BlobClientOptions client_options;
				client_options.Transport.Transport = transport;
				Azure::Storage::Blobs::BlockBlobClient client(url, client_options);
				writer = make_uniq<AzureBlobStorageFileHandle>(blob_fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_WRITE,
				                                               options, GetCache(blob_fs, context.opener, path, url),
				                                               client);
			}
			vector<data_t> data(200, 'x');
			writer->file_system.Write(*writer, data.data(), data.size());
			reader_fs.after_properties = [&]() {
				// Complete a rewrite after the reader receives old properties but before it publishes them.
				reader_fs.remote_length = 200;
				writer->Close();
			};
			TestAzureFileHandle in_flight(reader_fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(in_flight.PostConstruct());
			CHECK(in_flight.length == 100);
			CHECK(transport->commits == 1);
			AzureFileInfo cached;
			CHECK(!cache.cache->Find(cache.key, cached));
			TestAzureFileHandle next_reader(reader_fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(next_reader.PostConstruct());
			CHECK(next_reader.length == 200);
			CHECK(reader_fs.remote_loads == 2);
			TestAzureFileHandle cached_reader(reader_fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(cached_reader.PostConstruct());
			CHECK(cached_reader.length == 200);
			CHECK(reader_fs.remote_loads == 2);
		}
	}
}

TEST_CASE("Azure prefilled metadata stays on its handle across invalidation", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto global = make_shared_ptr<AzureMetadataCache>(false);
	TestAzureFileSystem fs(global);
	const string path = "az://container/file";
	auto cache = GetCache(fs, context.opener, path, "https://account.blob.core.windows.net/container/file");
	TestAzureFileHandle prefilled(fs, PrefilledInfo(path), FileFlags::FILE_FLAGS_READ, cache);
	TestAzureFileHandle writer(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_WRITE, cache);
	writer.Close();
	REQUIRE(prefilled.PostConstruct());
	CHECK(prefilled.length == 42);
	AzureFileInfo cached;
	CHECK(!cache.cache->Find(cache.key, cached));
	TestAzureFileHandle next_reader(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
	REQUIRE(next_reader.PostConstruct());
	CHECK(next_reader.length == 100);
}

TEST_CASE("Azure query end rejects metadata publication already in flight", "[azure_metadata_cache]") {
	CacheTestContext context;
	InterleavedTestFileSystem fs(make_shared_ptr<AzureMetadataCache>(false));
	const string path = "az://container/file";
	auto cache = GetCache(fs, context.opener, path, "https://account.blob.core.windows.net/container/file");
	fs.after_properties = [&]() {
		cache.cache->QueryEnd(*context.con.context);
		fs.remote_length = 200;
	};
	TestAzureFileHandle in_flight(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
	REQUIRE(in_flight.PostConstruct());
	AzureFileInfo cached;
	CHECK(!cache.cache->Find(cache.key, cached));
	TestAzureFileHandle next_reader(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
	REQUIRE(next_reader.PostConstruct());
	CHECK(next_reader.length == 200);
}

class MetadataPropertiesTransport : public Azure::Core::Http::HttpTransport {
public:
	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     const Azure::Core::Context &context) override {
		if (request.GetMethod() != Azure::Core::Http::HttpMethod::Head) {
			throw InternalException("Unexpected request in metadata properties test");
		}
		requests++;
		auto response =
		    std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
		response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(nullptr, 0));
		response->SetHeader("content-length", directory ? "0" : "100");
		response->SetHeader("etag", "\"file-etag\"");
		response->SetHeader("last-modified", "Wed, 07 Oct 2026 12:00:00 GMT");
		response->SetHeader("x-ms-creation-time", "Wed, 07 Oct 2026 12:00:00 GMT");
		response->SetHeader("x-ms-blob-type", "BlockBlob");
		response->SetHeader("x-ms-request-id", "metadata-cache-test");
		response->SetHeader("x-ms-server-encrypted", "true");
		if (directory) {
			response->SetHeader("x-ms-meta-hdi_isfolder", "true");
		}
		return response;
	}
	idx_t requests = 0;
	bool directory = false;
};

class MetadataTestDfsFileSystem : public TestDfsFileSystem {
public:
	MetadataTestDfsFileSystem(shared_ptr<AzureMetadataCache> cache,
	                          std::shared_ptr<Azure::Core::Http::HttpTransport> transport)
	    : TestDfsFileSystem(std::move(cache)), transport(std::move(transport)) {
	}

protected:
	shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener>, const string &,
	                                                   const AzureParsedUrl &) override {
		Azure::Storage::Files::DataLake::DataLakeClientOptions options;
		options.Transport.Transport = transport;
		Azure::Storage::Files::DataLake::DataLakeServiceClient service("https://account.dfs.core.windows.net", options);
		return make_shared_ptr<AzureDfsContextState>(service, TestOptions());
	}

private:
	std::shared_ptr<Azure::Core::Http::HttpTransport> transport;
};

TEST_CASE("Azure DFS directory hints do not replace the normal object's metadata", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		for (bool hint_first : {false, true}) {
			INFO("shared cache: " << shared_cache << ", hint first: " << hint_first);
			CacheTestContext context;
			Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
			Execute(context.con,
			        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
			auto transport = std::make_shared<MetadataPropertiesTransport>();
			MetadataTestDfsFileSystem fs(make_shared_ptr<AzureMetadataCache>(false), transport);
			context.con.context->RunFunctionInTransaction([&]() {
				if (!hint_first) {
					REQUIRE(fs.FileExists("abfss://container/file", &context.opener));
				}
				CHECK(fs.DirectoryExists("abfss://container/file/", &context.opener));
				CHECK(fs.FileExists("abfss://container/file", &context.opener));
				auto normal = fs.OpenFile("abfss://container/file", FileFlags::FILE_FLAGS_READ, &context.opener);
				REQUIRE(normal);
				CHECK(normal->Cast<AzureDfsStorageFileHandle>().GetType() == FileType::FILE_TYPE_REGULAR);
				CHECK(fs.GetFileSize(*normal) == 100);
				CHECK(transport->requests == 2);
			});
		}
	}
}

TEST_CASE("Azure DFS real directories remain directories with either path spelling", "[azure_metadata_cache]") {
	CacheTestContext context;
	Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
	Execute(context.con, "SET enable_http_metadata_cache = true");
	auto transport = std::make_shared<MetadataPropertiesTransport>();
	transport->directory = true;
	MetadataTestDfsFileSystem fs(make_shared_ptr<AzureMetadataCache>(false), transport);
	context.con.context->RunFunctionInTransaction([&]() {
		CHECK(fs.DirectoryExists("abfss://container/dir/", &context.opener));
		CHECK(fs.DirectoryExists("abfss://container/dir", &context.opener));
		CHECK(!fs.FileExists("abfss://container/dir", &context.opener));
		auto handle = fs.OpenFile("abfss://container/dir", FileFlags::FILE_FLAGS_READ, &context.opener);
		REQUIRE(handle);
		CHECK(handle->Cast<AzureDfsStorageFileHandle>().GetType() == FileType::FILE_TYPE_DIR);
		CHECK(fs.GetFileSize(*handle) == 0);
		CHECK(transport->requests == 2);
	});
}

class MetadataListingTransport : public MetadataPropertiesTransport {
public:
	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     const Azure::Core::Context &context) override {
		if (request.GetMethod() != Azure::Core::Http::HttpMethod::Get) {
			return MetadataPropertiesTransport::Send(request, context);
		}
		auto response =
		    std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
		response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(
		    reinterpret_cast<const uint8_t *>(body.data()), body.size()));
		response->SetHeader("content-length", std::to_string(body.size()));
		response->SetHeader("content-type", "application/xml");
		return response;
	}

private:
	std::string body = R"(<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://account.blob.core.windows.net/" ContainerName="container">
<Prefix></Prefix><Blobs><Blob><Name>marker/</Name><Properties>
<Creation-Time>Wed, 07 Oct 2026 12:00:00 GMT</Creation-Time>
<Last-Modified>Wed, 07 Oct 2026 12:00:00 GMT</Last-Modified><Etag>"file-etag"</Etag>
<Content-Length>100</Content-Length><BlobType>BlockBlob</BlobType><ServerEncrypted>true</ServerEncrypted>
</Properties></Blob></Blobs><NextMarker></NextMarker></EnumerationResults>)";
};

class MetadataTestBlobFileSystem : public TestBlobFileSystem {
public:
	MetadataTestBlobFileSystem(shared_ptr<AzureMetadataCache> cache,
	                           std::shared_ptr<Azure::Core::Http::HttpTransport> transport)
	    : TestBlobFileSystem(std::move(cache)), transport(std::move(transport)) {
	}
	using AzureStorageFileSystem::OpenFileExtended;

protected:
	shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener>, const string &,
	                                                   const AzureParsedUrl &) override {
		Azure::Storage::Blobs::BlobClientOptions options;
		options.Transport.Transport = transport;
		Azure::Storage::Blobs::BlobServiceClient service("https://account.blob.core.windows.net", options);
		return make_shared_ptr<AzureBlobContextState>(service, TestOptions());
	}

private:
	std::shared_ptr<Azure::Core::Http::HttpTransport> transport;
};

TEST_CASE("Azure Blob wildcard metadata retains trailing-slash directory semantics", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		INFO("shared cache: " << shared_cache);
		CacheTestContext context;
		Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
		Execute(context.con,
		        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
		auto transport = std::make_shared<MetadataListingTransport>();
		MetadataTestBlobFileSystem fs(make_shared_ptr<AzureMetadataCache>(false), transport);
		context.con.context->RunFunctionInTransaction([&]() {
			auto listing = fs.Glob("az://container/**", &context.opener);
			REQUIRE(listing.size() == 1);
			REQUIRE(listing[0].path == "az://container/marker/");
			auto from_listing = fs.OpenFileExtended(listing[0], FileFlags::FILE_FLAGS_READ, &context.opener);
			REQUIRE(from_listing);
			CHECK(from_listing->Cast<AzureBlobStorageFileHandle>().GetType() == FileType::FILE_TYPE_DIR);
			CHECK(fs.GetFileSize(*from_listing) == 0);
			auto ordinary = fs.OpenFile("az://container/marker/", FileFlags::FILE_FLAGS_READ, &context.opener);
			REQUIRE(ordinary);
			CHECK(ordinary->Cast<AzureBlobStorageFileHandle>().GetType() == FileType::FILE_TYPE_DIR);
			CHECK(fs.GetFileSize(*ordinary) == 0);
		});
	}
}

// Exercise real SDK requests and filesystem entry points while controlling mutation timing.
class MetadataMutationTransport : public Azure::Core::Http::HttpTransport {
public:
	idx_t length = 100;
	idx_t heads = 0;
	idx_t deletes = 0;
	bool exists = true;
	bool directory = false;
	std::function<void()> during_head;
	vector<string> listing_names {"file"};

	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     const Azure::Core::Context &) override {
		using Azure::Core::Http::HttpMethod;
		using Azure::Core::Http::HttpStatusCode;
		const auto query = request.GetUrl().GetQueryParameters();
		auto status = HttpStatusCode::Ok;
		const auto response_length = length;
		if (request.GetMethod() == HttpMethod::Head) {
			heads++;
			if (!exists) {
				status = HttpStatusCode::NotFound;
			}
			if (during_head) {
				auto callback = std::move(during_head);
				during_head = nullptr;
				callback();
			}
		} else if (request.GetMethod() == HttpMethod::Get && query.count("comp") && query.at("comp") == "list") {
			listing_body = "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults "
			               "ServiceEndpoint=\"https://account.blob.core.windows.net/\" ContainerName=\"container\">"
			               "<Prefix></Prefix><Blobs>";
			for (const auto &name : listing_names) {
				listing_body +=
				    "<Blob><Name>" + name +
				    "</Name><Properties>"
				    "<Creation-Time>Wed, 07 Oct 2026 12:00:00 GMT</Creation-Time>"
				    "<Last-Modified>Wed, 07 Oct 2026 12:00:00 GMT</Last-Modified><Etag>\"listed-etag\"</Etag>"
				    "<Content-Length>" +
				    to_string(length) +
				    "</Content-Length><BlobType>BlockBlob</BlobType>"
				    "<ServerEncrypted>true</ServerEncrypted></Properties></Blob>";
			}
			listing_body += "</Blobs><NextMarker></NextMarker></EnumerationResults>";
		} else if (request.GetMethod() == HttpMethod::Put && query.count("resource")) {
			status = HttpStatusCode::Created;
			exists = true;
			directory = query.at("resource") == "directory";
			length = 0;
		} else if (request.GetMethod() == HttpMethod::Put && query.count("comp")) {
			status = HttpStatusCode::Created;
			if (query.at("comp") == "block") {
				staged_length += request.GetBodyStream()->Length();
			} else if (query.at("comp") == "blocklist") {
				exists = true;
				length = staged_length;
			} else {
				throw InternalException("Unexpected Blob mutation in metadata test");
			}
		} else if (request.GetMethod() == HttpMethod::Patch && query.count("action")) {
			if (query.at("action") == "append") {
				status = HttpStatusCode::Accepted;
			} else if (query.at("action") == "flush") {
				length = std::stoull(query.at("position"));
			} else {
				throw InternalException("Unexpected DFS mutation in metadata test");
			}
		} else if (request.GetMethod() == HttpMethod::Delete) {
			status = exists ? HttpStatusCode::Accepted : HttpStatusCode::NotFound;
			deletes += exists ? 1 : 0;
			exists = false;
		} else {
			throw InternalException("Unexpected request in metadata mutation test");
		}
		auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, status, "OK");
		response->SetHeader("content-length",
		                    request.GetMethod() == HttpMethod::Head && exists
		                        ? to_string(response_length)
		                        : (request.GetMethod() == HttpMethod::Get ? to_string(listing_body.size()) : "0"));
		response->SetHeader("etag", "\"file-etag\"");
		response->SetHeader("last-modified", "Wed, 07 Oct 2026 12:00:00 GMT");
		response->SetHeader("x-ms-creation-time", "Wed, 07 Oct 2026 12:00:00 GMT");
		response->SetHeader("x-ms-blob-type", "BlockBlob");
		response->SetHeader("x-ms-request-id", "metadata-mutation-test");
		response->SetHeader("x-ms-server-encrypted", "true");
		response->SetHeader("x-ms-request-server-encrypted", "true");
		if (directory) {
			response->SetHeader("x-ms-meta-hdi_isfolder", "true");
		}
		if (status == HttpStatusCode::NotFound) {
			response->SetHeader("x-ms-error-code", request.GetUrl().GetHost().find(".dfs.") != string::npos
			                                           ? "PathNotFound"
			                                           : "BlobNotFound");
		}
		if (request.GetMethod() == HttpMethod::Get) {
			response->SetHeader("content-type", "application/xml");
			response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(
			    reinterpret_cast<const uint8_t *>(listing_body.data()), listing_body.size()));
		} else {
			response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(nullptr, 0));
		}
		return response;
	}

private:
	idx_t staged_length = 0;
	string listing_body;
};

TEST_CASE("Azure caller metadata neither seeds nor replaces cached remote metadata", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		for (const auto &scheme : {"az://", "abfss://"}) {
			INFO("shared cache: " << shared_cache << ", scheme: " << scheme);
			CacheTestContext context;
			Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
			Execute(context.con,
			        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
			auto global = make_shared_ptr<AzureMetadataCache>(false);
			TestAzureFileSystem fs(global);
			const string path = string(scheme) + "container/file";
			auto cache = GetCache(fs, context.opener, path, "https://account.blob.core.windows.net/container/file");
			TestAzureFileHandle prefilled(fs, PrefilledInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(prefilled.PostConstruct());
			CHECK(prefilled.length == 42);
			CHECK(fs.remote_loads == 0);
			AzureFileInfo cached;
			CHECK(!cache.cache->Find(cache.key, cached));
			TestAzureFileHandle remote(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(remote.PostConstruct());
			CHECK(remote.length == 100);
			TestAzureFileHandle another_prefill(fs, PrefilledInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(another_prefill.PostConstruct());
			CHECK(another_prefill.length == 42);
			TestAzureFileHandle next(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, cache);
			REQUIRE(next.PostConstruct());
			CHECK(next.length == 100);
			CHECK(fs.remote_loads == 1);
		}
	}
}

TEST_CASE("Azure old listings do not repopulate metadata after a completed rewrite", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		INFO("shared cache: " << shared_cache);
		CacheTestContext context;
		Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
		Execute(context.con,
		        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
		auto global = make_shared_ptr<AzureMetadataCache>(false);
		auto transport = std::make_shared<MetadataMutationTransport>();
		MetadataTestBlobFileSystem blob_fs(global, transport);
		MetadataTestDfsFileSystem dfs_fs(global, transport);
		context.con.context->RunFunctionInTransaction([&]() {
			auto listing = blob_fs.Glob("az://container/*", &context.opener);
			REQUIRE(listing.size() == 1);
			auto writer =
			    blob_fs.OpenFile("az://container/file",
			                     FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW, &context.opener);
			REQUIRE(writer);
			vector<data_t> data(200, 'x');
			blob_fs.Write(*writer, data.data(), data.size());
			writer->Close();
			REQUIRE(transport->length == 200);
			auto heads = transport->heads;
			auto prefilled = blob_fs.OpenFileExtended(listing[0], FileFlags::FILE_FLAGS_READ, &context.opener);
			REQUIRE(prefilled);
			CHECK(blob_fs.GetFileSize(*prefilled) == 100);
			CHECK(transport->heads == heads);
			auto plain = dfs_fs.OpenFile("abfss://container/file", FileFlags::FILE_FLAGS_READ, &context.opener);
			REQUIRE(plain);
			CHECK(dfs_fs.GetFileSize(*plain) == 200);
			CHECK(transport->heads == heads + 1);
			auto cached = blob_fs.OpenFile("azure://container/file", FileFlags::FILE_FLAGS_READ, &context.opener);
			REQUIRE(cached);
			CHECK(blob_fs.GetFileSize(*cached) == 200);
			CHECK(transport->heads == heads + 1);
		});
	}
}

TEST_CASE("Azure truncate on open rejects properties loaded during the write open", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		for (bool dfs_writer : {false, true}) {
			for (bool dfs_reader : {false, true}) {
				INFO("shared cache: " << shared_cache << ", DFS writer: " << dfs_writer
				                      << ", DFS reader: " << dfs_reader);
				CacheTestContext context;
				Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
				Execute(context.con, shared_cache ? "SET enable_http_metadata_cache = true"
				                                  : "SET enable_http_metadata_cache = false");
				auto global = make_shared_ptr<AzureMetadataCache>(false);
				auto transport = std::make_shared<MetadataMutationTransport>();
				MetadataTestBlobFileSystem blob_fs(global, transport);
				MetadataTestDfsFileSystem dfs_fs(global, transport);
				AzureStorageFileSystem &writer_fs =
				    dfs_writer ? static_cast<AzureStorageFileSystem &>(dfs_fs) : blob_fs;
				AzureStorageFileSystem &reader_fs =
				    dfs_reader ? static_cast<AzureStorageFileSystem &>(dfs_fs) : blob_fs;
				const string writer_path = dfs_writer ? "abfss://container/file" : "az://container/file";
				const string reader_path = dfs_reader ? "abfss://container/file" : "az://container/file";
				context.con.context->RunFunctionInTransaction([&]() {
					transport->during_head = [&]() {
						auto overlapping = reader_fs.OpenFile(reader_path, FileFlags::FILE_FLAGS_READ, &context.opener);
						REQUIRE(overlapping);
						REQUIRE(reader_fs.GetFileSize(*overlapping) == 100);
					};
					auto writer = writer_fs.OpenFile(
					    writer_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
					    &context.opener);
					REQUIRE(writer);
					REQUIRE(transport->length == 0);
					CHECK(writer_fs.GetFileSize(*writer) == 0);
					auto plain = reader_fs.OpenFile(reader_path, FileFlags::FILE_FLAGS_READ, &context.opener);
					REQUIRE(plain);
					CHECK(reader_fs.GetFileSize(*plain) == 0);
					CHECK(transport->heads == 3);
					auto cached = reader_fs.OpenFile(reader_path, FileFlags::FILE_FLAGS_READ, &context.opener);
					REQUIRE(cached);
					CHECK(reader_fs.GetFileSize(*cached) == 0);
					CHECK(transport->heads == 3);
					writer->Close();
				});
			}
		}
	}
}

TEST_CASE("Azure synthetic Blob directories do not replace same-name blobs in the cache", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		for (bool normal_first : {false, true}) {
			INFO("shared cache: " << shared_cache << ", normal first: " << normal_first);
			CacheTestContext context;
			Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
			Execute(context.con,
			        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
			auto transport = std::make_shared<MetadataMutationTransport>();
			transport->listing_names = {"dir", "dir/child"};
			MetadataTestBlobFileSystem fs(make_shared_ptr<AzureMetadataCache>(false), transport);
			context.con.context->RunFunctionInTransaction([&]() {
				if (normal_first) {
					auto ordinary = fs.OpenFile("az://container/dir", FileFlags::FILE_FLAGS_READ, &context.opener);
					REQUIRE(ordinary);
					REQUIRE(fs.GetFileSize(*ordinary) == 100);
				}
				OpenFileInfo directory_info("az://container/dir");
				REQUIRE(fs.ListFilesExtended(
				    "az://container/",
				    [&](OpenFileInfo &child) {
					    if (FileSystem::IsDirectory(child)) {
						    directory_info.extended_info = child.extended_info;
					    }
				    },
				    &context.opener));
				REQUIRE(directory_info.extended_info);
				auto prefilled = fs.OpenFileExtended(directory_info, FileFlags::FILE_FLAGS_READ, &context.opener);
				REQUIRE(prefilled);
				CHECK(prefilled->Cast<AzureFileHandle>().GetType() == FileType::FILE_TYPE_DIR);
				auto ordinary = fs.OpenFile("az://container/dir", FileFlags::FILE_FLAGS_READ, &context.opener);
				REQUIRE(ordinary);
				CHECK(ordinary->Cast<AzureFileHandle>().GetType() == FileType::FILE_TYPE_REGULAR);
				CHECK(fs.GetFileSize(*ordinary) == 100);
				CHECK(fs.Glob("az://container/dir", &context.opener).size() == 1);
				CHECK(transport->heads == 1);
			});
		}
	}
}

TEST_CASE("Azure delete entry points invalidate local and shared metadata", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		for (bool dfs : {false, true}) {
			for (bool if_exists : {false, true}) {
				INFO("shared cache: " << shared_cache << ", DFS: " << dfs << ", if exists: " << if_exists);
				CacheTestContext context;
				Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
				Execute(context.con, shared_cache ? "SET enable_http_metadata_cache = true"
				                                  : "SET enable_http_metadata_cache = false");
				auto global = make_shared_ptr<AzureMetadataCache>(false);
				auto transport = std::make_shared<MetadataMutationTransport>();
				MetadataTestBlobFileSystem blob_fs(global, transport);
				MetadataTestDfsFileSystem dfs_fs(global, transport);
				AzureStorageFileSystem &fs = dfs ? static_cast<AzureStorageFileSystem &>(dfs_fs) : blob_fs;
				const string path = dfs ? "abfss://container/file" : "az://container/file";
				auto cache =
				    dfs ? GetCache(dfs_fs, context.opener, path, "https://account.dfs.core.windows.net/container/file")
				        : GetCache(blob_fs, context.opener, path,
				                   "https://account.blob.core.windows.net/container/file");
				context.con.context->RunFunctionInTransaction([&]() {
					auto reader = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, &context.opener);
					REQUIRE(reader);
					AzureFileInfo info;
					REQUIRE(cache.cache->Find(cache.key, info));
					global->Insert(cache.key, info, global->GetGeneration());
					if (if_exists) {
						CHECK(fs.TryRemoveFile(path, &context.opener));
					} else {
						fs.RemoveFile(path, &context.opener);
					}
					CHECK(transport->deletes == 1);
					CHECK(!cache.cache->Find(cache.key, info));
					CHECK(!global->Find(cache.key, info));
					CHECK(!fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS,
					                   &context.opener));
					CHECK(!fs.TryRemoveFile(path, &context.opener));
				});
			}
		}
	}
}

TEST_CASE("Azure directory creation invalidates local and shared object metadata", "[azure_metadata_cache]") {
	for (bool shared_cache : {false, true}) {
		INFO("shared cache: " << shared_cache);
		CacheTestContext context;
		Execute(context.con, "CREATE SECRET s1 (TYPE AZURE, ACCOUNT_NAME 'account')");
		Execute(context.con,
		        shared_cache ? "SET enable_http_metadata_cache = true" : "SET enable_http_metadata_cache = false");
		auto global = make_shared_ptr<AzureMetadataCache>(false);
		auto transport = std::make_shared<MetadataMutationTransport>();
		transport->exists = false;
		MetadataTestDfsFileSystem dfs_fs(global, transport);
		MetadataTestBlobFileSystem blob_fs(global, transport);
		const string path = "abfss://container/dir";
		auto cache = GetCache(dfs_fs, context.opener, path, "https://account.dfs.core.windows.net/container/dir");
		AzureFileInfo stale;
		stale.file_type = FileType::FILE_TYPE_REGULAR;
		stale.length = 100;
		cache.cache->Insert(cache.key, stale, cache.cache->GetGeneration());
		global->Insert(cache.key, stale, global->GetGeneration());
		context.con.context->RunFunctionInTransaction([&]() {
			dfs_fs.CreateDirectory(path, &context.opener);
			CHECK(!cache.cache->Find(cache.key, stale));
			CHECK(!global->Find(cache.key, stale));
			CHECK(dfs_fs.DirectoryExists(path, &context.opener));
			CHECK(!dfs_fs.FileExists(path, &context.opener));
			CHECK(blob_fs.Glob("az://container/dir", &context.opener).empty());
			CHECK(transport->heads == 1);
		});
	}
}
