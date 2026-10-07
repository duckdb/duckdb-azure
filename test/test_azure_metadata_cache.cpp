#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "azure_blob_filesystem.hpp"
#include "azure_dfs_filesystem.hpp"
#include "azure_extension.hpp"
#include "duckdb/main/client_context_file_opener.hpp"

#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>

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
	CHECK(blob.key.account_or_onelake == "account.core.windows.net");
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
	blob.cache->Insert(blob.key, info);
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
		cache.Insert(keys[i], info);
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
	CHECK(cache.key.account_or_onelake == "onelake.fabric.microsoft.com");
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
	cache.cache->Insert(cache.key, info);
	AzureFileInfo cached;
	REQUIRE(blob.cache->Find(blob.key, cached));
	CHECK(cached.length == 42);
	blob.Invalidate();
	CHECK(!cache.cache->Find(cache.key, cached));
	info.length = 84;
	blob.cache->Insert(blob.key, info);
	REQUIRE(cache.cache->Find(cache.key, cached));
	CHECK(cached.length == 84);
	cache.Invalidate();
	CHECK(!blob.cache->Find(blob.key, cached));

	auto other = service.GetFileSystemClient("other-workspace").GetFileClient(item_path);
	auto other_cache =
	    GetCache(dfs_fs, context.opener, "abfss://other-workspace@onelake.dfs.fabric.microsoft.com/" + item_path,
	             other.GetUrl());
	CHECK(other_cache.key.account_or_onelake == cache.key.account_or_onelake);
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
	local.cache->Insert(local.key, info);
	global_cache->Insert(cache.key, info);
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
		CHECK(dfs.key.account_or_onelake == string(authority) + ".fabric.microsoft.com");
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
	CHECK(first.key.account_or_onelake == "onelake.fabric.microsoft.com");
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
		response->SetHeader("content-length", query["action"] == "flush" ? "4" : "0");
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
		global_cache->Insert(cache.key, info);
		cache.cache->Insert(cache.key, info);
		handle.Sync();
		CHECK(transport->stages == 1);
		CHECK(transport->commits == 1);
		CHECK(!global_cache->Find(cache.key, cached));
		CHECK(!cache.cache->Find(cache.key, cached));
		global_cache->Insert(cache.key, info);
		handle.Close();
		CHECK(!global_cache->Find(cache.key, cached));
		AzureBlobStorageFileHandle reader(fs, OpenFileInfo(path), FileFlags::FILE_FLAGS_READ, TestOptions(), cache,
		                                  client);
		global_cache->Insert(cache.key, info);
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
		global_cache->Insert(cache.key, info);
		cache.cache->Insert(cache.key, info);
		handle.Sync();
		CHECK(transport->stages == 1);
		CHECK(transport->commits == 1);
		CHECK(!global_cache->Find(cache.key, cached));
		CHECK(!cache.cache->Find(cache.key, cached));
		global_cache->Insert(cache.key, info);
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
	cache.cache->Insert(cache.key, info);
	global_cache->Insert(cache.key, info);
	handle->Close();
	AzureFileInfo cached;
	CHECK(!cache.cache->Find(cache.key, cached));
	CHECK(!global_cache->Find(cache.key, cached));
}
