#pragma once

#include "azure_parsed_url.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <azure/core/datetime.hpp>
#include <cstdint>
#include <ctime>
namespace duckdb {

struct AzureOptions {
	// 8 MiB - Base block size, copies rclone's default for concurrent transfers
	// 4000 MiB - Azure doc'd max per Append/StageBlock
	static const idx_t WRITE_BLOCK_SIZE_DEFAULT = (idx_t)8 * 1024 * 1024;
	static const idx_t WRITE_BLOCK_SIZE_MAX = (idx_t)4000 * 1024 * 1024;

	int32_t read_transfer_concurrency = 5;
	int64_t read_transfer_chunk_size = (int64_t)8 * 1024 * 1024;
	idx_t read_buffer_size = (idx_t)8 * 1024 * 1024;
	idx_t write_block_size = WRITE_BLOCK_SIZE_DEFAULT;
	idx_t write_staged_blocks_per_commit = 0;
};

struct AzureFileInfo {
	FileType file_type = FileType::FILE_TYPE_INVALID;
	idx_t length = 0;
	timestamp_t last_modified = timestamp_t(0);
	string etag;
};

struct AzureMetadataCacheKey {
	// Canonical endpoint identity shared by known Azure/OneLake Blob and DFS aliases.
	string endpoint_identity;
	string path;
	string snapshot;
	string version;

	bool operator==(const AzureMetadataCacheKey &other) const {
		return endpoint_identity == other.endpoint_identity && path == other.path && snapshot == other.snapshot &&
		       version == other.version;
	}

	bool operator!=(const AzureMetadataCacheKey &other) const {
		return !(*this == other);
	}
};

struct AzureMetadataCacheKeyHash {
	uint64_t operator()(const AzureMetadataCacheKey &key) const;
};

class AzureMetadataCache : public ClientContextState {
public:
	explicit AzureMetadataCache(bool flush_on_query_end_p) : flush_on_query_end(flush_on_query_end_p) {
	}

	uint64_t GetGeneration() {
		lock_guard<mutex> parallel_lock(lock);
		return generation;
	}

	void Insert(const AzureMetadataCacheKey &key, const AzureFileInfo &val, uint64_t expected_generation) {
		lock_guard<mutex> parallel_lock(lock);
		if (generation == expected_generation) {
			map[key] = val;
		}
	}

	void Erase(const AzureMetadataCacheKey &key) {
		lock_guard<mutex> parallel_lock(lock);
		generation++;
		map.erase(key);
	}

	bool Find(const AzureMetadataCacheKey &key, AzureFileInfo &ret_val) {
		lock_guard<mutex> parallel_lock(lock);
		auto lookup = map.find(key);
		if (lookup == map.end()) {
			return false;
		}
		ret_val = lookup->second;
		return true;
	}

	void Clear() {
		lock_guard<mutex> parallel_lock(lock);
		generation++;
		map.clear();
	}

	void QueryEnd(ClientContext &context) override {
		if (flush_on_query_end) {
			Clear();
		}
	}

private:
	// Query-local caches are still shared across parallel tasks within a query.
	mutex lock;
	unordered_map<AzureMetadataCacheKey, AzureFileInfo, AzureMetadataCacheKeyHash> map;
	// Any invalidation also rejects metadata loads already in flight.
	uint64_t generation = 0;
	bool flush_on_query_end;
};

struct AzureMetadataCacheHandle {
	shared_ptr<AzureMetadataCache> cache;
	shared_ptr<AzureMetadataCache> global_cache;
	AzureMetadataCacheKey key;

	void Invalidate() const {
		if (cache) {
			cache->Erase(key);
		}
		if (global_cache && global_cache != cache) {
			global_cache->Erase(key);
		}
	}
};

// A failed response cannot prove that the remote mutation was not applied.
class AzureMetadataCacheInvalidationGuard {
public:
	explicit AzureMetadataCacheInvalidationGuard(const AzureMetadataCacheHandle &cache_p) : cache(cache_p) {
	}
	~AzureMetadataCacheInvalidationGuard() {
		cache.Invalidate();
	}

private:
	const AzureMetadataCacheHandle &cache;
};

class AzureContextState : public ClientContextState {
public:
	const AzureOptions options;

public:
	virtual bool IsValid() const;
	void QueryEnd() override;

	template <class TARGET>
	TARGET &As() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &As() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	explicit AzureContextState(const AzureOptions &options);

protected:
	bool is_valid;
};

class AzureStorageFileSystem;

class AzureFileHandle : public FileHandle {
public:
	virtual bool PostConstruct();
	void SetFileInfo(FileType file_type_p, idx_t length_p, timestamp_t last_modified_p, const string &etag_p);
	void InvalidateMetadata();

	bool IsRemoteLoaded() {
		return is_remote_loaded;
	}

	FileType GetType() {
		return file_type;
	}

protected:
	AzureFileHandle(AzureStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags, FileType file_type,
	                const AzureOptions &options, AzureMetadataCacheHandle metadata_cache);

public:
	FileOpenFlags flags;

	// File info
	bool is_remote_loaded;
	FileType file_type;
	idx_t length;
	timestamp_t last_modified;
	string etag;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;
	const AzureOptions options;
	AzureMetadataCacheHandle metadata_cache;
	uint64_t metadata_cache_generation;
};

class AzureStorageFileSystem : public FileSystem {
public:
	explicit AzureStorageFileSystem(shared_ptr<AzureMetadataCache> global_metadata_cache);

	// FS methods
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return false;
	}
	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	string GetVersionTag(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;

	bool LoadFileInfo(AzureFileHandle &handle);

	string PathSeparator(const string &path) override {
		return "/";
	}

protected:
	unique_ptr<FileHandle> OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

	bool SupportsOpenFileExtended() const override {
		return true;
	}

	virtual duckdb::unique_ptr<AzureFileHandle> CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
	                                                         optional_ptr<FileOpener> opener) = 0;
	virtual void ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) = 0;

	virtual const string &GetContextPrefix() const = 0;
	shared_ptr<AzureContextState> GetOrCreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                        const AzureParsedUrl &parsed_url);
	virtual shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                           const AzureParsedUrl &parsed_url) = 0;

	virtual void LoadRemoteFileInfo(AzureFileHandle &handle) = 0;
	static AzureOptions ParseAzureOptions(optional_ptr<FileOpener> opener);
	static bool ParseAzureMetadataCacheEnabled(optional_ptr<FileOpener> opener);
	AzureMetadataCacheHandle GetMetadataCache(optional_ptr<FileOpener> opener, const string &path,
	                                          const AzureParsedUrl &parsed_url, const string &resolved_url);

public:
	static timestamp_t ToTimestamp(const Azure::DateTime &dt);
	static string StripETagQuotes(string etag);

private:
	shared_ptr<AzureMetadataCache> global_metadata_cache;
};

} // namespace duckdb
