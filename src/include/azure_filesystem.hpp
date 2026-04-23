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

struct AzureReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
};

struct AzureFileInfo {
	FileType file_type = FileType::FILE_TYPE_INVALID;
	idx_t length = 0;
	timestamp_t last_modified;
};

class AzureMetadataCache : public ClientContextState {
public:
	explicit AzureMetadataCache(bool flush_on_query_end_p, bool shared_p)
	    : flush_on_query_end(flush_on_query_end_p), shared(shared_p) {
	}

	void Insert(const string &path, const AzureFileInfo &val) {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			map[path] = val;
		} else {
			map[path] = val;
		}
	}

	void Erase(const string &path) {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			map.erase(path);
		} else {
			map.erase(path);
		}
	}

	bool Find(const string &path, AzureFileInfo &ret_val) {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			auto lookup = map.find(path);
			if (lookup == map.end()) {
				return false;
			}
			ret_val = lookup->second;
			return true;
		}
		auto lookup = map.find(path);
		if (lookup == map.end()) {
			return false;
		}
		ret_val = lookup->second;
		return true;
	}

	void Clear() {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			map.clear();
		} else {
			map.clear();
		}
	}

	void QueryEnd(ClientContext &context) override {
		if (flush_on_query_end) {
			Clear();
		}
	}

private:
	mutex lock;
	unordered_map<string, AzureFileInfo> map;
	bool flush_on_query_end;
	bool shared;
};

class AzureContextState : public ClientContextState {
public:
	const AzureReadOptions read_options;

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
	AzureContextState(const AzureReadOptions &read_options);

protected:
	bool is_valid;
};

class AzureStorageFileSystem;

class AzureFileHandle : public FileHandle {
public:
	virtual bool PostConstruct();
	void SetFileInfo(FileType file_type_p, idx_t length_p, timestamp_t last_modified_p);

	bool IsRemoteLoaded() {
		return is_remote_loaded;
	}

	FileType GetType() {
		return file_type;
	}

protected:
	AzureFileHandle(AzureStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags, FileType file_type,
	                const AzureReadOptions &read_options, optional_ptr<AzureMetadataCache> metadata_cache);

public:
	FileOpenFlags flags;

	// File info
	bool is_remote_loaded;
	FileType file_type;
	idx_t length;
	timestamp_t last_modified;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	const AzureReadOptions read_options;
	optional_ptr<AzureMetadataCache> metadata_cache;
};

class AzureStorageFileSystem : public FileSystem {
public:
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
	static AzureReadOptions ParseAzureReadOptions(optional_ptr<FileOpener> opener);
	static bool ParseAzureMetadataCacheEnabled(optional_ptr<FileOpener> opener);
	optional_ptr<AzureMetadataCache> GetMetadataCache(optional_ptr<FileOpener> opener);
	void InvalidateMetadata(optional_ptr<FileOpener> opener, const string &path);

public:
	static timestamp_t ToTimestamp(const Azure::DateTime &dt);

private:
	optional_ptr<AzureMetadataCache> GetGlobalMetadataCache();
	mutex global_cache_lock;
	duckdb::unique_ptr<AzureMetadataCache> global_metadata_cache;
};

} // namespace duckdb
