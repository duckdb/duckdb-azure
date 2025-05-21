#pragma once

#include "azure_parsed_url.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_state.hpp"
#include <azure/core/datetime.hpp>
#include <ctime>
#include <cstdint>

namespace duckdb {

struct AzureReadOptions {
	int32_t transfer_concurrency = 5;
	int64_t transfer_chunk_size = 1 * 1024 * 1024;
	idx_t buffer_size = 1 * 1024 * 1024;
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
	void Close() override {
	}

protected:
	AzureFileHandle(AzureStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
	                const AzureReadOptions &read_options);

public:
	FileOpenFlags flags;

	// File info
	idx_t length;
	time_t last_modified;

	// Read buffer
	duckdb::unique_ptr<data_t[]> read_buffer;
	// Read info
	idx_t buffer_available;
	idx_t buffer_idx;
	idx_t file_offset;
	idx_t buffer_start;
	idx_t buffer_end;

	const AzureReadOptions read_options;
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
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void FileSync(FileHandle &handle) override;

	bool LoadFileInfo(AzureFileHandle &handle);

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

public:
	static time_t ToTimeT(const Azure::DateTime &dt);
};

} // namespace duckdb
