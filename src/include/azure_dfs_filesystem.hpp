#pragma once

#include "azure_filesystem.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"

#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_file_system_client.hpp>
#include <azure/storage/files/datalake/datalake_service_client.hpp>
#include <string>
#include <vector>

namespace duckdb {

class AzureDfsContextState : public AzureContextState {
public:
	AzureDfsContextState(Azure::Storage::Files::DataLake::DataLakeServiceClient client,
	                     const AzureReadOptions &azure_read_options);
	Azure::Storage::Files::DataLake::DataLakeFileSystemClient
	GetDfsFileSystemClient(const std::string &file_system_name) const;

private:
	Azure::Storage::Files::DataLake::DataLakeServiceClient service_client;
};

class AzureDfsStorageFileSystem;

class AzureDfsStorageFileHandle : public AzureFileHandle {
public:
	AzureDfsStorageFileHandle(AzureDfsStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
	                          const AzureReadOptions &read_options,
	                          Azure::Storage::Files::DataLake::DataLakeFileClient client);
	~AzureDfsStorageFileHandle() override = default;

	void Close() override;

public:
	Azure::Storage::Files::DataLake::DataLakeFileClient file_client;
};

class AzureDfsStorageFileSystem : public AzureStorageFileSystem {
public:
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool ListFilesExtended(const string &path_in, const std::function<void(OpenFileInfo &info)> &callback,
	                       optional_ptr<FileOpener> opener) override;
	bool SupportsListFilesExtended() const override {
		return true;
	}

	bool CanHandleFile(const string &fpath) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	bool DirectoryExists(const string &filename, optional_ptr<FileOpener> opener) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override;
	string GetName() const override {
		return "AzureDfsStorageFileSystem";
	}

	// From AzureFilesystem
	void LoadRemoteFileInfo(AzureFileHandle &handle) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void FileSync(FileHandle &handle) override;

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;
	virtual bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) override;

public:
	static const string SCHEME;
	static const string PATH_PREFIX;
	static const string UNSECURE_SCHEME;
	static const string UNSECURE_PATH_PREFIX;

protected:
	// From AzureFilesystem
	const string &GetContextPrefix() const override {
		return PATH_PREFIX;
	}
	shared_ptr<AzureContextState> CreateStorageContext(optional_ptr<FileOpener> opener, const string &path,
	                                                   const AzureParsedUrl &parsed_url) override;
	unique_ptr<AzureFileHandle> CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
	                                         optional_ptr<FileOpener> opener) override;

	void ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out, idx_t buffer_out_len) override;
};

} // namespace duckdb
