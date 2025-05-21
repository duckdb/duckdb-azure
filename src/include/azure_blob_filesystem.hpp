#pragma once

#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "azure_parsed_url.hpp"
#include "azure_filesystem.hpp"
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_service_client.hpp>
#include <string>

namespace duckdb {

class AzureBlobContextState : public AzureContextState {
public:
	AzureBlobContextState(Azure::Storage::Blobs::BlobServiceClient client, const AzureReadOptions &azure_read_options);
	Azure::Storage::Blobs::BlobContainerClient GetBlobContainerClient(const std::string &blobContainerName) const;
	~AzureBlobContextState() override = default;

private:
	Azure::Storage::Blobs::BlobServiceClient service_client;
};

class AzureBlobStorageFileSystem;

class AzureBlobStorageFileHandle : public AzureFileHandle {
public:
	AzureBlobStorageFileHandle(AzureBlobStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
	                           const AzureReadOptions &read_options, Azure::Storage::Blobs::BlobClient blob_client);
	~AzureBlobStorageFileHandle() override = default;

public:
	Azure::Storage::Blobs::BlobClient blob_client;
};

class AzureBlobStorageFileSystem : public AzureStorageFileSystem {
public:
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	// FS methods
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool CanHandleFile(const string &fpath) override;
	string GetName() const override {
		return "AzureBlobStorageFileSystem";
	}

	// From AzureFilesystem
	void LoadRemoteFileInfo(AzureFileHandle &handle) override;

public:
	static const string SCHEME;
	static const string SHORT_SCHEME;

	static const string PATH_PREFIX;
	static const string SHORT_PATH_PREFIX;

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
