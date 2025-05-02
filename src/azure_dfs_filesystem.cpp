#include "azure_dfs_filesystem.hpp"
#include "azure_storage_account_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include <algorithm>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <azure/storage/files/datalake/datalake_file_system_client.hpp>
#include <azure/storage/files/datalake/datalake_directory_client.hpp>
#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>
#include <azure/storage/files/datalake/datalake_responses.hpp>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {
const string AzureDfsStorageFileSystem::SCHEME = "abfss";
const string AzureDfsStorageFileSystem::PATH_PREFIX = "abfss://";
const string AzureDfsStorageFileSystem::UNSECURE_SCHEME = "abfs";
const string AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX = "abfs://";

inline static bool IsDfsScheme(const string &fpath) {
	return fpath.rfind(AzureDfsStorageFileSystem::PATH_PREFIX, 0) == 0 || fpath.rfind(AzureDfsStorageFileSystem::UNSECURE_PATH_PREFIX, 0) == 0;
}

static void Walk(const Azure::Storage::Files::DataLake::DataLakeFileSystemClient &fs, const std::string &path,
                 const string &path_pattern, std::size_t end_match,
                 std::vector<std::string> *out_result,
                 AzurePathMetadataCache *out_path_metadata_cache) {
	auto directory_client = fs.GetDirectoryClient(path);

	bool recursive = false;
	const auto double_star = path_pattern.rfind("**", end_match);
	if (double_star != std::string::npos) {
		if (path_pattern.length() > end_match) {
			throw NotImplementedException("abfss do not manage recursive lookup patterns, %s is therefor illegal, only "
			                              "pattern ending by ** are allowed.",
			                              path_pattern);
		}
		// pattern end with a **, perform recursive listing from this point
		recursive = true;
	}

	Azure::Storage::Files::DataLake::ListPathsOptions options;
	while (true) {
		auto res = directory_client.ListPaths(recursive, options);

		for (const auto &elt : res.Paths) {
			if (elt.IsDirectory) {
				if (!recursive) { // Only perform recursive call if we are not already processing recursive result
					if (Glob(elt.Name.data(), elt.Name.length(), path_pattern.data(), end_match)) {
						if (end_match >= path_pattern.length()) {
							// Skip, no way there will be matches anymore
							continue;
						}
						Walk(fs, elt.Name, path_pattern,
						     std::min(path_pattern.length(), path_pattern.find('/', end_match + 1)),
						     out_result,
						     out_path_metadata_cache);
					}
				}
			} else {
				// File
				if (Glob(elt.Name.data(), elt.Name.length(), path_pattern.data(), path_pattern.length())) {
					out_result->push_back(elt.Name);
					auto last_modified = AzureStorageFileSystem::ToTimeT(elt.LastModified);
					auto file_size = (idx_t)elt.FileSize;
					out_path_metadata_cache->insert({elt.Name, AzurePathMetadata{last_modified, file_size}});
				}
			}
		}

		if (res.NextPageToken) {
			options.ContinuationToken = res.NextPageToken;
		} else {
			break;
		}
	}
}

//////// AzureDfsContextState ////////
AzureDfsContextState::AzureDfsContextState(Azure::Storage::Files::DataLake::DataLakeServiceClient client,
                                           const AzureReadOptions &azure_read_options)
    : AzureContextState(azure_read_options), service_client(std::move(client)) {
}

Azure::Storage::Files::DataLake::DataLakeFileSystemClient
AzureDfsContextState::GetDfsFileSystemClient(const std::string &file_system_name) const {
	return service_client.GetFileSystemClient(file_system_name);
}

//////// AzureDfsContextState ////////
AzureDfsStorageFileHandle::AzureDfsStorageFileHandle(AzureDfsStorageFileSystem &fs, string path, FileOpenFlags flags,
                                                     const AzureReadOptions &read_options,
                                                     Azure::Storage::Files::DataLake::DataLakeFileClient client)
    : AzureFileHandle(fs, std::move(path), flags, read_options), file_client(std::move(client)) {
}

//////// AzureDfsStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureDfsStorageFileSystem::CreateHandle(const string &path, FileOpenFlags flags,
                                                                    optional_ptr<FileOpener> opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot do Azure storage CreateHandle without FileOpener");
	}

	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	auto parsed_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, parsed_url);
	auto file_system_client = storage_context->As<AzureDfsContextState>().GetDfsFileSystemClient(parsed_url.container);

	auto handle = make_uniq<AzureDfsStorageFileHandle>(*this, path, flags, storage_context->read_options,
	                                                   file_system_client.GetFileClient(parsed_url.path));
	if (!handle->PostConstruct()) {
		return nullptr;
	}
	return std::move(handle);
}

bool AzureDfsStorageFileSystem::CanHandleFile(const string &fpath) {
	return IsDfsScheme(fpath);
}

vector<string> AzureDfsStorageFileSystem::Glob(const string &path, FileOpener *opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot do Azure storage Glob without FileOpener");
	}

	auto azure_url = ParseUrl(path);

	// If path does not contains any wildcard, we assume that an absolute path therefor nothing to do
	auto first_wildcard_pos = azure_url.path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		return {path};
	}

	// The path contains wildcard try to list file with the minimum calls
	auto dfs_storage_service = ConnectToDfsStorageAccount(opener, path, azure_url);
	auto dfs_filesystem_client = dfs_storage_service.GetFileSystemClient(azure_url.container);

	auto index_root_dir = azure_url.path.rfind('/', first_wildcard_pos);
	if (index_root_dir == string::npos) {
		index_root_dir = 0;
	}
	auto shared_path = azure_url.path.substr(0, index_root_dir);

	std::vector<std::string> result;
	Walk(dfs_filesystem_client, shared_path,
	     // pattern to match
	     azure_url.path, std::min(azure_url.path.length(), azure_url.path.find('/', index_root_dir + 1)),
	     // output result
	     &result,
	     &path_metadata_cache);

	if (!result.empty()) {
		const auto path_result_prefix =
		    (azure_url.is_fully_qualified ? (azure_url.prefix + azure_url.storage_account_name + '.' +
		                                     azure_url.endpoint + '/' + azure_url.container)
		                                  : (azure_url.prefix + azure_url.container)) +
		    '/';
		for (auto &elt : result) {
			elt = path_result_prefix + elt;
		}
	}

	return result;
}

void AzureDfsStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &hfh = handle.Cast<AzureDfsStorageFileHandle>();

	const std::string &path = handle.path;
	auto azure_url = ParseUrl(path);

	auto entry = path_metadata_cache.find(azure_url.path);
	if (entry != path_metadata_cache.end()) {
		hfh.length = entry->second.file_size;
		hfh.last_modified = entry->second.last_modified;
	}
	else {
		auto res = hfh.file_client.GetProperties();
		hfh.length = res.Value.FileSize;
		hfh.last_modified = ToTimeT(res.Value.LastModified);
	}
}

void AzureDfsStorageFileSystem::ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out,
                                          idx_t buffer_out_len) {
	auto &afh = handle.Cast<AzureDfsStorageFileHandle>();
	try {
		// Specify the range
		Azure::Core::Http::HttpRange range;
		range.Offset = (int64_t)file_offset;
		range.Length = buffer_out_len;
		Azure::Storage::Files::DataLake::DownloadFileToOptions options;
		options.Range = range;
		options.TransferOptions.Concurrency = afh.read_options.transfer_concurrency;
		options.TransferOptions.InitialChunkSize = afh.read_options.transfer_chunk_size;
		options.TransferOptions.ChunkSize = afh.read_options.transfer_chunk_size;
		auto res = afh.file_client.DownloadTo((uint8_t *)buffer_out, buffer_out_len, options);

	} catch (const Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to '%s' failed with %s Reason Phrase: %s", afh.path,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

shared_ptr<AzureContextState> AzureDfsStorageFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                              const string &path,
                                                                              const AzureParsedUrl &parsed_url) {
	auto azure_read_options = ParseAzureReadOptions(opener);

	return make_shared_ptr<AzureDfsContextState>(ConnectToDfsStorageAccount(opener, path, parsed_url),
	                                             azure_read_options);
}

} // namespace duckdb
