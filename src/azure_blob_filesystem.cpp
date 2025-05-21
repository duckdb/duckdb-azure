#include "azure_blob_filesystem.hpp"

#include "azure_storage_account_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "azure_http_state.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include <azure/storage/blobs.hpp>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>

namespace duckdb {

const string AzureBlobStorageFileSystem::SCHEME = "azure";
const string AzureBlobStorageFileSystem::SHORT_SCHEME = "az";

const string AzureBlobStorageFileSystem::PATH_PREFIX = "azure://";
const string AzureBlobStorageFileSystem::SHORT_PATH_PREFIX = "az://";

// taken from s3fs.cpp TODO: deduplicate!
static bool Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
                  vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end) {

	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		if (!Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}

//////// AzureBlobContextState ////////
AzureBlobContextState::AzureBlobContextState(Azure::Storage::Blobs::BlobServiceClient client,
                                             const AzureReadOptions &azure_read_options)
    : AzureContextState(azure_read_options), service_client(std::move(client)) {
}

Azure::Storage::Blobs::BlobContainerClient
AzureBlobContextState::GetBlobContainerClient(const std::string &blobContainerName) const {
	return service_client.GetBlobContainerClient(blobContainerName);
}

//////// AzureBlobStorageFileHandle ////////
AzureBlobStorageFileHandle::AzureBlobStorageFileHandle(AzureBlobStorageFileSystem &fs, const OpenFileInfo &info,
                                                       FileOpenFlags flags, const AzureReadOptions &read_options,
                                                       Azure::Storage::Blobs::BlobClient blob_client)
    : AzureFileHandle(fs, info, flags, read_options), blob_client(std::move(blob_client)) {
}

//////// AzureBlobStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureBlobStorageFileSystem::CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
                                                                     optional_ptr<FileOpener> opener) {
	if (!opener) {
		throw InternalException("Cannot do Azure storage CreateHandle without FileOpener");
	}

	D_ASSERT(flags.Compression() == FileCompressionType::UNCOMPRESSED);

	auto parsed_url = ParseUrl(info.path);
	auto storage_context = GetOrCreateStorageContext(opener, info.path, parsed_url);
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(parsed_url.container);
	auto blob_client = container.GetBlockBlobClient(parsed_url.path);

	auto handle = make_uniq<AzureBlobStorageFileHandle>(*this, info, flags, storage_context->read_options,
	                                                    std::move(blob_client));
	if (!handle->PostConstruct()) {
		return nullptr;
	}
	return std::move(handle);
}

bool AzureBlobStorageFileSystem::CanHandleFile(const string &fpath) {
	return fpath.rfind(PATH_PREFIX, 0) * fpath.rfind(SHORT_PATH_PREFIX, 0) == 0;
}

vector<OpenFileInfo> AzureBlobStorageFileSystem::Glob(const string &path, FileOpener *opener) {
	if (opener == nullptr) {
		throw InternalException("Cannot do Azure storage Glob without FileOpener");
	}

	auto azure_url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, azure_url);

	// Azure matches on prefix, not glob pattern, so we take a substring until the first wildcard
	auto first_wildcard_pos = azure_url.path.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos) {
		return {path};
	}

	string shared_path = azure_url.path.substr(0, first_wildcard_pos);
	auto container_client = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(azure_url.container);

	const auto pattern_splits = StringUtil::Split(azure_url.path, "/");
	vector<OpenFileInfo> result;

	Azure::Storage::Blobs::ListBlobsOptions options;
	options.Prefix = shared_path;

	const auto path_result_prefix =
	    (azure_url.is_fully_qualified ? (azure_url.prefix + azure_url.storage_account_name + '.' + azure_url.endpoint +
	                                     '/' + azure_url.container)
	                                  : (azure_url.prefix + azure_url.container));
	while (true) {
		// Perform query
		Azure::Storage::Blobs::ListBlobsPagedResponse res;
		try {
			res = container_client.ListBlobs(options);
		} catch (Azure::Storage::StorageException &e) {
			throw IOException("AzureStorageFileSystem Read to %s failed with %s Reason Phrase: %s", path, e.ErrorCode,
			                  e.ReasonPhrase);
		}

		// Assuming that in the majority of the case it's wildcard
		result.reserve(result.size() + res.Blobs.size());

		// Ensure that the retrieved element match the expected pattern
		for (const auto &key : res.Blobs) {
			vector<string> key_splits = StringUtil::Split(key.Name, "/");
			bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

			if (is_match) {
				auto result_full_url = path_result_prefix + '/' + key.Name;
				OpenFileInfo info(result_full_url);
				info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
				auto &options = info.extended_info->options;
				options.emplace("file_size", Value::BIGINT(key.BlobSize));
				options.emplace("last_modified", Value::TIMESTAMP(Timestamp::FromTimeT(
				                                     AzureStorageFileSystem::ToTimeT(key.Details.LastModified))));
				result.push_back(info);
			}
		}

		// Manage Azure pagination
		if (res.NextPageToken) {
			options.ContinuationToken = res.NextPageToken;
		} else {
			break;
		}
	}

	return result;
}

void AzureBlobStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &hfh = handle.Cast<AzureBlobStorageFileHandle>();

	if (hfh.length == 0 && hfh.last_modified == 0) {
		auto res = hfh.blob_client.GetProperties();
		hfh.length = res.Value.BlobSize;
		hfh.last_modified = ToTimeT(res.Value.LastModified);
	}
}

bool AzureBlobStorageFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	try {
		auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_READ, opener);
		auto &sfh = handle->Cast<AzureBlobStorageFileHandle>();
		if (sfh.length == 0) {
			return false;
		}
		return true;
	} catch (...) {
		return false;
	};
}

void AzureBlobStorageFileSystem::ReadRange(AzureFileHandle &handle, idx_t file_offset, char *buffer_out,
                                           idx_t buffer_out_len) {
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();

	try {
		// Specify the range
		Azure::Core::Http::HttpRange range;
		range.Offset = (int64_t)file_offset;
		range.Length = buffer_out_len;
		Azure::Storage::Blobs::DownloadBlobToOptions options;
		options.Range = range;
		options.TransferOptions.Concurrency = afh.read_options.transfer_concurrency;
		options.TransferOptions.InitialChunkSize = afh.read_options.transfer_chunk_size;
		options.TransferOptions.ChunkSize = afh.read_options.transfer_chunk_size;
		auto res = afh.blob_client.DownloadTo((uint8_t *)buffer_out, buffer_out_len, options);

	} catch (const Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to '%s' failed with %s Reason Phrase: %s", afh.path,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

shared_ptr<AzureContextState> AzureBlobStorageFileSystem::CreateStorageContext(optional_ptr<FileOpener> opener,
                                                                               const string &path,
                                                                               const AzureParsedUrl &parsed_url) {
	auto azure_read_options = ParseAzureReadOptions(opener);

	return make_shared_ptr<AzureBlobContextState>(ConnectToBlobStorageAccount(opener, path, parsed_url),
	                                              azure_read_options);
}

} // namespace duckdb
