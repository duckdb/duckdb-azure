#include "azure_blob_filesystem.hpp"

#include "azure_http_state.hpp"
#include "azure_storage_account_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/main/client_data.hpp"
#include "include/azure_blob_filesystem.hpp"

#include <azure/core/base64.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_options.hpp>

#include <cstdlib>

namespace duckdb {

// Encode uint32 as big-endian 4 bytes → base64 (always 8 chars, same length for all IDs)
static std::string MakeBlockId(uint32_t i) {
	uint8_t bytes[4] = {uint8_t(i >> 24), uint8_t(i >> 16), uint8_t(i >> 8), uint8_t(i)};
	return Azure::Core::Convert::Base64Encode({bytes, bytes + 4});
}

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
                                                       optional_ptr<AzureMetadataCache> metadata_cache,
                                                       Azure::Storage::Blobs::BlockBlobClient blob_client)
    : AzureFileHandle(fs, info, flags, FileType::FILE_TYPE_INVALID, read_options, metadata_cache),
      blob_client(std::move(blob_client)) {
}

void AzureBlobStorageFileHandle::Sync() {
	D_ASSERT((flags.OpenForWriting() || flags.OpenForAppending()) || pending_block_ids.empty());
	if (pending_block_ids.empty()) {
		return;
	}
	try {
		std::vector<std::string> all_ids;
		all_ids.reserve(committed_block_count + pending_block_ids.size());
		for (uint32_t i = 0; i < committed_block_count; i++) {
			all_ids.push_back(MakeBlockId(i));
		}
		all_ids.insert(all_ids.end(), pending_block_ids.begin(), pending_block_ids.end());
		auto res = blob_client.CommitBlockList(all_ids);
		last_modified = AzureBlobStorageFileSystem::ToTimestamp(res.Value.LastModified);
		committed_block_count += pending_block_ids.size();
		pending_block_ids.clear();
	} catch (const Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem FileSync of '%s' failed with %s Reason Phrase: %s", GetPath(),
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

void AzureBlobStorageFileHandle::Close() {
	Sync();
	DUCKDB_LOG_FILE_SYSTEM_CLOSE((*this));
}

//////// AzureBlobStorageFileSystem ////////
unique_ptr<AzureFileHandle> AzureBlobStorageFileSystem::CreateHandle(const OpenFileInfo &info, FileOpenFlags flags,
                                                                     optional_ptr<FileOpener> opener) {
	if (!opener) {
		throw InternalException("Unsupported(INTERNAL): cannot create an Azure blob Handle without FileOpener");
	}
	if (flags.Compression() != FileCompressionType::UNCOMPRESSED) {
		throw InternalException("Unsupported(INTERNAL): cannot open an Azure blob in compressed mode");
	}
	if (flags.OpenForReading() && (flags.OpenForWriting() || flags.OpenForAppending())) {
		throw NotImplementedException("Unsupported: cannot open an Azure blob in read+write mode");
	}
	if (flags.OpenForAppending()) {
		throw NotImplementedException("Unsupported: cannot open an Azure blob in append mode");
	}

	auto parsed_url = ParseUrl(info.path);
	auto storage_context = GetOrCreateStorageContext(opener, info.path, parsed_url);
	if (flags.OpenForWriting() || flags.OpenForAppending()) {
		InvalidateMetadata(opener, info.path);
	}
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(parsed_url.container);
	auto blob_client = container.GetBlockBlobClient(parsed_url.path);

	auto handle = make_uniq<AzureBlobStorageFileHandle>(*this, info, flags, storage_context->read_options,
	                                                    GetMetadataCache(opener), std::move(blob_client));
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
		vector<OpenFileInfo> rv;
		auto handle = OpenFile(path, FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);
		// Returning directories here suppresses DuckDB's fallback auto-glob path for multi-file readers.
		if (handle && handle->Cast<AzureBlobStorageFileHandle>().GetType() == FileType::FILE_TYPE_REGULAR) {
			rv.emplace_back(path);
		}
		return rv;
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
			throw IOException("AzureBlobStorageFileSystem Read to %s failed with %s Reason Phrase: %s", path,
			                  e.ErrorCode, e.ReasonPhrase);
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
				options.emplace("type", Value("file"));
				options.emplace("file_size", Value::BIGINT(key.BlobSize));
				options.emplace("last_modified", Value::TIMESTAMP(ToTimestamp(key.Details.LastModified)));
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

// Caller beware -- this is always recursive, performance may be terrible.
bool AzureBlobStorageFileSystem::ListFilesExtended(const string &path_in,
                                                   const std::function<void(OpenFileInfo &info)> &callback,
                                                   optional_ptr<FileOpener> opener) {
	if (path_in.find('*') != string::npos) {
		throw InvalidInputException("ListFiles does not support globs");
	}
	// Normalize: to end with '/' so it's a clear dir prefix
	auto path = (!path_in.empty() && path_in.back() == '/') ? path_in : (path_in + '/');
	auto url = ParseUrl(path);
	auto storage_context = GetOrCreateStorageContext(opener, path, url);
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(url.container);

	// NOTE: this code fakes dir listings by keeping a small cache of seen directories. Since blob store has no such
	// concept, it's up to us to track and release them in the result. This plays a role in overwrite detection, so
	// we do the work here.
	set<string> seen_dirs;

	bool rv = false;
	const Azure::Storage::Blobs::ListBlobsOptions options = {/* .Prefix = */ url.path};
	// NOTE: there's a LOT of metadata available, and tags, etc. -- see
	// https://github.com/Azure/azure-sdk-for-cpp/blob/main/sdk/storage/azure-storage-blobs/inc/azure/storage/blobs/rest_client.hpp#L1134
	// (struct BlobItemDetails) and
	// https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs
	for (auto page = container.ListBlobs(options); page.HasPage(); page.MoveToNextPage()) {
		for (auto &blob : page.Blobs) {
			// blob list returns _full paths_, so chop off the prefix first
			auto child = blob.Name.substr(url.path.size());
			// file case
			auto slash_pos = child.find('/');
			if (slash_pos == string::npos) {
				rv = true;
				OpenFileInfo info(child);
				info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
				auto &options = info.extended_info->options;
				options.emplace("type", Value("file"));
				options.emplace("file_size", Value::BIGINT(blob.BlobSize));
				options.emplace("last_modified", Value::TIMESTAMP(ToTimestamp(blob.Details.LastModified)));
				callback(info);
			} else {
				// chop off slash + tail, take the remainder and treat as directory, caching it in seen to avoid repeat.
				auto dir = child.substr(0, slash_pos);
				if (seen_dirs.find(dir) == seen_dirs.end()) {
					rv = true;
					seen_dirs.insert(dir);
					OpenFileInfo info(dir);
					info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
					auto &options = info.extended_info->options;
					options.emplace("type", Value("directory"));
					options.emplace("last_modified", Value::TIMESTAMP(ToTimestamp(blob.Details.LastModified)));
					callback(info);
				}
			}
		}
	}
	return rv;
}

void AzureBlobStorageFileSystem::LoadRemoteFileInfo(AzureFileHandle &handle) {
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();

	if (afh.IsRemoteLoaded()) {
		return;
	}

	// handling a couple situations here:
	// - does exist, but want exclusive create
	// - does exist, just get the data
	// - does exist, truncate (same API as create)
	// - doesn't exist, must be created (file; dir create doesn't happen here)
	// - doesn't exist, don't create

	auto set_props = [&](bool is_dir, idx_t length, timestamp_t last_mod) {
		afh.SetFileInfo(is_dir ? FileType::FILE_TYPE_DIR : FileType::FILE_TYPE_REGULAR, length, last_mod);
	};

	auto create_file = [&]() {
		auto res_create = afh.blob_client.CommitBlockList({});
		set_props(false, 0, ToTimestamp(res_create.Value.LastModified));
	};
	auto truncate_file = create_file;

	try {
		auto res_props = afh.blob_client.GetProperties();
		if (afh.flags.ExclusiveCreate()) {
			throw IOException("AzureBlobStorageFileSystem will not open file: '%s', ExclusiveCreate specified "
			                  "while file already exists.");
		} else if (afh.flags.OpenForWriting() && afh.flags.OverwriteExistingFile()) {
			return truncate_file();
		}
		// NOTE: honor convention for S3/Azure "foo/" empty file -> "foo" dir marker
		auto is_dir = StringUtil::EndsWith(afh.GetPath(), "/");
		if (!is_dir) {
			// NOTE: IFF blob proto connection to ADLSv2, check header `X-Ms-Meta-Hdi_isfolder` in raw resp
			// for is_dir;
			// The function MetadataIncidatesIsDirectory does this, albeit as `_detail` non-public api.
			// see https://forum.rclone.org/t/does-rclone-support-azure-data-lake-gen2/23940/5
			// and
			auto ite = res_props.Value.Metadata.find("hdi_isFolder"); // NOTE: Metadata is case-insensitive
			is_dir |= (ite != res_props.Value.Metadata.end() && ite->second == "true");
		}
		return set_props(is_dir, res_props.Value.BlobSize, ToTimestamp(res_props.Value.LastModified));
	} catch (const Azure::Storage::StorageException &e) {
		if (int(e.StatusCode) == 404 && afh.flags.OpenForWriting() &&
		    (afh.flags.OverwriteExistingFile() || afh.flags.CreateFileIfNotExists())) {
			return create_file();
		}
		throw;
	}
}

bool AzureBlobStorageFileSystem::DirectoryExists(const string &dirname, optional_ptr<FileOpener> opener) {
	// NOTE: existance of directory makes no sense in Blob -- since the concept isn't present.
	// That said we fake it -- if glob(dir/*) returns anything at all, say it exists, otherwise not.
	// If we want to, could add an option to behave in this, way, always return true or even always false.
	// This mechanism of relying on dir/ is also frequently used to fake dirs in e.g. S3, so it's consistent
	// with other common practice.
	if (opener == nullptr) {
		throw InternalException("Cannot do Azure storage directory test without FileOpener");
	}
	// auto handle = OpenFile(dirname, FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);
	auto dir_slashed = dirname[dirname.length() - 1] == '/' ? dirname : (dirname + '/');
	auto dir_url = ParseUrl(dir_slashed);
	auto storage_context = GetOrCreateStorageContext(opener, dirname, dir_url);
	auto container_client = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(dir_url.container);

	Azure::Storage::Blobs::ListBlobsOptions options;
	options.Prefix = dir_url.path;
	Azure::Storage::Blobs::ListBlobsPagedResponse res;
	try {
		res = container_client.ListBlobs(options);
	} catch (Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Read to %s failed with %s Reason Phrase: %s", dir_slashed,
		                  e.ErrorCode, e.ReasonPhrase);
	}
	return !res.Blobs.empty();
}

bool AzureBlobStorageFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);
	if (handle != nullptr) {
		auto &sfh = handle->Cast<AzureBlobStorageFileHandle>();
		return sfh.length >= 0; // aka return true; -- avoid optimizers and shenanigans -- deref handle to be sure
	}
	return false;
}

void AzureBlobStorageFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto url = ParseUrl(filename);
	auto storage_context = GetOrCreateStorageContext(opener, filename, url);
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(url.container);
	auto blob_client = container.GetBlockBlobClient(url.path);
	try {
		blob_client.Delete();
		InvalidateMetadata(opener, filename);
	} catch (Azure::Storage::StorageException &e) {
		throw IOException("AzureBlobStorageFileSystem Delete of %s failed with %s Reason Phrase: %s", filename,
		                  e.ErrorCode, e.ReasonPhrase);
	}
}

bool AzureBlobStorageFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto url = ParseUrl(filename);
	auto storage_context = GetOrCreateStorageContext(opener, filename, url);
	auto container = storage_context->As<AzureBlobContextState>().GetBlobContainerClient(url.container);
	auto blob_client = container.GetBlockBlobClient(url.path);
	auto removed = blob_client.DeleteIfExists().Value.Deleted;
	if (removed) {
		InvalidateMetadata(opener, filename);
	}
	return removed;
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

int64_t AzureBlobStorageFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();
	Write(handle, buffer, nr_bytes, afh.file_offset);
	// LOG in Write()
	return nr_bytes;
}

void AzureBlobStorageFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	D_ASSERT(nr_bytes >= 0);
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();

	if (!(afh.flags.OpenForWriting() || afh.flags.OpenForAppending())) {
		throw InternalException("Write called on file opened in read mode");
	}

	D_ASSERT(location == afh.file_offset || location == 0);

	auto block_id = MakeBlockId(afh.committed_block_count + static_cast<uint32_t>(afh.pending_block_ids.size()));
	auto body_stream = Azure::Core::IO::MemoryBodyStream(static_cast<uint8_t *>(buffer), nr_bytes);
	afh.blob_client.StageBlock(block_id, body_stream); // throws on error
	afh.pending_block_ids.push_back(block_id);
	afh.file_offset += nr_bytes;
	afh.length += nr_bytes;
	DUCKDB_LOG_FILE_SYSTEM_WRITE(handle, nr_bytes, afh.file_offset);
}

void AzureBlobStorageFileSystem::FileSync(FileHandle &handle) {
	auto &afh = handle.Cast<AzureBlobStorageFileHandle>();
	afh.Sync();
}

} // namespace duckdb
