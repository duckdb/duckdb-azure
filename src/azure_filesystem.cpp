#include "azure_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <azure/storage/common/storage_exception.hpp>

#include "azure_storage_account_client.hpp"

namespace duckdb {

AzureContextState::AzureContextState(const AzureReadOptions &read_options)
    : read_options(read_options), is_valid(true) {
}

bool AzureContextState::IsValid() const {
	return is_valid;
}

void AzureContextState::QueryEnd() {
	is_valid = false;
}

static string GetFileType(const Value &value) {
	auto type = value.ToString();
	if (!type.empty() && type.front() == '\'' && type.back() == '\'' && type.size() >= 2) {
		type = type.substr(1, type.size() - 2);
	}
	return type;
}

AzureFileHandle::AzureFileHandle(AzureStorageFileSystem &fs, const OpenFileInfo &info, FileOpenFlags flags,
                                 FileType file_type, const AzureReadOptions &read_options,
                                 optional_ptr<AzureMetadataCache> metadata_cache_p)
    : FileHandle(fs, info.path, flags), flags(flags),
      // File info
      is_remote_loaded(false), file_type(file_type), length(0), last_modified(0),
      // Read info
      buffer_available(0), buffer_idx(0), file_offset(0), buffer_start(0), buffer_end(0),
      // Options
      read_options(read_options), metadata_cache(metadata_cache_p) {
	if (!flags.RequireParallelAccess() && !flags.DirectIO()) {
		read_buffer = duckdb::unique_ptr<data_t[]>(new data_t[read_options.buffer_size]);
	}

	// Set metadata of file when available, it avoids to invoke to the storage to get them.
	bool has_file_type = file_type != FileType::FILE_TYPE_INVALID;
	bool has_length = false;
	bool has_last_modified = false;
	if (info.extended_info) {
		auto type_entry = info.extended_info->options.find("type");
		if (type_entry != info.extended_info->options.end()) {
			auto type = GetFileType(type_entry->second);
			if (type == "directory") {
				file_type = FileType::FILE_TYPE_DIR;
				has_file_type = true;
			} else if (type == "file") {
				file_type = FileType::FILE_TYPE_REGULAR;
				has_file_type = true;
			}
		}
		auto entry1 = info.extended_info->options.find("file_size");
		if (entry1 != info.extended_info->options.end()) {
			length = entry1->second.GetValue<uint64_t>();
			has_length = true;
		}
		auto entry2 = info.extended_info->options.find("last_modified");
		if (entry2 != info.extended_info->options.end()) {
			last_modified = entry2->second.GetValue<timestamp_t>();
			has_last_modified = true;
		}
	}
	if (has_file_type && has_last_modified && (file_type == FileType::FILE_TYPE_DIR || has_length)) {
		SetFileInfo(file_type, length, last_modified);
	}
}

void AzureFileHandle::SetFileInfo(FileType file_type_p, idx_t length_p, timestamp_t last_modified_p) {
	is_remote_loaded = true;
	file_type = file_type_p;
	length = file_type_p == FileType::FILE_TYPE_DIR ? 0 : length_p;
	last_modified = last_modified_p;
	file_offset = 0;
}

bool AzureFileHandle::PostConstruct() {
	return static_cast<AzureStorageFileSystem &>(file_system).LoadFileInfo(*this);
}

static bool CanUseMetadataCache(const AzureFileHandle &handle) {
	return handle.metadata_cache && !handle.flags.OpenForWriting() && !handle.flags.OpenForAppending() &&
	       !handle.flags.ExclusiveCreate();
}

static AzureFileInfo GetCacheEntry(const AzureFileHandle &handle) {
	AzureFileInfo info;
	info.file_type = handle.file_type;
	info.length = handle.length;
	info.last_modified = handle.last_modified;
	return info;
}

bool AzureStorageFileSystem::ParseAzureMetadataCacheEnabled(optional_ptr<FileOpener> opener) {
	Value metadata_cache_val;
	if (FileOpener::TryGetCurrentSetting(opener, "enable_http_metadata_cache", metadata_cache_val) &&
	    !metadata_cache_val.IsNull()) {
		return metadata_cache_val.GetValue<bool>();
	}
	return false;
}

optional_ptr<AzureMetadataCache> AzureStorageFileSystem::GetGlobalMetadataCache() {
	lock_guard<mutex> lock(global_cache_lock);
	if (!global_metadata_cache) {
		global_metadata_cache = make_uniq<AzureMetadataCache>(false);
	}
	return global_metadata_cache.get();
}

optional_ptr<AzureMetadataCache> AzureStorageFileSystem::GetMetadataCache(optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	auto client_context = FileOpener::TryGetClientContext(opener);
	if (!db) {
		return nullptr;
	}
	if (ParseAzureMetadataCacheEnabled(opener)) {
		return GetGlobalMetadataCache();
	}
	if (client_context) {
		return client_context->registered_state->GetOrCreate<AzureMetadataCache>("azure_metadata_cache", true).get();
	}
	return nullptr;
}

void AzureStorageFileSystem::InvalidateMetadata(optional_ptr<FileOpener> opener, const string &path) {
	auto metadata_cache = GetMetadataCache(opener);
	if (metadata_cache) {
		metadata_cache->Erase(path);
	}
}

bool AzureStorageFileSystem::LoadFileInfo(AzureFileHandle &handle) {
	try {
		if (handle.IsRemoteLoaded()) {
			if (CanUseMetadataCache(handle)) {
				handle.metadata_cache->Insert(handle.path, GetCacheEntry(handle));
			}
			if (handle.flags.ReturnNullIfExists()) {
				return false;
			}
			return true;
		}
		if (CanUseMetadataCache(handle)) {
			AzureFileInfo cached_info;
			if (handle.metadata_cache->Find(handle.path, cached_info)) {
				handle.SetFileInfo(cached_info.file_type, cached_info.length, cached_info.last_modified);
				if (handle.flags.ReturnNullIfExists()) {
					return false;
				}
				return true;
			}
		}

		LoadRemoteFileInfo(handle);
		if (CanUseMetadataCache(handle)) {
			handle.metadata_cache->Insert(handle.path, GetCacheEntry(handle));
		}
		if (handle.flags.ReturnNullIfExists()) {
			return false;
		}
	} catch (const Azure::Storage::StorageException &e) {
		if (int(e.StatusCode) == 404 && handle.flags.ReturnNullIfNotExists()) {
			return false;
		}
		throw IOException(
		    "AzureStorageFileSystem open file '%s' failed with code '%s', Reason Phrase: '%s', Message: '%s'",
		    handle.path, e.ErrorCode, e.ReasonPhrase, e.Message);
	} catch (const IOException &e) {
		throw;
	} catch (const std::exception &e) {
		throw IOException("AzureStorageFileSystem could not open file: '%s', unknown error occurred, this could mean "
		                  "the credentials used were wrong. Original error message: '%s' ",
		                  handle.path, e.what());
	}
	return true;
}

unique_ptr<FileHandle> AzureStorageFileSystem::OpenFileExtended(const OpenFileInfo &info, FileOpenFlags flags,
                                                                optional_ptr<FileOpener> opener) {
	auto handle = CreateHandle(info, flags, opener);
	if (handle && opener) {
		handle->TryAddLogger(*opener);
		DUCKDB_LOG_FILE_SYSTEM_OPEN((*handle));
	}
	return std::move(handle);
}

unique_ptr<FileHandle> AzureStorageFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	return OpenFileExtended(OpenFileInfo(path), flags, opener);
}

int64_t AzureStorageFileSystem::GetFileSize(FileHandle &handle) {
	auto &afh = handle.Cast<AzureFileHandle>();
	return afh.length;
}

timestamp_t AzureStorageFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &afh = handle.Cast<AzureFileHandle>();
	return afh.last_modified;
}

void AzureStorageFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &sfh = handle.Cast<AzureFileHandle>();
	sfh.file_offset = location;
}

idx_t AzureStorageFileSystem::SeekPosition(FileHandle &handle) {
	auto &afh = handle.Cast<AzureFileHandle>();
	return afh.file_offset;
}

// TODO: this code is identical to HTTPFS, look into unifying it
void AzureStorageFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &hfh = handle.Cast<AzureFileHandle>();

	idx_t to_read = nr_bytes;
	idx_t buffer_offset = 0;

	// Don't buffer when DirectIO is set.
	if (hfh.flags.DirectIO() || hfh.flags.RequireParallelAccess()) {
		if (to_read == 0) {
			DUCKDB_LOG_FILE_SYSTEM_READ(handle, nr_bytes, location);
			return;
		}
		ReadRange(hfh, location, (char *)buffer, to_read);
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location + nr_bytes;
		DUCKDB_LOG_FILE_SYSTEM_READ(handle, nr_bytes, location);
		return;
	}

	if (location >= hfh.buffer_start && location < hfh.buffer_end) {
		hfh.file_offset = location;
		hfh.buffer_idx = location - hfh.buffer_start;
		hfh.buffer_available = (hfh.buffer_end - hfh.buffer_start) - hfh.buffer_idx;
	} else {
		// reset buffer
		hfh.buffer_available = 0;
		hfh.buffer_idx = 0;
		hfh.file_offset = location;
	}
	while (to_read > 0) {
		auto buffer_read_len = MinValue<idx_t>(hfh.buffer_available, to_read);
		if (buffer_read_len > 0) {
			D_ASSERT(hfh.buffer_start + hfh.buffer_idx + buffer_read_len <= hfh.buffer_end);
			memcpy((char *)buffer + buffer_offset, hfh.read_buffer.get() + hfh.buffer_idx, buffer_read_len);

			buffer_offset += buffer_read_len;
			to_read -= buffer_read_len;

			hfh.buffer_idx += buffer_read_len;
			hfh.buffer_available -= buffer_read_len;
			hfh.file_offset += buffer_read_len;
		}

		if (to_read > 0 && hfh.buffer_available == 0) {
			auto new_buffer_available = MinValue<idx_t>(hfh.read_options.buffer_size, hfh.length - hfh.file_offset);

			// Bypass buffer if we read more than buffer size
			if (to_read > new_buffer_available) {
				ReadRange(hfh, location + buffer_offset, (char *)buffer + buffer_offset, to_read);
				hfh.buffer_available = 0;
				hfh.buffer_idx = 0;
				hfh.file_offset += to_read;
				break;
			} else {
				ReadRange(hfh, hfh.file_offset, (char *)hfh.read_buffer.get(), new_buffer_available);
				hfh.buffer_available = new_buffer_available;
				hfh.buffer_idx = 0;
				hfh.buffer_start = hfh.file_offset;
				hfh.buffer_end = hfh.buffer_start + new_buffer_available;
			}
		}
	}
	DUCKDB_LOG_FILE_SYSTEM_READ(handle, nr_bytes, location);
}

int64_t AzureStorageFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &hfh = handle.Cast<AzureFileHandle>();
	idx_t max_read = hfh.length - hfh.file_offset;
	nr_bytes = MinValue<idx_t>(max_read, nr_bytes);
	Read(handle, buffer, nr_bytes, hfh.file_offset);
	// LOG handled in Read()
	return nr_bytes;
}

static string GetContextKeyPath(optional_ptr<FileOpener> opener, const string &path, const AzureParsedUrl &parsed) {
	// context key == proto://{storage_account}{.}{endpoint}
	// when storage account / endpoint unavailable, try to fetch via secret manager
	string account;
	string endpoint;

	if (parsed.is_fully_qualified) {
		account = parsed.storage_account_name;
		endpoint = parsed.endpoint;
	} else {
		// no storage account? map it from the secret if possible
		auto secret_match = LookupSecret(opener, path);
		if (secret_match.HasMatch()) {
			const auto &secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());

			const auto account_from_secret = secret.TryGetValue("account_name", false);
			if (!account_from_secret.IsNull()) {
				account = account_from_secret.ToString();
			}
			const auto endpoint_from_secret = secret.TryGetValue("endpoint", false);
			if (!endpoint_from_secret.IsNull()) {
				endpoint = endpoint_from_secret.ToString();
			}
		}
	}

	// build key from account and/or endpoint, ow return ""
	const auto has_account = !account.empty();
	const auto has_endpoint = !endpoint.empty();
	if (!has_account && !has_endpoint) {
		return "";
	}
	if (has_account && has_endpoint) {
		return account + "." + endpoint;
	}
	return has_account ? account : endpoint;
}

shared_ptr<AzureContextState> AzureStorageFileSystem::GetOrCreateStorageContext(optional_ptr<FileOpener> opener,
                                                                                const string &path,
                                                                                const AzureParsedUrl &parsed_url) {
	Value value;
	bool azure_context_caching = true;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_context_caching", value)) {
		azure_context_caching = value.GetValue<bool>();
	}
	auto client_context = FileOpener::TryGetClientContext(opener);

	shared_ptr<AzureContextState> result;
	if (azure_context_caching && client_context) {
		string key_path = GetContextKeyPath(opener, path, parsed_url);
		auto &registered_state = client_context->registered_state;

		// Ok, now use account in key, or otherwise skip the cache
		if (!key_path.empty()) {
			auto context_key = GetContextPrefix() + key_path;
			result = registered_state->Get<AzureContextState>(context_key);
			if (!result || !result->IsValid()) {
				result = CreateStorageContext(opener, path, parsed_url);
				registered_state->Insert(context_key, result);
				return result;
			}
		}
	}

	return CreateStorageContext(opener, path, parsed_url);
}

AzureReadOptions AzureStorageFileSystem::ParseAzureReadOptions(optional_ptr<FileOpener> opener) {
	AzureReadOptions options;

	Value concurrency_val;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_read_transfer_concurrency", concurrency_val)) {
		options.transfer_concurrency = concurrency_val.GetValue<int32_t>();
	}

	Value chunk_size_val;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_read_transfer_chunk_size", chunk_size_val)) {
		options.transfer_chunk_size = chunk_size_val.GetValue<int64_t>();
	}

	Value buffer_size_val;
	if (FileOpener::TryGetCurrentSetting(opener, "azure_read_buffer_size", buffer_size_val)) {
		options.buffer_size = buffer_size_val.GetValue<idx_t>();
	}

	return options;
}

timestamp_t AzureStorageFileSystem::ToTimestamp(const Azure::DateTime &dt) {
	auto time_point = static_cast<std::chrono::system_clock::time_point>(dt);
	auto micros = std::chrono::duration_cast<std::chrono::microseconds>(time_point.time_since_epoch()).count();
	return timestamp_t(micros);
}

} // namespace duckdb
