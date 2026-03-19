#include "http_logging_policy.hpp"
#include <azure/core/http/http.hpp>
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/http_status_code.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

namespace duckdb {

HttpLoggingPolicy::HttpLoggingPolicy(shared_ptr<Logger> logger_p, std::unordered_set<std::string> redact_query_params_p,
                                     std::unordered_set<std::string> redact_headers_p)
    : logger(std::move(logger_p)), redact_query_params(std::move(redact_query_params_p)),
      redact_headers(std::move(redact_headers_p)) {
}

static Value CreateAzureHeadersValue(const Azure::Core::CaseInsensitiveMap &headers,
                                     const std::unordered_set<std::string> &redact_keys) {
	vector<Value> keys;
	vector<Value> values;
	for (const auto &header : headers) {
		auto lower_key = StringUtil::Lower(header.first);
		keys.emplace_back(lower_key);
		if (redact_keys.count(lower_key) > 0) {
			values.emplace_back("REDACTED");
		} else {
			values.emplace_back(header.second);
		}
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

//! Redact the values of the specified query parameters in the given URL string.
static std::string RedactUrlQueryParams(const std::string &url, const std::unordered_set<std::string> &redact_params) {
	if (redact_params.empty()) {
		return url;
	}
	auto query_start = url.find('?');
	if (query_start == std::string::npos) {
		return url;
	}

	std::string base = url.substr(0, query_start + 1);
	std::string query = url.substr(query_start + 1);

	// Parse and rebuild query string with redacted values
	std::string result = base;
	bool first = true;
	size_t pos = 0;
	while (pos <= query.size()) {
		auto amp = query.find('&', pos);
		std::string param = (amp == std::string::npos) ? query.substr(pos) : query.substr(pos, amp - pos);
		pos = (amp == std::string::npos) ? query.size() + 1 : amp + 1;

		if (param.empty()) {
			continue;
		}

		if (!first) {
			result += '&';
		}
		first = false;

		auto eq = param.find('=');
		if (eq == std::string::npos) {
			result += param;
		} else {
			std::string name = param.substr(0, eq);
			if (redact_params.count(name) > 0) {
				result += name + "=REDACTED";
			} else {
				result += param;
			}
		}
	}
	return result;
}

// Modeled after httpfs ext LogRequest
void HttpLoggingPolicy::LogRequest(Azure::Core::Http::Request &request, timestamp_t start_time, timestamp_t end_time,
                                   const Azure::Core::Http::RawResponse *response) const {
	if (!logger || !logger->ShouldLog(HTTPLogType::NAME, HTTPLogType::LEVEL)) {
		return;
	}
	auto duration_ms = Timestamp::GetEpochMs(end_time) - Timestamp::GetEpochMs(start_time);
	auto logged_url = RedactUrlQueryParams(request.GetUrl().GetAbsoluteUrl(), redact_query_params);
	child_list_t<Value> request_fields = {
	    {"type", Value(request.GetMethod().ToString())},
	    {"url", Value(logged_url)},
	    {"start_time", Value::TIMESTAMPTZ(timestamp_tz_t(start_time))},
	    {"duration_ms", Value::BIGINT(duration_ms)},
	    {"headers", CreateAzureHeadersValue(request.GetHeaders(), redact_headers)},
	};
	Value resp_value;
	if (response) {
		auto duckdb_status = static_cast<HTTPStatusCode>(static_cast<int>(response->GetStatusCode()));
		child_list_t<Value> response_fields = {
		    {"status", Value(EnumUtil::ToString(duckdb_status))},
		    {"reason", Value(response->GetReasonPhrase())},
		    {"headers", CreateAzureHeadersValue(response->GetHeaders(), redact_headers)},
		};
		resp_value = Value::STRUCT(std::move(response_fields));
	}
	child_list_t<Value> top_fields = {
	    {"request", Value::STRUCT(std::move(request_fields))},
	    {"response", std::move(resp_value)},
	};
	logger->WriteLog(HTTPLogType::NAME, HTTPLogType::LEVEL, Value::STRUCT(std::move(top_fields)).ToString());
}

std::unique_ptr<Azure::Core::Http::RawResponse>
HttpLoggingPolicy::Send(Azure::Core::Http::Request &request, Azure::Core::Http::Policies::NextHttpPolicy next_policy,
                        Azure::Core::Context const &context) const {
	auto start_time = Timestamp::GetCurrentTimestamp();
	std::unique_ptr<Azure::Core::Http::RawResponse> result;
	try {
		result = next_policy.Send(request, context);
	} catch (...) {
		LogRequest(request, start_time, Timestamp::GetCurrentTimestamp(), nullptr);
		throw;
	}
	LogRequest(request, start_time, Timestamp::GetCurrentTimestamp(), result.get());
	return result;
}

std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> HttpLoggingPolicy::Clone() const {
	return std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy>(
	    new HttpLoggingPolicy(logger, redact_query_params, redact_headers));
}

} // namespace duckdb
