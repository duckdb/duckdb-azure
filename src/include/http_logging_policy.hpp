#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include <azure/core/context.hpp>
#include <azure/core/http/http.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/core/http/raw_response.hpp>
#include <memory>
#include <string>
#include <unordered_set>

namespace duckdb {

class Logger;

class HttpLoggingPolicy : public Azure::Core::Http::Policies::HttpPolicy {
public:
	HttpLoggingPolicy(shared_ptr<Logger> logger, std::unordered_set<std::string> redact_query_params,
	                  std::unordered_set<std::string> redact_headers);

	std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request &request,
	                                                     Azure::Core::Http::Policies::NextHttpPolicy next_policy,
	                                                     Azure::Core::Context const &context) const override;

	std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> Clone() const override;

private:
	shared_ptr<Logger> logger;
	std::unordered_set<std::string> redact_query_params;
	std::unordered_set<std::string> redact_headers;
};

} // namespace duckdb
