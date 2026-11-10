#include "http_state_policy.hpp"
#include <azure/core/http/http.hpp>
#include "duckdb/common/shared_ptr.hpp"
#include <memory>
#include <string>
#include <utility>

const static std::string CONTENT_LENGTH = "content-length";

namespace duckdb {

HttpStatePolicy::HttpStatePolicy(shared_ptr<AzureHTTPState> http_state) : http_state(std::move(http_state)) {
}

std::unique_ptr<Azure::Core::Http::RawResponse>
HttpStatePolicy::Send(Azure::Core::Http::Request &request, Azure::Core::Http::Policies::NextHttpPolicy next_policy,
                      Azure::Core::Context const &context) const {
	using HttpMethod = ::Azure::Core::Http::HttpMethod;

	// The fact that there is a Clone method in the Azure SDK let me think that the SDK duplicate
	// the policy internally (probably because of multi threading). So we should probably add a mutex
	// here to keep things coherent, but we are only computing some stats (that already use the atomic
	// type) so if the result is not completely exact it will not matter that much.

	const auto &method = request.GetMethod();

	if (HttpMethod::Head == method) {
		http_state->head_count++;
	} else if (HttpMethod::Get == method) {
		http_state->get_count++;
	} else if (HttpMethod::Put == method) {
		http_state->put_count++;
	} else if (HttpMethod::Post == method) {
		http_state->post_count++;
	}

	const auto *body_stream = request.GetBodyStream();
	if (body_stream != nullptr) {
		http_state->total_bytes_sent += body_stream->Length();
	}

	auto result = next_policy.Send(request, context);
	// HEAD's Content-Length describes the object, not a transferred response body.
	if (result != nullptr && method != HttpMethod::Head) {
		const auto &response_body = result->GetBody();
		if (response_body.size() != 0) {
			http_state->total_bytes_received += response_body.size();
		} else {
			// the result of `GetBody().size()` doesn't seen to be accurate (zero)
			// the internal response as a BodyStream but the only wait to get it is by
			// taking the ownership on it. So to compute the size let use teh content-length
			// header it should be ok
			const auto &headers = result->GetHeaders();
			auto it = headers.find(CONTENT_LENGTH);
			if (it != headers.end()) {
				http_state->total_bytes_received += std::stoll(it->second);
			}
		}
	}

	return result;
}

std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy> HttpStatePolicy::Clone() const {
	return std::unique_ptr<Azure::Core::Http::Policies::HttpPolicy>(new HttpStatePolicy(http_state));
}

} // namespace duckdb
