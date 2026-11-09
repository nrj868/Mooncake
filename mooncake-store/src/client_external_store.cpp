#include "client_service.h"

#include <algorithm>
#include <utility>
#include <glog/logging.h>
#include <ylt/struct_json/json_reader.h>

#include "environ.h"
#include "storage/external/external_store_backend_factory.h"

namespace mooncake {

tl::expected<void, ErrorCode>
Client::InitExternalStoreBackendFromEnvironment() {
    const auto backend_type =
        Environ::GetString("MC_EXTERNAL_STORE_BACKEND", "");
    if (backend_type.empty()) return {};
    std::unordered_map<std::string, std::string> options;
    try {
        struct_json::from_json(
            options, Environ::GetString("MC_EXTERNAL_STORE_OPTIONS", "{}"));
    } catch (const std::exception& error) {
        LOG(ERROR) << "Invalid MC_EXTERNAL_STORE_OPTIONS: " << error.what();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto config_path = Environ::GetString("MC_EXTERNAL_STORE_CONFIG", "");
    if (!config_path.empty()) options["config_path"] = config_path;
    auto backend = CreateExternalStoreBackend(backend_type);
    if (!backend) {
        LOG(ERROR) << "Failed to create external store backend: "
                   << toString(backend.error());
        return tl::unexpected(backend.error());
    }
    auto initialized = SetExternalStoreBackend(*backend, options);
    if (!initialized) {
        LOG(ERROR) << "Failed to initialize external store backend: "
                   << toString(initialized.error());
    }
    return initialized;
}

tl::expected<void, ErrorCode> Client::SetExternalStoreBackend(
    std::shared_ptr<ExternalStoreBackend> backend,
    const std::unordered_map<std::string, std::string>& options) {
    if (external_store_backend_ || !backend) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto initialized = backend->Init(options);
    if (!initialized) return tl::unexpected(initialized.error());
    external_store_backend_ = std::move(backend);
    return {};
}

tl::expected<ExternalStoreTaskGuard, ErrorCode> Client::BatchPutExternalAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& sources) {
    if (keys.empty() || !external_store_backend_)
        return ExternalStoreTaskGuard{};
    auto submitted = external_store_backend_->BatchPutAsync(
        keys, sources, TenantId(master_client_.tenant_id()));
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_, *submitted);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode>
Client::BatchPutExternalRangesAsync(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& sources,
    const std::vector<std::vector<size_t>>& destination_offsets) {
    if (keys.empty() || !external_store_backend_)
        return ExternalStoreTaskGuard{};
    auto submitted = external_store_backend_->BatchPutRangesAsync(
        keys, sources, destination_offsets,
        TenantId(master_client_.tenant_id()));
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_, *submitted);
}

tl::expected<ExternalStoreTaskGuard, ErrorCode>
Client::BatchRemoveExternalAsync(const std::vector<ObjectKey>& keys) {
    if (keys.empty() || !external_store_backend_)
        return ExternalStoreTaskGuard{};
    auto submitted = external_store_backend_->BatchRemoveAsync(
        keys, TenantId(master_client_.tenant_id()));
    if (!submitted) return tl::unexpected(submitted.error());
    return ExternalStoreTaskGuard(external_store_backend_, *submitted);
}

tl::expected<void, ErrorCode> Client::CompleteExternalPut(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission,
    tl::expected<void, ErrorCode> native_result) {
    CompleteExternalPut(submission, std::span(&native_result, 1));
    return native_result;
}

// A key succeeds if either storage path succeeds; always wait for submitted
// work.
void Client::CompleteExternalPut(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission,
    std::span<tl::expected<void, ErrorCode>> native_results) {
    if (!submission) {
        LOG(WARNING) << "External store submission failed: "
                     << submission.error();
        return;
    }
    if (!*submission) return;
    auto external = submission->Wait();
    if (external.size() != native_results.size()) {
        LOG(ERROR) << "External store result count mismatch: expected="
                   << native_results.size() << ", actual=" << external.size();
        return;
    }
    for (size_t i = 0; i < external.size(); ++i) {
        if (!external[i]) {
            LOG(WARNING) << "External store operation failed at index=" << i
                         << ": " << external[i].error();
        } else {
            native_results[i] = {};
        }
    }
}

void Client::CompleteExternalRangePut(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission,
    const std::vector<std::vector<Slice>>& slices,
    std::vector<tl::expected<int64_t, ErrorCode>>& transfer) {
    std::vector<tl::expected<void, ErrorCode>> statuses(transfer.size());
    for (size_t i = 0; i < transfer.size(); ++i) {
        if (!transfer[i]) statuses[i] = tl::unexpected(transfer[i].error());
    }
    CompleteExternalPut(submission, std::span(statuses));
    for (size_t i = 0; i < transfer.size(); ++i) {
        if (!transfer[i] && statuses[i]) {
            // Count logical bytes once when only the external write
            // succeeds.
            int64_t bytes = 0;
            for (const auto& slice : slices[i]) bytes += slice.size;
            transfer[i] = bytes;
        }
    }
}

// Drain external deletes for diagnostics; callers return the native result.
void Client::CompleteExternalRemove(
    tl::expected<ExternalStoreTaskGuard, ErrorCode>& submission) {
    if (!submission) {
        LOG(WARNING) << "External delete submission failed: "
                     << submission.error();
    } else if (*submission) {
        auto results = submission->Wait();
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i]) {
                LOG(WARNING) << "External delete failed at index=" << i << ": "
                             << results[i].error();
            }
        }
    }
}

}  // namespace mooncake
