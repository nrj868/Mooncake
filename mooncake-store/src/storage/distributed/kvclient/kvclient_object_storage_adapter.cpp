#include "storage/distributed/kvclient/kvclient_object_storage_adapter.h"

#include <array>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "storage/distributed/kvclient/kvclient_adapter_factory.h"

namespace mooncake {
namespace {

template <typename T>
std::vector<tl::expected<T, ErrorCode>> Errors(size_t count, ErrorCode error) {
    std::vector<tl::expected<T, ErrorCode>> results;
    results.reserve(count);
    for (size_t i = 0; i < count; ++i)
        results.emplace_back(tl::make_unexpected(error));
    return results;
}

}  // namespace

KvClientObjectStorageAdapter::KvClientObjectStorageAdapter(
    KvClientAdapterConfig config)
    : config_(std::move(config)) {}

KvClientObjectStorageAdapter::KvClientObjectStorageAdapter(
    KvClientAdapterConfig config, std::unique_ptr<kv::KvClient> client)
    : config_(std::move(config)), kv_client_(std::move(client)) {}

KvClientObjectStorageAdapter::~KvClientObjectStorageAdapter() {
    if (kv_client_) kv_client_->Shutdown();
}

tl::expected<void, ErrorCode> KvClientObjectStorageAdapter::Init() {
    if (initialized_) return {};
    if (!config_.Validate())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    // Injected client (test seam): only the adapter config is validated.
    if (kv_client_) {
        initialized_ = true;
        return {};
    }

    auto client = kv::CreateKvClient();
    if (!client) return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);

    kv::KvClientConfig cfg;
    // The adapter needs the generic resolver so logical keys are hashed
    // verbatim, and the batch protocol so single-key requests carry per-entry
    // results.
    if (!kv::KvClientConfig::LoadFromFile(config_.config_path, cfg).ok() ||
        cfg.keyResolution.engine != kv::InferenceEngine::GENERIC ||
        !cfg.useBatchIoForSingleKey) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Init is not idempotent; the local client is only committed to the member
    // after this succeeds, so a retry starts from a fresh client.
    if (!client->Init(cfg).ok())
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);

    kv_client_ = std::move(client);
    initialized_ = true;
    LOG(INFO) << "KVClient adapter initialized";
    return {};
}

ErrorCode KvClientObjectStorageAdapter::ToErrorCode(const kv::Status& status,
                                                    OpKind op) {
    using kv::StatusCode;
    switch (status.code) {
        case StatusCode::OK:
            return ErrorCode::OK;

        case StatusCode::TIMEOUT:
        case StatusCode::CQE_IO_TIMEOUT:
            return ErrorCode::RPC_TIMEOUT;

        case StatusCode::CANCELED:
            return ErrorCode::TASK_CANCELED;

        // NOT_FOUND is a route/view error, not a missing object. Mapping it to
        // OBJECT_NOT_FOUND would make the read path evict a live DFS replica.
        case StatusCode::NOT_FOUND:
        case StatusCode::NOT_INITIALIZED:
        case StatusCode::CONNECTION_ERROR:
        case StatusCode::BUFFER_NOT_REGISTERED:
        case StatusCode::CQE_WRITE_FAULT:
        case StatusCode::CQE_UNRECOVERED_READ_ERROR:
            return ErrorCode::DFS_SERVICE_UNAVAILABLE;

        case StatusCode::ENTRY_DELETE_FAILED:
            return op == OpKind::kDelete ? ErrorCode::DFS_SERVICE_UNAVAILABLE
                                         : ErrorCode::INTERNAL_ERROR;

        case StatusCode::ENTRY_RETRY_ADVISED:
            return (op == OpKind::kStore || op == OpKind::kLoad)
                       ? ErrorCode::DFS_SERVICE_UNAVAILABLE
                       : ErrorCode::INTERNAL_ERROR;

        case StatusCode::ENTRY_NO_RETRY_ADVISED:
            return ErrorCode::INTERNAL_ERROR;

        // LOAD and BATCH_STORE share one decoder: a missing entry is a cache
        // miss on read and a failed write on store.
        case StatusCode::ENTRY_KEY_NOT_FOUND:
        case StatusCode::ENTRY_DATA_NOT_EXIST:
            if (op == OpKind::kLoad) return ErrorCode::OBJECT_NOT_FOUND;
            if (op == OpKind::kStore) return ErrorCode::FILE_WRITE_FAIL;
            return ErrorCode::INTERNAL_ERROR;

        case StatusCode::CQE_KEY_NOT_EXIST:
            if (op == OpKind::kLoad) return ErrorCode::OBJECT_NOT_FOUND;
            return ErrorCode::INTERNAL_ERROR;

        case StatusCode::INVALID_ARGUMENT:
            return ErrorCode::INVALID_PARAMS;

        case StatusCode::RESOURCE_BUSY:
        case StatusCode::CQE_RESOURCE_BUSY:
        case StatusCode::CQE_OUT_OF_CREATE_SIZE:
            return ErrorCode::NO_AVAILABLE_HANDLE;

        // Remaining codes cannot be produced for these operations, or are
        // protocol anomalies: ENTRY_KEY_EXIST/ENTRY_KEY_NOT_EXIST are consumed
        // by Exists, IO_ERROR is the unknown-result fallback, and
        // CQE_INTERNAL_ERROR, CQE_INVALID_COMMAND_OPCODE,
        // CQE_INVALID_FIELD_IN_COMMAND, CQE_KEY_ALREADY_EXISTED and
        // CQE_CHECK_RESULT_BUFFER are not valid outcomes here.
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

KvClientObjectStorageAdapter::Completion
KvClientObjectStorageAdapter::waitForCompletion(kv::TaskId task_id,
                                                size_t entry_count) {
    kv::TaskResult result;
    const auto wait_status = kv_client_->Wait(task_id, 0, result);
    const auto code = wait_status.ok() ? result.status.code : wait_status.code;
    const kv::Status& task_status =
        wait_status.ok() ? result.status : wait_status;

    Completion completion;
    if (code == kv::StatusCode::OK || code == kv::StatusCode::PARTIAL_FAILED) {
        if (result.entryStatus.size() != entry_count) {
            completion.entryStatus.assign(
                entry_count, kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                               "entry count mismatch"));
            return completion;
        }
        completion.entryStatus = std::move(result.entryStatus);
        return completion;
    }

    // Wait consumes the task even on timeout, so no task-level error is
    // retryable here; report it for every entry.
    completion.entryStatus.assign(entry_count, task_status);
    return completion;
}

tl::expected<void, ErrorCode> KvClientObjectStorageAdapter::Delete(
    const std::string& key) {
    std::array<std::string, 1> keys{key};
    auto results = DeleteBatch(keys);
    return results.empty() ? tl::make_unexpected(ErrorCode::INVALID_PARAMS)
                           : std::move(results.front());
}

ObjectStorageIoResults KvClientObjectStorageAdapter::DeleteBatch(
    std::span<const std::string> keys) {
    if (!initialized_ || !kv_client_) {
        return Errors<void>(keys.size(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (keys.empty()) return {};

    std::vector<kv::CacheKey> cache_keys;
    cache_keys.reserve(keys.size());
    for (const auto& key : keys) {
        if (key.empty())
            return Errors<void>(keys.size(), ErrorCode::INVALID_PARAMS);
        cache_keys.push_back(kv::CacheKey::FromString(key));
    }

    kv::TaskId task_id = kv::kInvalidTaskId;
    const auto submit = kv_client_->DeleteAsync(cache_keys, task_id);
    if (!submit.ok())
        return Errors<void>(keys.size(), ToErrorCode(submit, OpKind::kDelete));

    const auto completion = waitForCompletion(task_id, cache_keys.size());
    ObjectStorageIoResults results(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& status = completion.entryStatus[i];
        // Deleting an absent key is success; "not exist" is the end state the
        // caller asked for.
        results[i] =
            (status.ok() || status.code == kv::StatusCode::CQE_KEY_NOT_EXIST)
                ? tl::expected<void, ErrorCode>{}
                : tl::make_unexpected(ToErrorCode(status, OpKind::kDelete));
    }
    return results;
}

tl::expected<void, ErrorCode> KvClientObjectStorageAdapter::Put(
    const std::string&, std::span<const char>) {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

tl::expected<void, ErrorCode> KvClientObjectStorageAdapter::PutV(
    const std::string&, const iovec*, int) {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

std::vector<tl::expected<void, ErrorCode>>
KvClientObjectStorageAdapter::PutBatch(
    const std::vector<ObjectPutRequest>& requests) {
    return Errors<void>(requests.size(), ErrorCode::NOT_SUPPORTED);
}

tl::expected<size_t, ErrorCode> KvClientObjectStorageAdapter::Get(
    const std::string&, void*, size_t) {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

std::vector<tl::expected<size_t, ErrorCode>>
KvClientObjectStorageAdapter::GetBatch(
    const std::vector<ObjectGetRequest>& requests) {
    return Errors<size_t>(requests.size(), ErrorCode::NOT_SUPPORTED);
}

tl::expected<bool, ErrorCode> KvClientObjectStorageAdapter::Exists(
    const std::string&) {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
KvClientObjectStorageAdapter::ListKeys() {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

tl::expected<std::unique_ptr<ObjectStorageAdapter>, ErrorCode>
CreateKvClientObjectStorageAdapter() {
    auto config = KvClientAdapterConfig::FromEnvironment();
    if (!config) return tl::make_unexpected(config.error());
    return std::make_unique<KvClientObjectStorageAdapter>(*config);
}

}  // namespace mooncake
