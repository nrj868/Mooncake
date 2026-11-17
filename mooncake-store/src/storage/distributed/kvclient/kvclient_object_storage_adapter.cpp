#include "storage/distributed/kvclient/kvclient_object_storage_adapter.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <xxhash.h>

#include "storage/distributed/kvclient/kvclient_adapter_factory.h"

namespace mooncake {
namespace {

inline constexpr uint64_t kKvClientHashSeed = 5381;

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
    // No separate Shutdown() method: stop accepting work and let the SDK drain
    // pending tasks.
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
    if (!client)
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);

    kv::KvClientConfig cfg;
    if (!kv::KvClientConfig::LoadFromFile(config_.config_path, cfg).ok())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    cfg.keyResolution = {};  // Force GENERIC.
    if (cfg.timeoutMs < config_.wait_timeout_ms) {
        LOG(ERROR) << "KVClient config timeoutMs=" << cfg.timeoutMs
                   << " is shorter than wait_timeout_ms="
                   << config_.wait_timeout_ms;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Init is not idempotent; the local client is only committed to the member
    // after this succeeds, so a retry starts from a fresh client.
    if (!client->Init(cfg).ok())
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);

    kv_client_ = std::move(client);
    initialized_ = true;
    LOG(INFO) << "KVClient adapter initialized, max_object_bytes="
              << config_.max_object_bytes;
    return {};
}

ErrorCode KvClientObjectStorageAdapter::ToErrorCode(const kv::Status& status) {
    using kv::StatusCode;
    switch (status.code) {
        case StatusCode::OK:
            return ErrorCode::OK;
        case StatusCode::TIMEOUT:
        case StatusCode::CQE_IO_TIMEOUT:
            return ErrorCode::RPC_TIMEOUT;
        case StatusCode::IN_PROGRESS:
        case StatusCode::CANCELED:
            return ErrorCode::INTERNAL_ERROR;

        // Object misses: only entry-level key-not-found is a miss.
        case StatusCode::CQE_KEY_NOT_EXIST:
        case StatusCode::ENTRY_KEY_NOT_FOUND:
        case StatusCode::ENTRY_KEY_NOT_EXIST:
            return ErrorCode::OBJECT_NOT_FOUND;

        // NOT_FOUND is a routing/config/view error, not a missing object.
        // Mapping it to OBJECT_NOT_FOUND would let PR3 delete metadata and
        // PR2 evict live metadata on a routing failure.
        case StatusCode::NOT_FOUND:
        case StatusCode::NOT_INITIALIZED:
        case StatusCode::CONNECTION_ERROR:
        case StatusCode::IO_ERROR:
        case StatusCode::UNSUPPORTED:
        // Delete entry failures use ENTRY_DELETE_FAILED; ENTRY_KEY_NOT_EXIST
        // is produced only for QUERY.
        case StatusCode::ENTRY_DELETE_FAILED:
            return ErrorCode::DFS_SERVICE_UNAVAILABLE;

        case StatusCode::INVALID_ARGUMENT:
        case StatusCode::BUFFER_NOT_REGISTERED:
        case StatusCode::BUFFER_NOT_SUPPORTED:
            return ErrorCode::INVALID_PARAMS;
        case StatusCode::CQE_OUT_OF_CREATE_SIZE:
        case StatusCode::RESOURCE_BUSY:
        case StatusCode::CQE_RESOURCE_BUSY:
            return ErrorCode::NO_AVAILABLE_HANDLE;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

KvClientObjectStorageAdapter::Completion
KvClientObjectStorageAdapter::waitForCompletion(kv::TaskId task_id,
                                                size_t entry_count) {
    kv::TaskResult result;
    const auto wait_status =
        kv_client_->Wait(task_id, config_.wait_timeout_ms, result);
    const auto code = wait_status.ok() ? result.status.code : wait_status.code;
    const kv::Status& task_status =
        wait_status.ok() ? result.status : wait_status;

    Completion completion;
    switch (code) {
        case kv::StatusCode::TASK_NOT_FOUND:
            // Wait removes the task even after a timeout, so an unknown task
            // does not prove that its I/O has finished. Keep it non-terminal
            // so callers retain handles and staging buffers.
            completion.terminal = false;
            completion.entryStatus.assign(
                entry_count,
                kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                                  "task not found"));
            return completion;
        case kv::StatusCode::TIMEOUT:
        case kv::StatusCode::IN_PROGRESS:
        case kv::StatusCode::CANCELED:
            // The SDK may have populated entryStatus before returning the
            // timeout, but the task is not terminal; per-entry results are not
            // trustworthy. Fill with the task-level status and retain
            // resources.
            completion.terminal = false;
            completion.entryStatus.assign(entry_count, task_status);
            return completion;
        default:
            // The task finished: OK/PARTIAL_FAILED and any task-level final
            // error are terminal.
            completion.terminal = true;
            break;
    }

    if (result.entryStatus.size() != entry_count) {
        completion.entryStatus.assign(
            entry_count,
            kv::Status::Error(kv::StatusCode::INTERNAL_ERROR,
                              "entry count mismatch"));
        return completion;
    }
    completion.entryStatus = std::move(result.entryStatus);
    return completion;
}

kv::BinaryKey KvClientObjectStorageAdapter::MakeCacheKey(
    std::string_view logical_key) const {
    uint64_t hash =
        XXH64(logical_key.data(), logical_key.size(), kKvClientHashSeed);
    // The protocol rejects an all-zero key; map it to the seed to keep the
    // wire key non-zero without a second constant.
    if (hash == 0) hash = kKvClientHashSeed;
    kv::BinaryKey key{};
    // Pack explicitly little-endian like the SDK's key_resolver; a memcpy of
    // the host-order uint64_t would differ on big-endian clients.
    for (size_t i = 0; i < sizeof(hash); ++i)
        key[i] = static_cast<std::byte>(hash >> (8 * i));
    return key;
}

tl::expected<void, ErrorCode> KvClientObjectStorageAdapter::Delete(
    const std::string& key) {
    std::array<std::string, 1> keys{key};
    auto results = DeleteBatch(keys);
    return results.empty()
               ? tl::make_unexpected(ErrorCode::INVALID_PARAMS)
               : std::move(results.front());
}

ObjectStorageIoResults KvClientObjectStorageAdapter::DeleteBatch(
    std::span<const std::string> keys) {
    if (!initialized_ || !kv_client_) {
        return ObjectStorageIoResults(
            keys.size(),
            tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    ObjectStorageIoResults results(keys.size());
    if (keys.empty()) return results;

    std::vector<kv::CacheKey> cache_keys;
    std::vector<size_t> indices;
    cache_keys.reserve(keys.size());
    indices.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (keys[i].empty()) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        cache_keys.push_back(kv::CacheKey::FromBytes(MakeCacheKey(keys[i])));
        indices.push_back(i);
    }
    if (cache_keys.empty()) return results;

    kv::TaskId task_id = kv::kInvalidTaskId;
    const auto submit = kv_client_->DeleteAsync(cache_keys, task_id);
    if (!submit.ok()) {
        for (size_t i : indices)
            results[i] = tl::make_unexpected(ToErrorCode(submit));
        return results;
    }

    const auto completion = waitForCompletion(task_id, cache_keys.size());
    for (size_t j = 0; j < indices.size(); ++j) {
        results[indices[j]] = completion.entryStatus[j].ok()
                                  ? tl::expected<void, ErrorCode>{}
                                  : tl::make_unexpected(
                                        ToErrorCode(completion.entryStatus[j]));
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
