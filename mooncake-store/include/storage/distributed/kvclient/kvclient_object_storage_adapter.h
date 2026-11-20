#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "kv_client.h"
#include "kv_types.h"
#include "storage/distributed/kvclient/kvclient_config.h"
#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

inline constexpr char kKvClientAdapterName[] = "kvclient";

// This header includes the KVClient SDK, so only SDK-enabled TUs may include
// it. Keep it out of storage_backend.cpp and every SDK-less unit.
class KvClientObjectStorageAdapter final : public ObjectStorageAdapter {
   public:
    explicit KvClientObjectStorageAdapter(KvClientAdapterConfig config = {});
    // Test seam: inject an SDK client instead of creating one in Init().
    KvClientObjectStorageAdapter(KvClientAdapterConfig config,
                                 std::unique_ptr<kv::KvClient> client);
    ~KvClientObjectStorageAdapter() override;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override;
    std::vector<tl::expected<void, ErrorCode>> PutBatch(
        const std::vector<ObjectPutRequest>& requests) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override;
    std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) override;
    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override;
    tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) override;
    ObjectStorageIoResults DeleteBatch(
        std::span<const std::string> logical_keys) override;
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;

    tl::expected<void, ErrorCode> Init() override;
    const char* GetName() const override { return kKvClientAdapterName; }

   private:
    struct Completion {
        bool terminal = false;
        std::vector<kv::Status> entryStatus;
    };

    kv::BinaryKey MakeCacheKey(std::string_view logical_key) const;
    Completion waitForCompletion(kv::TaskId task_id, size_t entry_count);
    static ErrorCode ToErrorCode(const kv::Status& status);

    KvClientAdapterConfig config_;
    std::unique_ptr<kv::KvClient> kv_client_;
    bool initialized_ = false;
};

}  // namespace mooncake
