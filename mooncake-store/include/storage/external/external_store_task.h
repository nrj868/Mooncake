#pragma once

#include <memory>
#include <optional>

#include "storage/external/external_store_backend.h"

namespace mooncake {

// Single owner of an accepted backend task; destruction drains pending work.
class ExternalStoreTaskGuard {
   public:
    ExternalStoreTaskGuard() = default;
    ExternalStoreTaskGuard(std::shared_ptr<ExternalStoreBackend> backend,
                           ExternalStoreBackend::TaskHandle task);
    ~ExternalStoreTaskGuard();

    ExternalStoreTaskGuard(const ExternalStoreTaskGuard&) = delete;
    ExternalStoreTaskGuard& operator=(const ExternalStoreTaskGuard&) = delete;
    ExternalStoreTaskGuard(ExternalStoreTaskGuard&& other) noexcept;
    ExternalStoreTaskGuard& operator=(ExternalStoreTaskGuard&& other);

    explicit operator bool() const { return task_.has_value(); }
    // Consumes the task. Empty and moved-from guards perform no backend work.
    std::vector<tl::expected<void, ErrorCode>> Wait();

   private:
    std::shared_ptr<ExternalStoreBackend> backend_;
    std::optional<ExternalStoreBackend::TaskHandle> task_;
};

}  // namespace mooncake
