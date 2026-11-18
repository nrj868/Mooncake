#include "storage/external/external_store_task.h"

#include <utility>

namespace mooncake {

ExternalStoreTaskGuard::ExternalStoreTaskGuard(
    ExternalStoreBackend* backend, ExternalStoreBackend::TaskHandle task)
    : backend_(backend), task_(task) {}

ExternalStoreTaskGuard::~ExternalStoreTaskGuard() {
    if (task_) {
        ExternalStoreBackend::TaskOutput output;
        (void)Wait(output);
    }
}

ExternalStoreTaskGuard::ExternalStoreTaskGuard(
    ExternalStoreTaskGuard&& other) noexcept
    : backend_(std::exchange(other.backend_, nullptr)),
      task_(std::exchange(other.task_, std::nullopt)) {}

ExternalStoreTaskGuard& ExternalStoreTaskGuard::operator=(
    ExternalStoreTaskGuard&& other) {
    if (this != &other) {
        if (task_) {
            ExternalStoreBackend::TaskOutput output;
            (void)Wait(output);
        }
        backend_ = std::exchange(other.backend_, nullptr);
        task_ = std::exchange(other.task_, std::nullopt);
    }
    return *this;
}

std::vector<tl::expected<void, ErrorCode>> ExternalStoreTaskGuard::Wait(
    ExternalStoreBackend::TaskOutput& output) {
    auto task = std::exchange(task_, std::nullopt);
    if (!task) {
        output = {};
        return {};
    }
    return backend_->Wait(*task, output);
}

}  // namespace mooncake
