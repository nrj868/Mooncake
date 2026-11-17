#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

// Vendor-neutral factory declaration: this header is SDK-free and is the only
// kvclient header storage_backend.cpp may include.
tl::expected<std::unique_ptr<ObjectStorageAdapter>, ErrorCode>
CreateKvClientObjectStorageAdapter();

}  // namespace mooncake
