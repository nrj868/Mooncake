#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "environment_variables.h"
#include "environ.h"
#include "types.h"

namespace mooncake {

// SDK-free configuration unit: this header must stay includable without the
// KVClient SDK.
inline constexpr uint64_t kKvClientMaxValueBytes = 16'776'704;
// Same value as the SDK's kAlignmentBytes; declared locally so this
// configuration unit does not depend on SDK headers.
inline constexpr size_t kKvClientAlignment = 512;

struct KvClientAdapterConfig {
    std::string config_path;
    uint64_t max_object_bytes = kKvClientMaxValueBytes;
    uint64_t wait_timeout_ms = 30'000;

    // Environ::ReadOr silently falls back to the default for malformed
    // numbers; reject explicitly-set-but-unparseable values instead.
    static tl::expected<uint64_t, ErrorCode> ReadUint(
        const EnvironmentVariable<uint64_t>& variable,
        uint64_t default_value) {
        const auto raw = Environ::Process().Get(variable.name);
        if (!raw.has_value()) return default_value;
        const auto parsed = Environ::Process().GetTyped(variable);
        if (!parsed.has_value()) {
            LOG(ERROR) << "Invalid value for env " << variable.name << ": '"
                       << *raw << "'";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return *parsed;
    }

    static tl::expected<KvClientAdapterConfig, ErrorCode> FromEnvironment() {
        KvClientAdapterConfig cfg;
        cfg.config_path = Environ::ReadOr(
            KvClientEnvironmentVariables::MOONCAKE_KVCLIENT_CONFIG,
            std::string{});

        auto max_object_bytes = ReadUint(
            KvClientEnvironmentVariables::MOONCAKE_KVCLIENT_MAX_OBJECT_BYTES,
            kKvClientMaxValueBytes);
        if (!max_object_bytes)
            return tl::make_unexpected(max_object_bytes.error());
        cfg.max_object_bytes = *max_object_bytes;

        auto wait_timeout_ms = ReadUint(
            KvClientEnvironmentVariables::MOONCAKE_KVCLIENT_WAIT_TIMEOUT_MS,
            uint64_t{30'000});
        if (!wait_timeout_ms)
            return tl::make_unexpected(wait_timeout_ms.error());
        cfg.wait_timeout_ms = *wait_timeout_ms;

        if (!cfg.Validate())
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return cfg;
    }

    bool Validate() const {
        return !config_path.empty() &&
               max_object_bytes > 0 &&
               max_object_bytes % kKvClientAlignment == 0 &&
               max_object_bytes <= kKvClientMaxValueBytes &&
               wait_timeout_ms > 0;
    }
};

}  // namespace mooncake
