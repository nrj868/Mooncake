#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

#include "environment_variables.h"
#include "environ.h"
#include "types.h"

namespace mooncake {

// SDK-free configuration unit: this header must stay includable without the
// KVClient SDK.
struct KvClientAdapterConfig {
    std::string config_path;

    static tl::expected<KvClientAdapterConfig, ErrorCode> FromEnvironment() {
        KvClientAdapterConfig cfg;
        cfg.config_path = Environ::ReadOr(
            KvClientEnvironmentVariables::MOONCAKE_KVCLIENT_CONFIG,
            std::string{});
        if (!cfg.Validate())
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        return cfg;
    }

    bool Validate() const { return !config_path.empty(); }
};

}  // namespace mooncake
