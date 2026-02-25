#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class MultiwriterExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override { return "multiwriter"; }
    std::string Version() const override { return "v0.1.0"; }
};

} // namespace duckdb
