//===----------------------------------------------------------------------===//
//                         DuckDB
//
// distributed_extension.hpp
//
// Distributed query execution extension for DuckDB.
// Implements a coordinator-worker model where multiple DuckDB instances
// can collaborate to execute queries across distributed datasets.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DistributedExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
