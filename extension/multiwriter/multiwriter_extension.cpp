//===----------------------------------------------------------------------===//
// multiwriter_extension.cpp  –  Extension entry point
//===----------------------------------------------------------------------===//
#include "multiwriter_extension.hpp"
#include "functions/mwdb_functions.hpp"

namespace duckdb {

void MultiwriterExtension::Load(ExtensionLoader &loader) {
    RegisterMWDBFunctions(loader);
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(multiwriter, loader) {
    duckdb::MultiwriterExtension ext;
    ext.Load(loader);
}

}
