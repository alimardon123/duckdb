cmake_minimum_required(VERSION 2.8.12...3.29)

project(MultiwriterExtension)

include_directories(src/include)

set(MULTIWRITER_EXTENSION_FILES
    multiwriter_extension.cpp
    src/mwdb_format.cpp
    src/mwdb_pk_index.cpp
    src/functions/mwdb_functions.cpp)

build_static_extension(multiwriter ${MULTIWRITER_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(multiwriter ${PARAMETERS} ${MULTIWRITER_EXTENSION_FILES})

install(
  TARGETS multiwriter_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
