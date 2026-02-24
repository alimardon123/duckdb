cmake_minimum_required(VERSION 2.8.12...3.29)

project(DistributedExtension)

include_directories(include)

set(DISTRIBUTED_EXTENSION_FILES
    distributed_extension.cpp
    distributed_context.cpp
    worker_server.cpp)

build_static_extension(distributed ${DISTRIBUTED_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(distributed ${PARAMETERS} ${DISTRIBUTED_EXTENSION_FILES})

# Link against pthread for thread support
find_package(Threads REQUIRED)
target_link_libraries(distributed_extension Threads::Threads)
target_link_libraries(distributed_loadable_extension Threads::Threads)

install(
  TARGETS distributed_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
