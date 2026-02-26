add_library_unity(
  duckdb_storage_multifile
  OBJECT
  mwdb_format.cpp
  mwdb_pk_index.cpp
  multifile_storage.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_multifile>
    PARENT_SCOPE)
