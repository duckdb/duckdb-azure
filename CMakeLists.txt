cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME azure)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/azure_extension.cpp
    src/azure_secret.cpp
    src/azure_filesystem.cpp
    src/azure_http_state.cpp
    src/azure_storage_account_client.cpp
    src/azure_blob_filesystem.cpp
    src/azure_dfs_filesystem.cpp
    src/http_state_policy.cpp
    src/azure_parsed_url.cpp)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

find_package(azure-identity-cpp CONFIG)
find_package(azure-storage-blobs-cpp CONFIG)
find_package(azure-storage-files-datalake-cpp CONFIG)

if(NOT ${azure-identity-cpp_FOUND}
   OR NOT ${azure-storage-blobs-cpp_FOUND}
   OR NOT ${azure-storage-files-datalake-cpp_FOUND})
  message(FATAL_ERROR "Azure SDK not found, did you set up vcpkg correctly?")
endif()

target_compile_features(${EXTENSION_NAME} PUBLIC cxx_std_14)
target_compile_features(${LOADABLE_EXTENSION_NAME} PUBLIC cxx_std_14)
set_target_properties(${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME} PROPERTIES
    CXX_STANDARD_REQUIRED ON
)

# Static lib
target_link_libraries(
  ${EXTENSION_NAME} Azure::azure-identity Azure::azure-storage-blobs
  Azure::azure-storage-files-datalake)
target_include_directories(
  ${EXTENSION_NAME} PRIVATE Azure::azure-identity Azure::azure-storage-blobs
                            Azure::azure-storage-files-datalake)

# Loadable binary
target_link_libraries(
  ${LOADABLE_EXTENSION_NAME} Azure::azure-identity
  Azure::azure-storage-blobs Azure::azure-storage-files-datalake)
target_include_directories(
  ${LOADABLE_EXTENSION_NAME}
  PRIVATE Azure::azure-identity Azure::azure-storage-blobs
          Azure::azure-storage-files-datalake)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
