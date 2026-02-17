set(CORE_TOOLS
  sst_dump.cc
  ldb.cc)
foreach(src ${CORE_TOOLS})
  get_filename_component(exename ${src} NAME_WE)
  add_executable(${exename}${ARTIFACT_SUFFIX}
    ${src})
  target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB})
  list(APPEND core_tool_deps ${exename})
endforeach()

if(WITH_TOOLS)
  set(TOOLS
    db_sanity_test.cc
    write_stress.cc
    db_repl_stress.cc
    dump/rocksdb_dump.cc
    dump/rocksdb_undump.cc)
  foreach(src ${TOOLS})
    get_filename_component(exename ${src} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX}
      ${src})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
    list(APPEND tool_deps ${exename})
  endforeach()

  add_custom_target(ldb_tests
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/ldb_tests.py
    DEPENDS ldb)
endif()

# eRPC BloomCheck client (optional, requires ERPC_ROOT)
if(ERPC_ROOT AND EXISTS "${ERPC_ROOT}")
  # Find eRPC library (prefer build_perf/liberpc.a)
  set(ERPC_LIB_PATH "${ERPC_ROOT}/build_perf/liberpc.a")
  if(NOT EXISTS "${ERPC_LIB_PATH}")
    set(ERPC_LIB_PATH "${ERPC_ROOT}/build/liberpc.a")
  endif()
  
  if(EXISTS "${ERPC_LIB_PATH}")
    # Include directories
    set(ERPC_INCLUDE_DIRS
      "${ERPC_ROOT}/include"
      "${ERPC_ROOT}/src"
      "${ERPC_ROOT}"
      "${ERPC_ROOT}/third_party/asio/include")
    
    # Add bloomcheck_erpc_client executable
    add_executable(bloomcheck_erpc_client${ARTIFACT_SUFFIX}
      bloomcheck_erpc_client.cc
      ../util/bloomcheck_erpc_client.cc
      bloomcheck.pb.cc)
    
    target_include_directories(bloomcheck_erpc_client${ARTIFACT_SUFFIX} PRIVATE
      ${ERPC_INCLUDE_DIRS}
      ${CMAKE_CURRENT_SOURCE_DIR}/..)

    # eRPC headers trigger some warnings under our C++20 + -Werror toolchain.
    target_compile_options(bloomcheck_erpc_client${ARTIFACT_SUFFIX} PRIVATE
      -Wno-error=volatile
      -Wno-error=unused-function)
    target_compile_definitions(bloomcheck_erpc_client${ARTIFACT_SUFFIX} PRIVATE
      ERPC_INFINIBAND)
    
    # Link libraries
    target_link_libraries(bloomcheck_erpc_client${ARTIFACT_SUFFIX}
      ${ERPC_LIB_PATH}
      pthread
      numa
      ibverbs
      protobuf)
    
    # Add to tool dependencies
    list(APPEND tool_deps bloomcheck_erpc_client)
    
    message(STATUS "eRPC BloomCheck client enabled: ERPC_ROOT=${ERPC_ROOT}")
  else()
    message(WARNING "eRPC library not found at ${ERPC_LIB_PATH}, skipping bloomcheck_erpc_client")
  endif()
else()
  message(STATUS "eRPC BloomCheck client disabled: ERPC_ROOT not set or invalid")
endif()
