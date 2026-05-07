# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

# CMakeLists.txt for tests
cmake_minimum_required(VERSION 3.20)

project(a_raft_library_tests LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

set(A_BUILD_VARIANT "debug" CACHE STRING
    "Variant to link via a_raft_library::a_raft_library (debug|memory|static|shared)")
set_property(CACHE A_BUILD_VARIANT PROPERTY STRINGS debug memory static shared)

option(A_ENABLE_COVERAGE "Enable code coverage instrumentation" OFF)

find_library(M_LIB m)

find_package(libuv CONFIG REQUIRED)
find_package(h2o_c_library CONFIG REQUIRED)
find_package(the_lz4_library CONFIG REQUIRED)

# Fallback for standalone test builds (when not included via add_subdirectory)
if(NOT TARGET a_raft_library::a_raft_library)
  find_package(a_raft_library CONFIG REQUIRED)
endif()

set(TEST_EXECUTABLES "")

# ---- Targets ----
# ==============================================================================
# test_awal Target (Standard Test)
# ==============================================================================
add_executable(test_awal
  src/test_awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
)

target_include_directories(test_awal PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_awal)

set_target_properties(test_awal PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_awal PRIVATE libuv::uv)
target_link_libraries(test_awal PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_awal PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_awal PRIVATE a_raft_library::a_raft_library)

if(M_LIB)
  target_link_libraries(test_awal PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_awal PRIVATE /W4)
else()
  target_compile_options(test_awal PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_awal PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_awal PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_awal PRIVATE -O0 -g --coverage)
    target_link_options(test_awal PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_awal COMMAND $<TARGET_FILE:test_awal>)
# ==============================================================================
# test_raft_chaos Target (Standard Test)
# ==============================================================================
add_executable(test_raft_chaos
  src/test_raft_chaos.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
)

target_include_directories(test_raft_chaos PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_raft_chaos)

set_target_properties(test_raft_chaos PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_raft_chaos PRIVATE libuv::uv)
target_link_libraries(test_raft_chaos PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_raft_chaos PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_raft_chaos PRIVATE a_raft_library::a_raft_library)

if(M_LIB)
  target_link_libraries(test_raft_chaos PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_raft_chaos PRIVATE /W4)
else()
  target_compile_options(test_raft_chaos PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_raft_chaos PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_raft_chaos PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_raft_chaos PRIVATE -O0 -g --coverage)
    target_link_options(test_raft_chaos PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_raft_chaos COMMAND $<TARGET_FILE:test_raft_chaos>)
# ==============================================================================
# test_raft_cluster Target (Standard Test)
# ==============================================================================
add_executable(test_raft_cluster
  src/test_raft_cluster.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
)

target_include_directories(test_raft_cluster PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_raft_cluster)

set_target_properties(test_raft_cluster PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_raft_cluster PRIVATE libuv::uv)
target_link_libraries(test_raft_cluster PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_raft_cluster PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_raft_cluster PRIVATE a_raft_library::a_raft_library)

if(M_LIB)
  target_link_libraries(test_raft_cluster PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_raft_cluster PRIVATE /W4)
else()
  target_compile_options(test_raft_cluster PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_raft_cluster PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_raft_cluster PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_raft_cluster PRIVATE -O0 -g --coverage)
    target_link_options(test_raft_cluster PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_raft_cluster COMMAND $<TARGET_FILE:test_raft_cluster>)
# ==============================================================================
# test_raft_codec Target (Standard Test)
# ==============================================================================
add_executable(test_raft_codec
  src/test_raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
)

target_include_directories(test_raft_codec PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_raft_codec)

set_target_properties(test_raft_codec PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_raft_codec PRIVATE libuv::uv)
target_link_libraries(test_raft_codec PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_raft_codec PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_raft_codec PRIVATE a_raft_library::a_raft_library)

if(M_LIB)
  target_link_libraries(test_raft_codec PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_raft_codec PRIVATE /W4)
else()
  target_compile_options(test_raft_codec PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_raft_codec PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_raft_codec PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_raft_codec PRIVATE -O0 -g --coverage)
    target_link_options(test_raft_codec PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_raft_codec COMMAND $<TARGET_FILE:test_raft_codec>)
# ==============================================================================
# test_raft_core Target (Standard Test)
# ==============================================================================
add_executable(test_raft_core
  src/test_raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
)

target_include_directories(test_raft_core PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_raft_core)

set_target_properties(test_raft_core PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_raft_core PRIVATE libuv::uv)
target_link_libraries(test_raft_core PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_raft_core PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_raft_core PRIVATE a_raft_library::a_raft_library)

if(M_LIB)
  target_link_libraries(test_raft_core PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_raft_core PRIVATE /W4)
else()
  target_compile_options(test_raft_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_raft_core PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_raft_core PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_raft_core PRIVATE -O0 -g --coverage)
    target_link_options(test_raft_core PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_raft_core COMMAND $<TARGET_FILE:test_raft_core>)
# ==============================================================================
# test_raft_fuzz Target (Fuzzer)
# ==============================================================================
if (CMAKE_C_COMPILER_ID MATCHES "Clang")
  add_executable(test_raft_fuzz
    src/test_raft_fuzz.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
  )

  target_include_directories(test_raft_fuzz PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
  )

  list(APPEND TEST_EXECUTABLES test_raft_fuzz)

  set_target_properties(test_raft_fuzz PROPERTIES
    C_STANDARD 23
    C_STANDARD_REQUIRED YES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
  )

  target_link_libraries(test_raft_fuzz PRIVATE libuv::uv)
  target_link_libraries(test_raft_fuzz PRIVATE h2o_c_library::h2o_c_library)
  target_link_libraries(test_raft_fuzz PRIVATE the_lz4_library::the_lz4_library)
  target_link_libraries(test_raft_fuzz PRIVATE a_raft_library::a_raft_library)

  if(M_LIB)
    target_link_libraries(test_raft_fuzz PRIVATE ${M_LIB})
  endif()

  # THE MAGIC FUZZING FLAGS (ASan + UBSan + libFuzzer)
  target_compile_options(test_raft_fuzz PRIVATE
    -O1 -g -fsanitize=fuzzer,address,undefined -Wall -Wextra -Wpedantic
  )
  target_link_options(test_raft_fuzz PRIVATE
    -fsanitize=fuzzer,address,undefined
    -fuse-ld=lld
  )

  # Add to CTest, but restrict it to run for exactly 2 seconds.
  add_test(NAME test_raft_fuzz COMMAND $<TARGET_FILE:test_raft_fuzz> -max_total_time=2)

else()
  message(WARNING "Skipping 'test_raft_fuzz' - libFuzzer requires Clang/LLVM.")
endif()

# ==============================================================================
# test_raft_io Target (Standard Test)
# ==============================================================================
add_executable(test_raft_io
  src/test_raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/awal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_codec.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_io.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/raft_server.c
)

target_include_directories(test_raft_io PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_raft_io)

set_target_properties(test_raft_io PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_raft_io PRIVATE libuv::uv)
target_link_libraries(test_raft_io PRIVATE h2o_c_library::h2o_c_library)
target_link_libraries(test_raft_io PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_raft_io PRIVATE a_raft_library::a_raft_library)

if(M_LIB)
  target_link_libraries(test_raft_io PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_raft_io PRIVATE /W4)
else()
  target_compile_options(test_raft_io PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_raft_io PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_raft_io PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_raft_io PRIVATE -O0 -g --coverage)
    target_link_options(test_raft_io PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_raft_io COMMAND $<TARGET_FILE:test_raft_io>)

enable_testing()

# ---- Coverage aggregation ----
add_custom_target(coverage_report COMMENT "Generate coverage report")

if(A_ENABLE_COVERAGE AND CMAKE_C_COMPILER_ID MATCHES "Clang")
  include(ProcessorCount)
  ProcessorCount(NPROC)
  if(NPROC EQUAL 0)
    set(NPROC 4)
  endif()

  add_custom_command(TARGET coverage_report PRE_BUILD
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -j ${NPROC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running tests with coverage instrumentation"
  )

  find_program(LLVM_PROFDATA_EXECUTABLE llvm-profdata)
  find_program(LLVM_COV_EXECUTABLE llvm-cov)
  if(LLVM_PROFDATA_EXECUTABLE AND LLVM_COV_EXECUTABLE)
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_PROFDATA_EXECUTABLE} merge -sparse *.profraw -o default.profdata
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Merging coverage data"
    )

    # Normal compiled library path
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_COV_EXECUTABLE} show
              ../liba_raft_library_${A_BUILD_VARIANT}.a
              -instr-profile=default.profdata
              -format=html -output-dir=coverage_html
              --ignore-filename-regex='/usr/.*'
              --show-regions --show-line-counts-or-regions
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generated LLVM coverage report → open coverage_html/index.html"
    )
  endif()
endif()
