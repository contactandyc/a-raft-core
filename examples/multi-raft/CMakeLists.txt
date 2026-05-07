# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(a_raft_library_multi-raft LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

find_library(M_LIB m)

find_package(libuv CONFIG REQUIRED)
find_package(h2o_c_library CONFIG REQUIRED)
find_package(the_lz4_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(client
  src/client.c
)

target_include_directories(client PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(client PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_raft_library::a_raft_library)
  find_package(a_raft_library CONFIG REQUIRED)
endif()
# target_link_libraries(client PRIVATE a_raft_library::a_raft_library)

target_link_libraries(client PRIVATE
  libuv::uv
  h2o_c_library::h2o_c_library
  the_lz4_library::the_lz4_library
  a_raft_library::a_raft_library
)

if(M_LIB)
  target_link_libraries(client PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(client PRIVATE /W4)
else()
  target_compile_options(client PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS client RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
add_executable(server
  src/server.c
)

target_include_directories(server PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(server PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_raft_library::a_raft_library)
  find_package(a_raft_library CONFIG REQUIRED)
endif()
# target_link_libraries(server PRIVATE a_raft_library::a_raft_library)

target_link_libraries(server PRIVATE
  libuv::uv
  h2o_c_library::h2o_c_library
  the_lz4_library::the_lz4_library
  a_raft_library::a_raft_library
)

if(M_LIB)
  target_link_libraries(server PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(server PRIVATE /W4)
else()
  target_compile_options(server PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS server RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
