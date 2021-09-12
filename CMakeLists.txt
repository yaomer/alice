cmake_minimum_required (VERSION 2.8)

project (alice)

set (CMAKE_CXX_FLAGS "-std=c++2a -Wall -O2")

set (SERVER "${PROJECT_SOURCE_DIR}/src")
set (MMDB "${SERVER}/mmdb")
set (SSDB "${SERVER}/ssdb")

set (PROXY "${PROJECT_SOURCE_DIR}/proxy")

set (CLIAPI "${PROJECT_SOURCE_DIR}/client")

set (SERVER_SRC
    ${SERVER}/server.cc
    ${SERVER}/config.cc
    ${SERVER}/util.cc
    ${SERVER}/parser.cc
    ${SERVER}/sentinel.cc
    ${MMDB}/mmdb.cc
    ${MMDB}/mm_string.cc
    ${MMDB}/mm_list.cc
    ${MMDB}/mm_hash.cc
    ${MMDB}/mm_set.cc
    ${MMDB}/mm_zset.cc
    ${MMDB}/mm_sort.cc
    ${MMDB}/rdb.cc
    ${MMDB}/aof.cc
    ${MMDB}/evict.cc
    ${SSDB}/ssdb.cc
    ${SSDB}/ss_list.cc
    ${SSDB}/ss_string.cc
    ${SSDB}/ss_hash.cc
    ${SSDB}/ss_set.cc
    # ${SSDB}/ss_zset.cc
)

set (CLIENT_SRC
    ${SERVER}/client.cc
    ${SERVER}/hint.cc
    ${SERVER}/util.cc
)

set (PROXY_SRC
    ${PROXY}/proxy.cc
    ${SERVER}/util.cc)

set (CLIAPI_SRC
    ${CLIAPI}/client.cc
    ${SERVER}/util.cc)

set (EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})

set (LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR})

add_library(angel STATIC IMPORTED)
set_target_properties(angel PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libangel.a")

add_library(leveldb STATIC IMPORTED)
set_target_properties(leveldb PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libleveldb.a")

add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libsnappy.a")

add_library(linenoise STATIC IMPORTED)
set_target_properties(linenoise PROPERTIES IMPORTED_LOCATION "/usr/local/lib/liblinenoise.a")

add_executable(alice-server ${SERVER_SRC})
target_link_libraries(alice-server angel leveldb snappy)

add_executable(alice-cli ${CLIENT_SRC})
target_link_libraries(alice-cli angel linenoise)

add_executable(alice-proxy ${PROXY_SRC})
target_link_libraries(alice-proxy angel)

add_library(alice-api STATIC ${CLIAPI_SRC})
target_link_libraries(alice-api angel)
