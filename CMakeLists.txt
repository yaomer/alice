cmake_minimum_required (VERSION 2.8)

project (Alice)

set (CMAKE_CXX_FLAGS "-std=c++17 -Wall -O2")

set (SERVER_DIR "${PROJECT_SOURCE_DIR}/src")
set (CLIENT_DIR "${PROJECT_SOURCE_DIR}/client")
set (PROXY_DIR "${PROJECT_SOURCE_DIR}/proxy")

set (SERVER_FILES
    ${SERVER_DIR}/server.cc
    ${SERVER_DIR}/db.cc
    ${SERVER_DIR}/db_string.cc
    ${SERVER_DIR}/db_list.cc
    ${SERVER_DIR}/db_hash.cc
    ${SERVER_DIR}/db_set.cc
    ${SERVER_DIR}/db_zset.cc
    ${SERVER_DIR}/db_sort.cc
    ${SERVER_DIR}/evict.cc
    ${SERVER_DIR}/rdb.cc
    ${SERVER_DIR}/aof.cc
    ${SERVER_DIR}/sentinel.cc
    ${SERVER_DIR}/config.cc
    ${SERVER_DIR}/util.cc
)

set (CLI_CLIENT_FILES
    ${SERVER_DIR}/client.cc
    ${SERVER_DIR}/hint.cc
    ${SERVER_DIR}/util.cc
)

set (PROXY_FILES
    ${PROXY_DIR}/proxy.cc
    ${SERVER_DIR}/util.cc
)

set (CLIENT_FILES
    ${CLIENT_DIR}/client.cc
    ${SERVER_DIR}/util.cc
)

add_library(angel STATIC IMPORTED)
set_target_properties(angel PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libangel.a")

add_library(linenoise STATIC IMPORTED)
set_target_properties(linenoise PROPERTIES IMPORTED_LOCATION "/usr/local/lib/liblinenoise.a")

add_executable(alice-server ${SERVER_FILES})
target_link_libraries(alice-server angel)

add_executable(alice-cli ${CLI_CLIENT_FILES})
target_link_libraries(alice-cli angel linenoise)

add_executable(alice-proxy ${PROXY_FILES})
target_link_libraries(alice-proxy angel)

add_library(alice-api STATIC ${CLIENT_FILES})
target_link_libraries(alice-api angel)
