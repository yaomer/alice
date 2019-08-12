cmake_minimum_required (VERSION 2.8)

project (Angel)

set (CMAKE_CXX_FLAGS "-std=c++17 -Wall -O2")

set (PROJECT_SRC_DIR "${PROJECT_SOURCE_DIR}/src")

set (SERV_SRC_FILES
    ${PROJECT_SRC_DIR}/server.cc
    ${PROJECT_SRC_DIR}/db.cc
    ${PROJECT_SRC_DIR}/rdb.cc
    ${PROJECT_SRC_DIR}/aof.cc
    ${PROJECT_SRC_DIR}/config.cc
)

set (CLI_SRC_FILES
    ${PROJECT_SRC_DIR}/client.cc
    ${PROJECT_SRC_DIR}/hint.cc
)

add_library(angel STATIC IMPORTED)
set_target_properties(angel PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libangel.a")

add_library(linenoise STATIC IMPORTED)
set_target_properties(linenoise PROPERTIES IMPORTED_LOCATION "/usr/local/lib/liblinenoise.a")

add_executable(alice-server ${SERV_SRC_FILES})
target_link_libraries(alice-server angel)

add_executable(alice-cli ${CLI_SRC_FILES})
target_link_libraries(alice-cli angel linenoise)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})
