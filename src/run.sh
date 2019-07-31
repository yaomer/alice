#!/bin/bash

PLATFORM=$(uname -s)
CXXFLAGS="-std=c++17 -Wall -Wno-unused-private-field -llinenoise"

if [ "$PLATFORM" == "Darwin" ]; then
    CXX=clang++
elif [ "$PLATFORM" == "Linux" ]; then
    CXX=g++
fi

$CXX $CXXFLAGS server.cc db.cc rdb.cc aof.cc config.cc -langel -o serv
$CXX $CXXFLAGS client.cc hint.cc -langel -o cli
