CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(LSMTree)

set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_STANDARD 14)

INCLUDE_DIRECTORIES(src/include)

SET(CMAKE_CXX_FLAGS "-std=c++14 -Wall")

ADD_EXECUTABLE(correctness src/correctness.cc src/kvstore.cc src/sstable/sstable.cpp src/memtable/memtable.cpp)

ADD_EXECUTABLE(persistence src/persistence.cc src/kvstore.cc)

ADD_EXECUTABLE(test src/test.cc src/kvstore.cc src/sstable/sstable.cpp src/memtable/memtable.cpp)
