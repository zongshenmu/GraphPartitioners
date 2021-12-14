cmake_minimum_required(VERSION 3.17)
project(GraphPartitioning)

add_library(SOURCE_FILES
        converter/conversions.cpp
        converter/shuffler.cpp
        ne_partitioner/graph.cpp
        ne_partitioner/ne.cpp
        dbh.cpp
        hdrf.cpp
        streaming_vp/ldg.cpp
        streaming_vp/fennel.cpp)
add_executable(GraphPartitioning main.cpp)
target_link_libraries (GraphPartitioning glog SOURCE_FILES)