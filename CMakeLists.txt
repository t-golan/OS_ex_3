cmake_minimum_required(VERSION 3.21)
project(Resources_20220426)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

include_directories(Atomic Barrier Sample Client)

add_executable(Resources_20220426
        MapReduceFramework.h
        MapReduceFramework.cpp
        MapReduceClient.h
        Sample\ Client/SampleClient.cpp
        Atomic/atomicdemo.cpp Atomic/atomicdemo_multivar.cpp
        Barrier/barrierdemo.cpp Barrier/Barrier.cpp )
