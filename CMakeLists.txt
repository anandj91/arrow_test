cmake_minimum_required(VERSION 3.13.4)

project(ArrowTest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/src/)

add_executable(main main.cpp)
target_link_libraries(main PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/build/debug/libarrow.a
    ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/build/debug/libarrow_bundled_dependencies.a
)
