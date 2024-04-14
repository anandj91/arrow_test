cmake_minimum_required(VERSION 3.13.4)

project(ArrowTest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
add_subdirectory(third_party/flatbuffers
        ${CMAKE_CURRENT_BINARY_DIR}/flatbuffers-build
        EXCLUDE_FROM_ALL)

add_executable(main main.cpp)
target_link_libraries(main PRIVATE flatbuffers)
