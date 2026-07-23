cmake_minimum_required(VERSION 3.20)
project(chronos_cpp_package_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CHRONOS_CLIENT_VERSION "0.1.0" CACHE STRING "Chronos client version under test")

find_package(chronos "${CHRONOS_CLIENT_VERSION}" CONFIG REQUIRED)

add_executable(package_consumer main.cc)
target_link_libraries(package_consumer PRIVATE chronos::client)
