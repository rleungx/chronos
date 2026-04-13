cmake_minimum_required(VERSION 3.20)
project(chronos_cpp_client LANGUAGES CXX)

enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(gRPC REQUIRED)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/../../tso.proto")
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${GENERATED_DIR}")

get_filename_component(PROTO_DIR "${PROTO_FILE}" DIRECTORY)

set(PROTO_SRC "${GENERATED_DIR}/tso.pb.cc")
set(PROTO_HDR "${GENERATED_DIR}/tso.pb.h")
set(GRPC_SRC "${GENERATED_DIR}/tso.grpc.pb.cc")
set(GRPC_HDR "${GENERATED_DIR}/tso.grpc.pb.h")

add_custom_command(
  OUTPUT "${PROTO_SRC}" "${PROTO_HDR}" "${GRPC_SRC}" "${GRPC_HDR}"
  COMMAND protobuf::protoc
  ARGS
    --proto_path="${PROTO_DIR}"
    --cpp_out="${GENERATED_DIR}"
    --grpc_out="${GENERATED_DIR}"
    --plugin=protoc-gen-grpc="$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    "${PROTO_FILE}"
  DEPENDS "${PROTO_FILE}"
)

add_library(chronos_proto "${PROTO_SRC}" "${GRPC_SRC}")
target_include_directories(chronos_proto PUBLIC "${GENERATED_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(chronos_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_library(chronos_client_lib "client.cc")
target_include_directories(chronos_client_lib PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(chronos_client_lib PUBLIC chronos_proto)

add_executable(client_test "client_test.cc")
target_include_directories(client_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" "${GENERATED_DIR}")
target_link_libraries(client_test PRIVATE chronos_client_lib)

add_test(NAME client_test COMMAND client_test)
