cmake_minimum_required(VERSION 3.20)
project(chronos_cpp_client LANGUAGES CXX)

include(GNUInstallDirs)

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
set_target_properties(chronos_proto PROPERTIES EXPORT_NAME proto)
target_include_directories(
  chronos_proto
  PUBLIC
    "$<BUILD_INTERFACE:${GENERATED_DIR}>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/chronos>"
)
target_link_libraries(chronos_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_library(chronos_client_lib "client.cc")
add_library(chronos::client ALIAS chronos_client_lib)
set_target_properties(chronos_client_lib PROPERTIES OUTPUT_NAME chronos_client EXPORT_NAME client)
target_include_directories(
  chronos_client_lib
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/chronos>"
)
target_link_libraries(chronos_client_lib PUBLIC chronos_proto)

add_executable(client_test "client_test.cc")
target_include_directories(client_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" "${GENERATED_DIR}")
target_link_libraries(client_test PRIVATE chronos_client_lib)

add_executable(client_example "../../examples/cpp/client_example.cc")
target_include_directories(client_example PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}" "${GENERATED_DIR}")
target_link_libraries(client_example PRIVATE chronos_client_lib)

add_test(NAME client_test COMMAND client_test)

install(
  TARGETS chronos_client_lib chronos_proto
  EXPORT chronosClientTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)
install(
  FILES "client.h" "${PROTO_HDR}" "${GRPC_HDR}"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/chronos"
)
install(
  EXPORT chronosClientTargets
  NAMESPACE chronos::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/chronos"
)
