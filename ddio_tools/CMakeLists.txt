cmake_minimum_required(VERSION 3.2)

ADD_DEFINITIONS( -std=c++17)

set(ggflags_DIR "../third_party/gflags")
add_subdirectory(${ggflags_DIR} gflags)
include_directories("${ggflags_DIR}/include")

project(dca)

include_directories(../third_party)

add_executable(setup_dca dca.cc )

target_link_libraries(setup_dca  ${CMAKE_SOURCE_DIR}/pciutils-3.5.1/lib/libpci.a udev z gflags)

add_executable(ddio_tune ddio_tune.cc ../third_party/r2/src/logging.cc)

target_link_libraries(ddio_tune  ${CMAKE_SOURCE_DIR}/pciutils-3.5.1/lib/libpci.a udev z gflags)
