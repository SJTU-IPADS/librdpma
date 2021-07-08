cmake_minimum_required(VERSION 3.2)

set(ggflags_DIR "${CMAKE_SOURCE_DIR}/third_party/gflags")
add_subdirectory(${ggflags_DIR})
include_directories("${ggflags_DIR}/include")

project(rocc)

option( LINK_STATIC_LIB "Link static version of libssmalloc and libboost" true)

set(ROCC_ROOT "." )
#set(CMAKE_BUILD_TYPE Debug)

ADD_DEFINITIONS( -std=c++14)

#set(CMAKE_CXX_COMPILER g++ - 4.8)
include_directories(./src)
include_directories(.)

## third party libraries
include_directories(third_party/sparsehash-c11)
include_directories(third_party)

#with benchmark profiling
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE -DBENCH_PROFILE")

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/rocc.cmake)
load_global_config()
load_tx_config()


## seperate install
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

## without benchmark profiling
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE ")##ndebug will optimze out assert
set (MACRO_FLAGS "-DBASE_LINE")

## currently drtm in this codebase is not supported, i will fix this later 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread -O2 -g ${MACRO_FLAGS} -msse3 -mavx -mavx512f")

## TODO, we shall replace it with a pre-complied lib, but since now the lib is not stable, so we just add sources here
file(GLOB RDMA_SOURCES "third_party/libRDMA/src/*.cc" "third_party/micautil/*.cc")

## add main files
file(GLOB SOURCES
          "src/app/micro_benches/*.cc" "src/app/tpcc/*.cc"  "src/app/smallbank/*.cc"             # apps
          "src/app/tpce/*.cc"                                                                    # apps continued
          "src/app/graph/*.cc"                                                                   # apps continued 
          "src/framework/*.cc"  "src/framework/utils/*.cc"                                       # framework
          "src/memstore/*.cc"                                                                    # memstore
          "src/core/*.cc"                                                                        # nocc core
          "src/db/*.cc" "src/db/txs/*.cc"                                                        # framework + CC
          "src/util/*.cc" "src/util/micautil/*.cc" "src/port/*.cc" "src/oltp/utils/*.cc"         # utils
          "src/rtx/*.cc"       # new rtx's code
          )

include(cmake/tpce.cmake)
add_executable(noccocc ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccocc PRIVATE "-DOCC_TX")

add_executable(nvm_hello "./third_party/r2/src/logging.cc" ${RDMA_SOURCES}  ./nvm/benchs/local.cc)
target_compile_options(nvm_hello PRIVATE "-DRAD_TX")

#add_executable(nvm_rdma_hello "./src/core/logging.cc" ${RDMA_SOURCES}  ./nvm/hello_rdma.cc)
add_executable(nvm_rdma_hello "./src/core/logging.cc" ${RDMA_SOURCES}  ./nvm/hello_world.cc)
target_compile_options(nvm_rdma_hello PRIVATE "-DRAD_TX")

add_executable(nvm_aclient ./nvm/benchs/one_sided/aclient.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_client ./nvm/benchs/one_sided/client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_server ./nvm/benchs/one_sided/server.cc)
add_executable(nvm_bandwidth ./nvm/benchs/one_sided/bandwidth.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)

add_executable(nvm_tserver ./nvm/benchs/two_sided/server.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_tclient ./nvm/benchs/two_sided/client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)

add_executable(nvm_rtserver ./nvm/benchs/two_sided/rc_server.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_rtclient ./nvm/benchs/two_sided/rc_client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)

add_executable(nvm_rrtserver ./nvm/benchs/two_sided/ring_server.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_rrtclient ./nvm/benchs/two_sided/ring_client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)


add_executable(noccsi ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccsi PRIVATE "-DSI_TX")

add_executable(noccfarm ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccfarm PRIVATE "-DFARM")

add_executable(noccrad ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(noccrad PRIVATE "-DRAD_TX")

## Micro with one-sided RDMA
add_executable(nn ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(nn PRIVATE "-DRPC=0" "-DOCC_TX")

#Micro with RDMA RPC
add_executable(nr ${SOURCES} ${TPCE_SOURCES} ${RDMA_SOURCES})
target_compile_options(nr PRIVATE "-DRPC=1" "-DOCC_TX")

## Install static dependencies
include(cmake/Dependencies.cmake)

#
#Configure binary linking
#
set(apps noccrad noccocc noccsi noccfarm nn nr nvm_hello nvm_rdma_hello
     nvm_client nvm_server  nvm_bandwidth
     nvm_aclient 
     nvm_tserver nvm_tclient 
     nvm_rtserver nvm_rtclient 
     nvm_rrtserver nvm_rrtclient )

foreach( prog ${apps} )
  if( LINK_STATIC_LIB )
    target_link_libraries( ${prog}
      ${LIBZMQ} rt ${LIBIBVERBS}
      ssmalloc numa
      boost_coroutine boost_chrono boost_thread boost_context boost_system cpuinfo gflags)
  else()
    target_link_libraries( ${prog}
      ${LIBZMQ} rt ${LIBIBVERBS}
      ssmalloc
      boost_coroutine boost_system )
  endif()
  add_dependencies( ${prog} ralloc libboost1.61 )
  add_custom_command(TARGET ${prog}
    POST_BUILD
    COMMAND mv ${prog} ${CMAKE_SOURCE_DIR}/scripts
    )
endforeach( prog )

#for the ease of running
set(CMAKE_INSTALL_PREFIX ./)
install(TARGETS noccocc  DESTINATION scripts) 
install(TARGETS noccfarm DESTINATION scripts)
install(TARGETS noccsi   DESTINATION scripts)

## for tests
if(GTEST)
    include(cmake/gtest.cmake)
    enable_testing()
    include(cmake/unit_tests.cmake)
endif()
