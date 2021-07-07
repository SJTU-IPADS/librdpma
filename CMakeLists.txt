cmake_minimum_required(VERSION 3.2)

set(ggflags_DIR "${CMAKE_SOURCE_DIR}/third_party/gflags")
add_subdirectory(${ggflags_DIR})
include_directories("${ggflags_DIR}/include")

project(rocc)

option( LINK_STATIC_LIB "Link static version of libssmalloc and libboost" true)

set(ROCC_ROOT "." )
#set(CMAKE_BUILD_TYPE Debug)

ADD_DEFINITIONS( -std=c++17)

#set(CMAKE_CXX_COMPILER g++ - 4.8)
include_directories(./src)
include_directories(.)

## third party libraries
#include_directories(third_party/sparsehash-c11)
include_directories(third_party)



## seperate install
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

## without benchmark profiling
#set(MACRO_FLAGS "-DNDEBUG -DBASE_LINE ")##ndebug will optimze out assert
set (MACRO_FLAGS "-DBASE_LINE")

## currently drtm in this codebase is not supported, i will fix this later 
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DLEVELDB_PLATFORM_POSIX -pthread -DOS_LINUX -mrtm -pthread -O2 -g ${MACRO_FLAGS} -msse3 -mavx -mavx512f")



add_executable(nvm_aclient ./nvm/benchs/one_sided/aclient.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_client ./nvm/benchs/one_sided/client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_server ./nvm/benchs/one_sided/server.cc)

add_executable(nvm_tserver ./nvm/benchs/two_sided/server.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_tclient ./nvm/benchs/two_sided/client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)

add_executable(nvm_rtserver ./nvm/benchs/two_sided/rc_server.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_rtclient ./nvm/benchs/two_sided/rc_client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)

add_executable(nvm_rrtserver ./nvm/benchs/two_sided/ring_server.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)
add_executable(nvm_rrtclient ./nvm/benchs/two_sided/ring_client.cc ./third_party/r2/src/sshed.cc ./third_party/r2/src/logging.cc)



## Install static dependencies
include(cmake/Dependencies.cmake)

#
#Configure binary linking
#
set(apps 
     nvm_client nvm_server 
     nvm_aclient 
     nvm_tserver nvm_tclient 
     nvm_rtserver nvm_rtclient 
     nvm_rrtserver nvm_rrtclient )

foreach(prog ${apps} )
  if( LINK_STATIC_LIB )
    target_link_libraries( ${prog}
      ${LIBZMQ} rt ${LIBIBVERBS}
      numa
      boost_coroutine boost_chrono boost_thread boost_context boost_system gflags)
  else()
    target_link_libraries( ${prog}
      ${LIBZMQ} rt ${LIBIBVERBS}
      ssmalloc
      boost_coroutine boost_system )
  endif()
  add_dependencies( ${prog} libboost1.61 )
  add_custom_command(TARGET ${prog}
    POST_BUILD
    COMMAND mv ${prog} ${CMAKE_SOURCE_DIR}/scripts
    )
endforeach( prog )
