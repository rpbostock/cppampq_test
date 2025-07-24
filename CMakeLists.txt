cmake_minimum_required(VERSION 3.31)
project(cppampq_test)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include the module path for FindEvent.cmake
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

include(gtest.cmake)
include(amqpcpp.cmake)

find_package(PkgConfig REQUIRED)
find_package(Boost 1.74.0 EXACT REQUIRED COMPONENTS log log_setup)
pkg_check_modules(LIBEVENT REQUIRED libevent)

find_package(Event REQUIRED)
if(NOT Event_FOUND)
  message(FATAL_ERROR "libevent not found. Please install libevent-dev")
endif()

# Add the executable
add_executable(cppampq_test
  src/logging.cpp
  test/main.cpp
  test/test_amqp.cpp
  test/test_reliable_message_manager.cpp
)


# Set include directories
target_include_directories(cppampq_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${Event_INCLUDE_DIR}
)

# Link libraries
target_link_libraries(cppampq_test PRIVATE
  amqpcpp
  ${Event_LIBRARIES}
  ${Boost_LIBRARIES}
  gtest
  gtest_main
  pthread
)
