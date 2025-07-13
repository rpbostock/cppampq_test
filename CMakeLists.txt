cmake_minimum_required(VERSION 3.31)
project(cppampq_test)

set(CMAKE_CXX_STANDARD 20)

include(amqpcpp.cmake)
include(FindEvent.cmake)

add_executable(cppampq_test main.cpp)

target_link_libraries(cppampq_test PUBLIC
  ${Event_LIBRARIES} amqpcpp pthread)