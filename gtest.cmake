cmake_minimum_required(VERSION 3.11 FATAL_ERROR)
include(FetchContent)

message(STATUS "GoogleTest.. Starting")

FetchContent_Declare(
        googletest
        GIT_REPOSITORY "https://github.com/google/googletest.git"
        GIT_TAG "v1.14.0"
        SOURCE_DIR  ${CMAKE_BINARY_DIR}/external/gtest
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
set(BUILD_GTEST ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(googletest)

FetchContent_GetProperties(googletest)
if(NOT googletest_POPULATED)
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif()

message(STATUS "GoogleTest.. Finished")