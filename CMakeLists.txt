cmake_minimum_required(VERSION 3.10)

project(comparer)

if(MSVC AND CMAKE_VERSION VERSION_GREATER_EQUAL 3.21)
    set(CMAKE_CXX_STANDARD 23) # /std:c++latest
else()
    set(CMAKE_CXX_STANDARD 20)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

set(SRC_FILES
    src/comparer.cpp)

add_executable(comparer ${SRC_FILES})

if(WIN32)
    add_definitions(-D_WIN32_WINNT=0x0602 -DNOMINMAX)
endif()

find_package(tdscpp REQUIRED)
find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

target_link_libraries(comparer tdscpp)
target_link_libraries(comparer fmt::fmt-header-only)
target_link_libraries(comparer Threads::Threads)

if(NOT MSVC)
    target_compile_options(comparer PUBLIC -Wall -Werror=cast-function-type -Wno-expansion-to-defined -Wunused-parameter -Wtype-limits -Wextra -Wconversion -Wnoexcept)

    target_compile_options(comparer PUBLIC -fdata-sections -ffunction-sections)
    target_compile_options(comparer PUBLIC -g0)
    target_link_options(comparer PUBLIC -Wl,--gc-sections)

    #target_compile_options(comparer PUBLIC -gcodeview)
    #target_link_options(comparer PUBLIC -gcodeview)
else()
	add_definitions(-D_CRT_SECURE_NO_WARNINGS)

    target_link_options(comparer PUBLIC /MANIFEST:NO)
endif()
