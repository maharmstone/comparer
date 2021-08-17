cmake_minimum_required(VERSION 3.10)

project(comparer)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

set(SRC_FILES
    src/comparer.cpp)

add_executable(comparer ${SRC_FILES})

add_definitions(-D_WIN32_WINNT=0x0602 -DNOMINMAX)

find_package(tdscpp REQUIRED)

target_link_libraries(comparer tdscpp)

if(NOT MSVC)
    target_compile_options(comparer PUBLIC -Wall -Werror=cast-function-type -Wno-expansion-to-defined -Wunused-parameter -Wtype-limits -Wextra -Wconversion -Wnoexcept)

    target_compile_options(comparer PUBLIC -fdata-sections -ffunction-sections)
    target_compile_options(comparer PUBLIC -g0)
    target_link_options(comparer PUBLIC -Wl,--gc-sections)

    target_compile_options(comparer PUBLIC -gcodeview)
    target_link_options(comparer PUBLIC -gcodeview)
else()
    target_link_options(comparer PUBLIC /MANIFEST:NO)
endif()
