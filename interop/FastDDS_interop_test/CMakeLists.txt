cmake_minimum_required(VERSION 3.22)

project("secure_shapes")

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(fastcdr 2 REQUIRED)
find_package(fastrtps 2.12 REQUIRED)

add_library(shape_type Shape.cxx)
target_link_libraries(shape_type fastcdr fastrtps)

add_executable(secure_shapes ShapePubSubTypes.cxx ShapePublisher.cpp ShapeSubscriber.cpp main.cpp)
target_link_libraries(secure_shapes fastcdr fastrtps shape_type)