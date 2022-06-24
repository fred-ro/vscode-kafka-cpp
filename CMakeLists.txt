project(producer)
add_executable(producer producer.cpp)
target_link_libraries(producer PUBLIC rdkafka++)
target_link_libraries(producer PUBLIC rdkafka)