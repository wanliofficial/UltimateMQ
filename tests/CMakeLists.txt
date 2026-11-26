
# Set the test executable name
set(TEST_EXECUTABLE_NAME flowmq_tests)

# Add test source files
set(TEST_SOURCES
    MQTTTests.cpp
    TrieTests.cpp
    TopicTests.cpp
    BrokerTests.cpp
    FrameTests.cpp
    FrameStreamTests.cpp
    SessionLifecycleTests.cpp
    ConnectionStateTests.cpp
    AuthTests.cpp
    StoreTests.cpp
    TLSTests.cpp
    SysTopicsTests.cpp
    Utf8Tests.cpp
    MQTTSNTests.cpp
    ServerTests.cpp
    WebSocketTests.cpp
    AMQPTests.cpp
)

# Create the test executable
add_executable(${TEST_EXECUTABLE_NAME} ${TEST_SOURCES})

# Link the test executable with Google Test and your project's library
target_link_libraries(${TEST_EXECUTABLE_NAME}   
    PRIVATE
    gtest
    gtest_main
    flowmq_lib  # Link against the library, not the executable
)

# Add the test to CTest
add_test(NAME ${TEST_EXECUTABLE_NAME} COMMAND ${TEST_EXECUTABLE_NAME})

# Add custom target for running tests
add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS ${TEST_EXECUTABLE_NAME}
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running tests..."
)
