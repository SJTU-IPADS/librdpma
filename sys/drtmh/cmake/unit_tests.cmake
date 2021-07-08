add_executable(logger src/rtx/logger_test.cxx)
target_link_libraries(logger gtest_main)
target_link_libraries(logger
      ssmalloc
      boost_coroutine boost_chrono boost_thread boost_context boost_system )
add_test(NAME logger_test COMMAND logger)    

add_executable(op src/rtx/op_test.cxx)
target_link_libraries(op gtest_main)
target_link_libraries(op
      ssmalloc
      boost_coroutine boost_chrono boost_thread boost_context boost_system )
add_test(NAME operator_test COMMAND op)    
