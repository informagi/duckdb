add_library_unity(test_sql_varchar OBJECT test_big_varchar.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_varchar>
    PARENT_SCOPE)
