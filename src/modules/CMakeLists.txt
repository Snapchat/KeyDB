get_filename_component(base_name "${CMAKE_CURRENT_SOURCE_DIR}" NAME)

foreach (module
        "helloacl"
        "helloblock"
        "hellocluster"
        "hellodict"
        "hellohook"
        "hellotimer"
        "hellotype"
        "helloworld")
    set(LIBRARY_NAME "${PROJECT_NAME}_${base_name}")
    source_group("${LIBRARY_NAME} Source Files" FILES "${module}.c")

    add_library("${LIBRARY_NAME}" "${Source_Files}")

    target_link_libraries("${LIBRARY_NAME}" PRIVATE "${PROJECT_NAME}_compiler_flags")
    list(APPEND installable_libs "${LIBRARY_NAME}")
endforeach ()
