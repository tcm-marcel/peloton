# - Try to find Libffi
#
#  A Portable Foreign Function Interface Library (https://sourceware.org/libffi)
#
# Usage:
# LIBFFI_INCLUDE_DIRS, location of header files
# LIBFFI_LIBRARIES, location of library
# LIBFFI_FOUND, indicates if libffi was found

# Look for the header file.
execute_process(COMMAND brew --prefix libffi OUTPUT_VARIABLE LIBFFI_BREW_PREFIX)
find_path(LIBFFI_INCLUDE_DIRS NAMES ffi.h HINT LIBFFI_BREW_PREFIX)

# DEBUG
find_path(STATUS "DEBUG LIBFFI_BREW_PREFIX: ${LIBFFI_BREW_PREFIX}")
execute_process(COMMAND ls ${LIBFFI_BREW_PREFIX} OUTPUT_VARIABLE LIBFFI_BREW_LS)
find_path(STATUS "DEBUG LIBFFI_BREW_LS: ${LIBFFI_BREW_LS}")

# Look for the library.
find_library(LIBFFI_LIBRARIES NAMES libffi.so)

# Handle the QUIETLY and REQUIRED arguments and set SQLITE3_FOUND to TRUE if all listed variables are TRUE.
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LIBFFI DEFAULT_MSG LIBFFI_LIBRARIES LIBFFI_INCLUDE_DIRS)

message(STATUS "Found Libffi (include: ${LIBFFI_INCLUDE_DIRS}, library: ${LIBFFI_LIBRARIES})")