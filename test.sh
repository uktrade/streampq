#!/bin/bash -e

# So we have coverage for sub-processes
SITE_PACKAGES_DIR=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
echo "import coverage; coverage.process_startup()" > "${SITE_PACKAGES_DIR}/coverage.pth"
export COVERAGE_PROCESS_START=.coveragerc
gcc -shared -fPIC test_override_malloc.c -o test_override_malloc.so -ldl
LD_PRELOAD=$PWD/test_override_malloc.so python3 -m pytest -v "$@"