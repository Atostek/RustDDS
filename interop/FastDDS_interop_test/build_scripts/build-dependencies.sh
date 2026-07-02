#!/bin/bash

# based on https://fast-dds.docs.eprosima.com/en/latest/installation/sources/sources_linux.html

if [ $EUID -ne 0 ]; then
    echo "Please run using sudo."
    exit 2
fi

cd /home/$SUDO_USER/Fast-DDS

cd foonathan_memory_vendor/build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local/ -DBUILD_SHARED_LIBS=ON
cmake --build . --target install

cd ../../Fast-CDR/build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local/ -DBUILD_SHARED_LIBS=ON
cmake --build . --target install
