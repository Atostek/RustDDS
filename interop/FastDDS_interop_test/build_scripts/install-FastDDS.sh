#!/bin/bash

# based on https://fast-dds.docs.eprosima.com/en/latest/installation/sources/sources_linux.html

if [ $EUID -ne 0 ]; then
    echo "Please run using sudo."
    exit 2
fi

sudo -u $SUDO_USER ./clone-repositories.sh $1
./build-dependencies.sh
./build-FastDDS.sh $1
