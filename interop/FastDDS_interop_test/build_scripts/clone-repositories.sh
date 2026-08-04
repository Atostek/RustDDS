#!/bin/bash

# based on https://fast-dds.docs.eprosima.com/en/latest/installation/sources/sources_linux.html

echo Create ~/Fast-DDS
mkdir ~/Fast-DDS
cd ~/Fast-DDS

echo Clone foonathan memory
git clone https://github.com/eProsima/foonathan_memory_vendor.git
mkdir foonathan_memory_vendor/build

echo Clone Fast-CDR
git clone https://github.com/eProsima/Fast-CDR.git
mkdir Fast-CDR/build

echo Clone Fast-DDS
if [ $1="m" ]; then
    echo "Using the modified version."
    git clone https://github.com/ohuopio/Fast-DDS-with-security-interoperability-modifications.git
    mkdir Fast-DDS-with-security-interoperability-modifications/build
elif [ $1="u" ]; then
    echo "Using the unmodified version."
    git clone https://github.com/eProsima/Fast-DDS.git
    mkdir Fast-DDS/build
else 
    echo "Provide argument m or u to choose the modified or unmodified FastDDS repository."
    exit 1
fi
