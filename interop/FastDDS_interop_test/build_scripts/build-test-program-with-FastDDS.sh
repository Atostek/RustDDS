#!/bin/bash

if [[ $EUID -ne 0 ]]; then
    echo "Please run using sudo."
    exit 2
fi

./build-FastDDS.sh $1
./build-test-program.sh