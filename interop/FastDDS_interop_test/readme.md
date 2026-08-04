A simple test program for testing compatibility with FastDDS. The program is expected to be able to interact with shapes demo.

First, install the modified FastDDS by going to the `build_scripts` directory and running 
```sudo ./install-FastDDS.sh m```.
To use the unmodified version, use the parameter `u` instead of `m` in this and subsequent calls.

The library path can be added to bash configuration using
```echo 'export LD_LIBRARY_PATH=/usr/local/lib/' >> ~/.bashrc```.

To build, go to the build directory and run
```cmake ..; make```, or use ```./build-test-program.sh```. If you want to include new modifications to FastDDS, run ```sudo ./build-test-program-with-FastDDS.sh m``` instead.

To start a secure publisher or subscriber, go to the build directory and run `./secure_shapes p` or `./secure_shapes s` respectively. To test the unsecure case, use the values `up` or `us`.