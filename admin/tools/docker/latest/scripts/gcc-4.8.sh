#!/bin/bash

# Install and set gcc-4.8 as default compiler 

# @author  Fabrice Jammes, IN2P3/SLAC

apt-get install gcc-4.8 g++-4.8
update-alternatives --remove-all gcc
update-alternatives --remove-all g++

update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 20 --slave /usr/bin/g++ g++ /usr/bin/g++-4.8
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.9 10 --slave /usr/bin/g++ g++ /usr/bin/g++-4.9

update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 30
update-alternatives --set cc /usr/bin/gcc

update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++ 30
update-alternatives --set c++ /usr/bin/g++
