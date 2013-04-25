#!/bin/bash

yum install scons

# Scientific Linux 6 dependencies
# data partitioning dependency
yum install numpy;

# Twisted
yum install python-twisted python-twisted-names python-twisted-runner python-twisted-web 

# xrootd
yum install gcc-c++ git zlib-devel

# zope_interface
yum install python-devel

# mysql
yum install ncurses-devel MySQL-python

# qserv
yum install boost-devel openssl-devel antlr swig

# lua
yum install readline-devel

# mysql-proxy
yum install glib2-devel

