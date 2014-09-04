#!/bin/sh

set -e
set -x

#############################
# CUSTOMIZE NEXT PARAMETERS :
#############################

INTERNET_FREE_DISTSERVER_DIR=shared/dir/available/to/all/nodes

# MODIFY PARAMETERS BELOW ONLY IF YOU KNOW WHAT YOU'RE DOING
DISTSERVER_ARCHIVE_URL=http://lsst-web.ncsa.illinois.edu/~fjammes/qserv-internet-free/qserv-internet-free-distserver.tar.gz
TARBALL_DIR=${HOME}/tarballs

#############################


# OPTIONAL : cleaning of previous distribution server
mkdir -p ${TARBALL_DIR} 
mkdir -p ${INTERNET_FREE_DISTSERVER_DIR} 
rm -f ${TARBALL_DIR}/qserv-internet-free-distserver.tar.gz
rm -rf ${INTERNET_FREE_DISTSERVER_DIR}/* 

# prepare internet-free distribution server
cd ${TARBALL_DIR}
curl -O ${DISTSERVER_ARCHIVE_URL}
cd ${INTERNET_FREE_DISTSERVER_DIR} 
tar zxvf ~/tarballs/qserv-internet-free-distserver.tar.gz --strip-components=1

# OPTIONAL : python 2.7 is required, if it isn't available on you system, and
# if you can't install system packages, Anaconda may be a solution :
# curl -O http://repo.continuum.io/archive/Anaconda-1.8.0-Linux-x86_64.sh
