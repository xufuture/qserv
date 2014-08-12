#!/bin/bash -x

# Standard LSST install procedure

STACK_DIR=$HOME/stack
#REF=master-ga1c1526733

######################
#
# WITH NETWORK ACCESS
#
######################
# TODO remove and manage below
#NEWINSTALL_URL=http://sw.lsstcorp.org/eupspkg/newinstall.sh
#EUPS_PKGROOT_QSERV=http://lsst-web.ncsa.illinois.edu/~fjammes/qserv

########################
#
# WITHOUT NETWORK ACCESS
#
########################
EUPS_PKGROOT=${EUPS_PKGROOT:-"http://sw.lsstcorp.org/eupspkg"}
# TODO if newinstall doesn't start with protocol:// add file://
NEWINSTALL_URL="file://${EUPS_PKGROOT}/newinstall.sh"
EUPS_PKGROOT_QSERV=${EUPS_PKGROOT_QSERV:-"http://lsst-web.ncsa.illinois.edu/~fjammes/qserv"}

if [ -d ${STACK_DIR} ]; then
    chmod -R 755 $STACK_DIR &&
    rm -rf $STACK_DIR ||
    {
        echo "Unable to remove install directory previous content : ${STACK_DIR}"
        exit 1
    }
    
fi
mkdir $STACK_DIR &&
cd $STACK_DIR ||
{
    echo "Unable to go to install directory : ${STACK_DIR}"
    exit 1
}
echo
echo "Installing eups"
echo "==============="
echo
curl -O ${NEWINSTALL_URL} ||
{
    echo "Unable to download from ${NEWINSTALL_URL}"
    exit 1
}
time bash newinstall.sh
source loadLSST.sh

echo
echo "Installing Qserv"
echo "================"
echo
time eups distrib install qserv ${REF} -r ${EUPS_PKGROOT_QSERV} &&
setup qserv ||
{
    echo "Unable to install Qserv"
    exit 1
}
echo
echo "Installing Qserv integration tests datasets"
echo "==========================================="
echo
time eups distrib install qserv_testdata -r ${EUPS_PKGROOT_QSERV} &&
setup qserv_testdata ||
{
    echo "Unable to install Qserv test datasets"
    exit 1
}

echo
echo "Configuring Qserv"
echo "================="
echo
cd $QSERV_DIR/admin &&
qserv-configure --all ||
{
    echo "Unable to configure Qserv as a mono-node instance"
    exit 1
}

echo
echo "Starting Qserv"
echo "=============="
echo
qserv-start.sh ||
{
    echo "Unable to start Qserv"
    exit 1
}

echo
echo "Running Qserv integration tests"
echo "==============================="
echo
qserv-test-integration.py ||
{
    echo "Integration tests failed"
    exit 1
}

echo
echo "Stopping Qserv"
echo "=============="
echo
qserv-stop.sh ||
{
    echo "Unable to stop Qserv"
    exit 1
}

