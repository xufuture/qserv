#!/bin/sh

PKGROOT=/data/fjammes/distserver
REMOTE_HOST=lsst-dev.ncsa.illinois.edu

EUPS_VERSION=${EUPS_VERSION:-1.5.0}
EUPS_TARBALL="$EUPS_VERSION.tar.gz"
EUPS_TARURL="https://github.com/RobertLuptonTheGood/eups/archive/$EUPS_TARBALL"

EUPS_GITREPO=$"https://github.com/RobertLuptonTheGood/eups.git"

if [ ! -d ${PKGROOT} ]; then
    mkdir ${PKGROOT} ||
    {
        echo "Unable to create local distribution directory : ${PKGROOT}"
        exit 1
    }

fi
cd ${PKGROOT} ||
{
    echo "Unable to go to local distribution directory directory : ${PKGROOT}"
    exit 1
}

echo
echo "Downloading eups tarball"
echo "========================"
echo
curl -L ${EUPS_TARURL} > ${EUPS_TARBALL}

echo
echo "Downloading distribution server data on ${REMOTE_HOST}"
echo "======================================================"
echo
EUPS_PKGROOT_QSERV="${PKGROOT}/eupspkg_root_qserv"
rsync -ave ssh ${REMOTE_HOST}:/lsst/home/fjammes/public_html/qserv/ ${EUPS_PKGROOT_QSERV} ||
{
    echo "Unable to synchronize with Qserv distribution server on ${REMOTE_HOST}"
    exit 1
}

echo
echo "Downloading git repositories for Qserv and dependencies on ${REMOTE_HOST}"
echo "========================================================================="
echo
GIT_REPOS="${PKGROOT}/git-repositories"
rsync -ave ssh ${REMOTE_HOST}:/lsst/home/fjammes/src/lsstsw-release/build/${GIT_REPOS} ||
{
    echo "Unable to synchronize with git repositories on ${REMOTE_HOST}"
    exit 1
}

EUPS_TARURL="file://${PKGROOT}/$EUPS_TARBALL"
EUPS_GITREPO="${PKGROOT}/eups.git"

echo "export EUPS_PKGROOT=${EUPS_PKGROOT}"
echo "export EUPS_PKGROOT_QSERV=${EUPS_PKGROOT_QSERV}"
echo "export EUPS_TARURL=${EUPS_TARURL}"
echo "export EUPS_GIT<F6>REPO=${EUPS_GITREPO}"
