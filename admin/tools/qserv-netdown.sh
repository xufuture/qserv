#!/bin/sh

PKGROOT=${HOME}/tmp
REMOTE_HOST=lsst-dev.ncsa.illinois.edu

EUPS_VERSION=${EUPS_VERSION:-1.5.0}
EUPS_TARBALL="$EUPS_VERSION.tar.gz"
EUPS_TARURL="https://github.com/RobertLuptonTheGood/eups/archive/$EUPS_TARBALL"

EUPS_GITREPO=$"https://github.com/RobertLuptonTheGood/eups.git"

cd ${PKGROOT}
EUPS_PKGROOT="${PKGROOT}/eupspkg_root"
EUPS_PKGROOT_QSERV="${PKGROOT}/eupspkg_root_qserv"
rsync -ave ssh ${REMOTE_HOST}:/lsst/home/lsstsw/distserver/production/ ${EUPS_PKGROOT}
rsync -ave ssh ${REMOTE_HOST}:/lsst/home/fjammes/public_html/qserv/ ${EUPS_PKGROOT_QSERV}

curl -L ${EUPS_TARURL} > ${EUPS_TARBALL} 
git clone ${EUPS_GITREPO}

EUPS_TARURL="file://${PKGROOT}/$EUPS_TARBALL"
EUPS_GITREPO="${PKGROOT}/eups.git"

echo "export EUPS_PKGROOT=${EUPS_PKGROOT}"
echo "export EUPS_PKGROOT_QSERV=${EUPS_PKGROOT_QSERV}"
echo "export EUPS_TARURL=${EUPS_TARURL}"
echo "export EUPS_GITREPO=${EUPS_GITREPO}"
