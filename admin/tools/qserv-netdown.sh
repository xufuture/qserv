#!/bin/sh

# on $REMOTE_HOST :
# rebuild lsst qserv qserv_testdata
# publish -t current -b bXX lsst qserv qserv_testdata

PKGROOT=/space/data-qserv/qserv-dist
REMOTE_HOST=lsst-dev.ncsa.illinois.edu

EUPS_VERSION=${EUPS_VERSION:-1.5.0}
EUPS_TARBALL="$EUPS_VERSION.tar.gz"
EUPS_TARURL="https://github.com/RobertLuptonTheGood/eups/archive/$EUPS_TARBALL"

EUPS_GITREPO="https://github.com/RobertLuptonTheGood/eups.git"

git_update_bare() {
    if [ -z "$1" ]; then
        echo "git_update_bare() requires one arguments"
        exit 1
    fi
    local giturl=$1
    local product=$(basename ${giturl})
    local retval=1

    if [ ! -d ${product} ]; then
        echo "Cloning ${giturl}"
        git clone --bare ${giturl} && retval=0
    else
        echo "Updating ${giturl}"
        cd ${product}
        git fetch origin +refs/heads/*:refs/heads/* && retval=0
        cd .. 
    fi

    if [ ! retval ]; then 
        echo "ERROR : git update failed"
    fi  
    return ${retval}
}

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

cat > git.include.files <<EOF
+ */.git/
+ */.git/config
# exclude everything else 
- */**
EOF

echo
echo "Downloading eups tarball"
echo "========================"
echo
curl -L ${EUPS_TARURL} > ${EUPS_TARBALL}


echo
echo "Downloading distribution server data on ${REMOTE_HOST}"
echo "======================================================"
echo
EUPS_PKGROOT="${PKGROOT}/eupspkg"
rsync -ave ssh ${REMOTE_HOST}:/lsst/home/fjammes/public_html/qserv/ ${EUPS_PKGROOT} ||
{
    echo "Unable to synchronize with Qserv distribution server on ${REMOTE_HOST}"
    exit 1
}

# newinstall.sh in EUPS_PKGROOT is obsolete
echo
echo "Downloading LSST stack install script"
echo "====================================="
echo
curl -O http://sw.lsstcorp.org/eupspkg/newinstall.sh
mv newinstall.sh ${EUPS_PKGROOT}

echo
echo "Downloading git repositories config for Qserv and dependencies on ${REMOTE_HOST}"
echo "======================================================================================="
echo
GIT_REPOS="${PKGROOT}/git-repositories"
GIT_REPOS_CFG="${GIT_REPOS}/config"
rsync -ave ssh --include-from=git.include.files ${REMOTE_HOST}:/lsst/home/fjammes/src/lsstsw-release/build/ ${GIT_REPOS_CFG} ||
{
    echo "Unable to download git repositories config on ${REMOTE_HOST}"
    exit 1
}

GIT_URLS=$(cat ${GIT_REPOS_CFG}/*/.git/config | grep "url =" | cut -d= -f2)
cd ${GIT_REPOS}
for url in ${GIT_URLS}
do
    if ! git_update_bare $url; then
        echo "Unable to synchronize with next git repository : $url"
        exit 1
    fi
done
cd ${PKGROOT}

EUPS_TARURL="file://${PKGROOT}/$EUPS_TARBALL"
EUPS_GITREPO="${PKGROOT}/eups.git"

echo "export EUPS_PKGROOT=${EUPS_PKGROOT}"
echo "export EUPS_TARURL=${EUPS_TARURL}"
echo "export EUPS_GIT_REPO=${EUPS_GITREPO}"
echo "export LOCAL_GIT_REPOS=${GIT_REPOS}"
