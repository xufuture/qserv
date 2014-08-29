cd ~/tarballs
rm ~/tarballs/qserv-offline-distserver.tar.gz
curl -O http://lsst-web.ncsa.illinois.edu/~fjammes/qserv-offline/qserv-offline-distserver.tar.gz
rm -rf ~/distserver/*
cd ~
tar zxvf ~/tarballs/qserv-offline-distserver.tar.gz
echo "DISTSERVER OK"
