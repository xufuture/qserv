#### S E T U P #####################################################################

# Zookeeper
# For details, see http://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html
# Quick recipe:
#  1) wget http://www.picotopia.org/apache/zookeeper/stable/zookeeper-3.4.5.tar.gz
#  2) tar xfz zookeeper-3.4.5.tar.gz
#  3) mkdir dataDir
#  4) create zookeeper-3.4.5/conf/zoo.cfg with:
        tickTime=2000
        dataDir=<YourBasePath>/dataDir
        clientPort=2181
#  5) bin/zkServer.sh start

# Kazoo:
#  1) python -V
#  2) mkdir -p localPython/lib/python<version>/site-packages
#  3) export PYTHONPATH=<YourBasePath>/localPython/lib/python<version>/site-packages
#  4) easy_install --prefix=<YourBasePath>/localPython kazoo

# dbserv
# Needed: db.py. Hacky way to do it for now:
# 1) git clone git@git.lsstcorp.org:LSST/DMS/dbserv.git dbserv
# 2) export PYTHONPATH=<kazooPath>:<YourBasePath>/dbserv/python/lsst/dbserv/


#### T E S T I N G #################################################################

#### set path to the cssInterface
export PYTHONPATH=<kazooPath>:<YourBasePath>/dbserv/python/lsst/dbserv/:<YourBasePath>/qserv/css/

#### change kazoo logging level (if you find that it clatters css output)
#### 50- critical, 40-error, 30-warning, 20-info, 10-debug
export KAZOO_LOGGING=50

#### clean up everything
echo "drop everything;" | ./client/qserv_admin.py

#### in one window, start the watcher
  ## this is without logging:
  ./css/watcher.py
  ## this is with logging:
  ./css/watcher.py -v 10 -f watcher.log

#### in second window, run the test:
  ## this is without logging:
  ./client/qserv_admin.py  < ./client/tests/test_qserv_admin
  ## this is with logging:
  ./client/qserv_admin.py 0v 10 -f qadm.log < ./client/tests/test_qserv_admin
