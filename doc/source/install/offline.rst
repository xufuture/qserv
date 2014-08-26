####################
Offline installation
####################

Cluster often doesn't provide internet access for security reasons, that's why
it is possible to install Qserv on machines without internet access.
Qserv distribution offers an archive containing distribution server data which can copied on a distributed file system and used by offline servers in order to rebuild Qserv from source.

**************
Pre-requisites
**************

Install system dependencies
===========================

This steps needs to be done on each machine of the cluster.
Here you may need to set-up a local apt or yum repository, or download manually all the system dependencies packages.
See :ref:`quick-start-pre-requisites-system-deps`

Download distribution server data
=================================

Please run next scripts under **non-root user account**.
This step needs to be done on a machine with internet access.

.. literalinclude:: ../../../admin/tools/prepare-install-from-offline-distserver.example.sh
   :language: bash
   :emphasize-lines: 15-
   :linenos:

************
Installation
************

Internet access isn't required during next steps. This operation can be performed on each node of the cluster which can access `OFFLINE_DISTSERVER_DIR`.

.. code-block:: bash

   # OPTIONAL : if python 2.7 isn't available, installing Anaconda may be a solution : 
   ${OFFLINE_DISTSERVER_DIR}/Anaconda-1.8.0-Linux-x86_64.sh 

.. literalinclude:: ../../../admin/tools/install-from-offline-distserver.example.sh
   :language: bash 
   :emphasize-lines: 11-
   :linenos:

********************************************************
Advanced HOW-TO : package an offline distribution server
********************************************************

This step will allow Qserv packagers to prepare Qserv offline distserver archive and put it on a web-server. This step needs to be done on a machine with internet access and which provides write access to a web-server repository.

First, please install `lsstsw` :

.. code-block:: bash

   SRC_DIR=dir/where/lsstsw/build/tool/will/be/installed
   cd ${SRC_DIR}
   git clone git://git.lsstcorp.org/LSST/DMS/devenv/lsstsw.git
   cd lsstsw
   ./bin/deploy

Then edit `${SRC_DIR}/lsstsw/etc/settings.cfg.sh` :

.. literalinclude:: ../_static/lsstsw/etc/settings.cfg.sh.diff
   :language: bash

Then, source `lsstsw` environment : 

.. code-block:: bash
   :emphasize-lines: 8 

   cat > setup.sh <<EOF
   # package mode will embed source code in each eupspkg package
   export EUPSPKG_SOURCE=package
   export LSSTSW=${SRC_DIR}/lsstsw
   export EUPS_PATH=\${LSSTSW}/stack
   . \${LSSTSW}/bin/setup.sh
   EOF
   source setup.sh

Then rebuild Qserv distribution :

.. code-block:: bash

   rebuild git
   setup git 1.8.5.2 
   rebuild -r 9.2 git lsst qserv qserv_testdata
   # bXXX is the build id and is available at the bottom of rebuild command standard output
   publish -b bXXX -t current git lsst qserv qserv_testdata

And then adapt and run next script :download:`qserv-package-offline-distserver.sh <../../../admin/tools/qserv-package-offline-distserver.sh>` to prepare the offline distserver archive and copy it to the webserver.

Testing
=======

If you want to test your offline distserver archive. There's no need to unplug the network cable, `iptable` can help you :

.. code-block:: bash

    # block internet acces for qserv user only, but keep local network access
    IP_ADRESS=172.17.8.3
    iptables -A OUTPUT -m owner --uid-owner qserv -d ${IP_ADRESS}/24 -j ACCEPT
    iptables -A OUTPUT -m owner --uid-owner qserv -d 127.0.0.0/8 -j ACCEPT
    iptables -A OUTPUT -m owner --uid-owner qserv -j DROP
    # undo
    iptables -D OUTPUT -m owner --uid-owner qserv -d ${IP_ADRESS}/24 -j ACCEPT
    iptables -D OUTPUT -m owner --uid-owner qserv -d 127.0.0.0/8 -j ACCEPT
    iptables -D OUTPUT -m owner --uid-owner qserv -j DROP
