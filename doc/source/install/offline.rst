####################
Offline installation
####################

Cluster often doesn't provide internet access for security reasons, that's why
it is possible to install Qserv on machines without internet access.

Qserv distribution offers an archive containing distribution server data which can copied on a distributed file system and
used by offline servers in order to rebuild Qserv from source.

**************
Pre-requisites
**************

Install system dependencies
===========================

Here you may need to set-up a local apt or yum repository, or download manually all the system dependencies packages.

See :ref:`quick-start-pre-requisites-system-deps`

Download distribution server data
=================================

Please run next scripts under **non-root user account**:
This need to be done on a machine with internet access.

.. code-block:: bash

   SHARED_DIR=shared/dir/available/to/all/nodes
   cd SHARED_DIR    
   # use Qserv official distribution server
   curl -O http://lsst-web.ncsa.illinois.edu/~fjammes/qserv-offline/qserv-offline-distserver.tar.gz
   tar zxvf qserv-offline-distserver.tar.gz
   # python 2.7 is required, if it isn't available on you system, please download anaconda :
   curl -O http://repo.continuum.io/archive/Anaconda-1.8.0-Linux-x86_64.sh

Internet access won't be no more required during next steps.

************
Installation
************

.. code-block:: bash

   # python 2.7 is required, if it isn't available on you system, please download anaconda :
   bash ${SHARED_DIR}/Anaconda-1.8.0-Linux-x86_64.sh
   INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed
   # e.g. ~qserv, please note $INSTALL_DIR must be empty 
   bash ${SHARED_DIR}/qserv-install.sh -d ${SHARED_DIR}/distserver -i ${INSTALL_DIR}
