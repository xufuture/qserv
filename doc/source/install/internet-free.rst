####################
Offline installation
####################

Cluster often doesn't provide internet access for security reasons, that's why
it is possible to install Qserv on machines without internet access.
Qserv distribution offers an archive containing distribution server data which can copied on a distributed file system and used by internet-free servers in order to rebuild Qserv from source.

.. note::

   Please note that this high-level documentation is dedicated to system administrators and eups beginners.
   If you want to have a more fine-grained management of your install please see and adjust :ref:`quick-start-install-qserv` and :ref:`quick-start-devel-setup-qserv`. 

**************
Pre-requisites
**************

Install system dependencies
===========================

This step needs to be done on each machine of the cluster.
Here you may need to set-up a local apt or yum repository, or download manually all the system dependencies packages.
See :ref:`quick-start-pre-requisites-system-deps`

Download distribution server data
=================================

Please run next scripts under **non-root user account**.
This step needs to be done on a machine **with Internet access**.

.. literalinclude:: ../../../admin/tools/prepare-install-from-internet-free-distserver.example.sh
   :language: bash
   :emphasize-lines: 15-
   :linenos:

************
Installation
************

During next steps :

* only the read-only access to `OFFLINE_DISTSERVER_DIR` is needed, **Internet access is no longer required**,
* use the **non-root user account** which will run Qserv,
* if you install Qserv on a cluster, perform next operations on each node.

.. code-block:: bash

   # OPTIONAL : if python 2.7 isn't available, installing Anaconda may be a solution : 
   ${OFFLINE_DISTSERVER_DIR}/Anaconda-1.8.0-Linux-x86_64.sh 

.. literalinclude:: ../../../admin/tools/install-from-internet-free-distserver.example.sh
   :language: bash 
   :emphasize-lines: 11-
   :linenos:
