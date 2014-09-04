##########################
Internet-free installation
##########################

Cluster often doesn't provide internet access for security reasons, that's why
it is possible to install Qserv on machines without internet access.
Qserv distribution offers an archive containing distribution server data which can copied on a distributed file system and used by internet-free servers in order to rebuild Qserv from source.

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

* only the read-only access to `INTERNET_FREE_DISTSERVER_DIR` is needed, **Internet access is no longer required**,
* use the **non-root user account** which will run Qserv,
* if you install Qserv on a cluster, perform next operations on each node.



You can then follow standard Qserv install procedure (Start here : :ref:`quick-start-install-lsst-stack`), except that you have to specify eups to use the local distribution server.
**This must be done twice** :

- before installing LSST stack (See :ref:`quick-start-install-lsst-stack`), replace `NEWINSTALL_URL` value with :

.. code-block:: bash

   NEWINSTALL_URL=${INTERNET_FREE_DISTSERVER_DIR}

- before installing Qserv (See :ref:`quick-start-install-qserv`), replace `EUPS_PKGROOT` value with :

.. code-block:: bash

   # `. loadLSST.sh` instruction updates EUPS_PKGROOT value, that's why you need to correct it 
   # before installing Qserv
   export EUPS_PKGROOT=${INTERNET_FREE_DISTSERVER_DIR}

  
