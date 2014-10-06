.. _quick-start-devel:

################################
Quick start guide for developers
################################

Using Qserv with your own custom code or arbitrary versions can be done by
connecting your local git clone with an eups software stack containing Qserv
dependencies.

**************
Pre-requisites
**************

Follow classical install :ref:`quick-start-pre-requisites`, and then build and install all
source dependencies for the current Qserv release:

.. code-block:: bash

   # use Qserv official distribution server
   eups distrib install --onlydepend --repository http://lsst-web.ncsa.illinois.edu/~fjammes/qserv qserv
   # only if you want to launch integration tests with your Qserv code
   eups distrib install qserv_testdata
   setup qserv_testdata

.. note::

   Above 'eups distrib' command will install dependencies for the current Qserv release. If you want to develop with an other set of dependencies, you may
   have to install them one by one, or specify a given Qserv version or tag (using -t).

.. _quick-start-devel-setup-qserv:

************************************
Setup your own Qserv version in eups
************************************

These commands explain how to connect your local Qserv git repository to an eups software stack containing Qserv dependencies.
Once Qserv dependencies are installed in eups stack, please use next commands in order to install your Qserv development version:

.. code-block:: bash

   # clone Qserv repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   # anonymous access :
   git clone git://git.lsstcorp.org/LSST/DMS/qserv
   # or authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/qserv
   # build and install your Qserv version
   cd qserv
   # if following "setup" command fails due to missing packages one has to
   # manually install those packages with regular "eups distrib install ..."
   setup -k -r .
   # build Qserv. Optional, covered by next command (i.e. install)
   eupspkg -e build
   # install Qserv in-place (i.e. in ${SRC_DIR}/qserv/)
   eupspkg -e PREFIX=$PWD install

   # Each time you want to test your code, run :
   eupspkg -e PREFIX=$PWD install

Once the qserv eups stack is integrated with your local Qserv repository, you
will need to configure and (if desired) test it (see :ref:`quick-start-configuration`).

*******************
Updating test cases
*******************

If you want to modify tests datasets, please clone Qserv test data repository :

.. code-block:: bash

   cd ~/src/
   # authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/testdata/qserv_testdata.git

In order to test it with your Qserv version :

.. code-block:: bash

   QSERV_TESTDATA_SRC_DIR=${HOME}/src/qserv_testdata/
   cd $QSERV_TESTDATA_SRC_DIR
   setup -k -r .
   eupspkg -e build                # build
   eupspkg -e PREFIX=$PWD install  # install in-place

*****************************************
Building Qserv with specific dependencies
*****************************************

You may want to build Qserv against a specific set of
dependencies.

Several solutions are available:

Setup dependencies after Qserv setup
====================================

.. code-block:: bash

   cd ${SRC_DIR}/qserv
   setup -r .
   setup foo 1.2.3
   setup bar 3.4.5


Save the current EUPS environment, and restore it later
=======================================================

.. code-block:: bash

   eups list -s > foo.tag # saves currently setup-ed products
   ...
   cd ${SRC_DIR}/qserv
   setup -t foo.tag -r .  # sets up the product with dependencies in file foo.tag

See https://dev.lsstcorp.org/trac/wiki/EupsTips#Tags for details.

Keep the dependency version-to-be-used declared as 'current'
============================================================

To declare a version of an installed product as 'current', do:

.. code-block:: bash

   eups declare -t current <product> <version>
   ...
   cd ${SRC_DIR}/qserv
   setup -r .

Declare your own, personal, EUPS tags
=====================================

When there is a need to build a specific ticket against a very specific set of 
versions, you can use EUPS tags to manage that.

Specifically, you can declare your own, personal, EUPS tags, as described at: 
https://dev.lsstcorp.org/trac/wiki/EupsTips#Tags

Once you edit your ~/.eups/startup.py as described in there, you will be able to do things such as:

.. code-block:: bash

   eups declare foo 1.2.3.4 -t dm1234
   eups declare bar 5.6.7.8 -t dm1234

So when you're working on resolving DM-1234 that needs those specific versions,
you can set them up with:

.. code-block:: bash

   ...
   cd ${SRC_DIR}/qserv
   setup -t dm1234 -r .

EUPS expert argue that this is the preferred way to do this, when you need it.
