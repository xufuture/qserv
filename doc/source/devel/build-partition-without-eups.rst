##########################################
Build partition package without using eups
##########################################

Developer may be interested to build Qserv packages without having to install
eups stack. An example is given here with the partition package, whose build
system relies on sconsUtils.

**************
Pre-requisites
**************

* Install scons v2.1.0 or greater.
* Install Boost binaries and header.

.. note::

   This build has been successfully tested on Fedora19 with manually installed boost-1.55, 
   and on Debian 7.4 with next apt-get packages :
   libboost1.49-dev,
   libboost-program-options1.49-dev,
   libboost-system1.49-dev,
   libboost-date-time1.49.0,
   libboost-serialization1.49.0,
   libboost-serialization1.49-dev,
   libboost-date-time1.49-dev,
   libboost-filesystem1.49-dev,
   libboost-thread1.49-dev,
   libboost-test1.49.0,
   libboost-test1.49-dev. 


*****
Build 
*****

* Install sconsUtils:
.. code-block:: bash

   # clone sconsUtils repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   git clone git://git.lsstcorp.org/LSST/DMS/devenv/sconsUtils

* Clone partition repository:
.. code-block:: bash

   # clone partition repository
   # anonymous access : 
   git clone git://git.lsstcorp.org/LSST/DMS/partition 
   # or authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/partition

* Retrieve sconsUtils configuration files for Boost in partition source directory: 
.. code-block:: bash

   cd partition
   git archive --remote=git://git.lsstcorp.org/LSST/external/boost --format=tar master ups/*.cfg | tar xv

.. note::

   Here you may have to edit "libs" variables of `ups/boost*.cfg` files if your boost version use libraries names with special suffixes.

   See http://stackoverflow.com/questions/8940249/boost-how-bjam-constructs-a-library-name and http://www.boost.org/doc/libs/1_56_0/more/getting_started/unix-variants.html#library-naming
   for additional information about Boost library naming convention.


* Define build environment:
.. code-block:: bash

   export PYTHONPATH=${PYTHONPATH}:${SRC_DIR}/sconsUtils/python/
   export PARTITION_DIR=${SRC_DIR}/partition/
   # Variables below are not required if you use system-provided Boost libraries
   # define them only if you use you own Boost libraries :
   export BOOST_DIR=dir/where/your/own/boost/is/installed
   export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:$BOOST_DIR/lib

* Launch the build:

.. code-block:: bash

   scons
 
