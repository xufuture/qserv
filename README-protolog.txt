This is a feature branch created by Bill Chickering on 3/13/14 that contains an
implementation of a prototype logging mechanism for qserv built using log4cxx.

To deploy an instance of qserv from this source code, in addition to any/all
procedures "normally" required, one must create a log4cxx XML configuration file
in the $QSERV_BASE/etc directory and change the hardcoded path within
$QSERV_BASE/qserv/core/modules/protolog/ProtoLog.cc. An example configuration
file called Log4cxxConfig.xml.example is included in this directory. Note that
this file contains multiple file paths that should updated for a particular
qserv instance.

Two preexisting qserv source files have been augmented to demonstrate the
logging mechanism. In the C++ layer, the add() function defined within
$QSERV_BASE/qserv/core/modules/control/AsyncQueryManager.cc and the __init__()
function defined within
$QSERV_BASE/qserv/core/modules/czar/lsst/qserv/master/app.py have example
logging statements.
