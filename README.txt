Quick start guide :
-------------------

Pre-requisites :
----------------

By default, Qserv install script rebuild MySQL. But MySQL configuration file (/etc/my.cnf on RedHat-like distributions or /etc/mysql/my.cnf on Debian-like distributions) can produce conflict with MySQL provided with Qserv package. So, you should backup and then remove this file.

Dependencies :
--------------

Depending on your distribution, you can install dependencies, as root, with next commands :

  # admin/qserv-install-deps-sl6.sh
  # admin/qserv-install-deps-ubuntu.sh

Installing Qserv :
------------------

  Configure the build :
  ---------------------

First do :
 
  $ cd /path_to_qserv_src/ 
  $ cp qserv-env.example.sh qserv-env.sh
  $ cp qserv-install-params.example.sh qserv-install-params.sh
  $ export QSERV_SRC=${PWD}

and then set your environment variables and install parameters in : 
- qserv-env.sh
- qserv-install-params.sh (restrictive rights recommended as it contains MySQL password)

QSERV_SRC is a variable which points to your Qserv source directory

You could add next line to your ~/.bashrc :
  source /path_to_qserv_src/qserv-env.sh 
it will provide you usefull aliases and update your PATH with qserv binaries path.
  
  Create base directories (Optional) :
  ------------------------------------

Then launch as root (don't forget to re-set QSERV_SRC before !) :

  # admin/qserv-init.sh

This step can also be done by hands, indeed this small script is straight-forward.

  Download source main package and dependencies :
  -----------------------------------------------

Then, as a normal user, download qserv source dependencies 
  $ admin/qserv-download.sh

  Run the full install :
  ----------------------

  $ admin/qserv-install-all.sh
It may take a while ...


An example of using Qserv with PT1.1 data is described in admin/examples/PT11data_use.txt

Official documentation : 
------------------------
It is located in ${QSERV_SRC}/admin/Install.txt

