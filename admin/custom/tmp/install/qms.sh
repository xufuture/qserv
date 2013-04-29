#!/bin/bash
cd %(QSERV_SRC_DIR)s/meta
scons
%(QSERV_BASE_DIR)s/bin/mysqld_safe
%(QSERV_BASE_DIR)s/bin/mysql -vvv --socket %(MYSQLD_SOCK)s --user=%(MYSQLD_USER)s --pass=%(MYSQLD_PASS)s < %(QSERV_BASE_DIR)s/tmp/qms_qmsdb.sql
%(QSERV_BASE_DIR)s/bin/mysqld_safe stop
%(QSERV_BASE_DIR)s/bin/mysqladmin -S %(MYSQLD_SOCK)s --user=%(MYSQLD_USER)s --pass=%(MYSQLD_PASS)s shutdown
cp %(QSERV_BASE_DIR)s/tmp/qms_admclient.cnf ${HOME}/.qmsadm
