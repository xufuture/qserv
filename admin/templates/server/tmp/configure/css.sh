#!/usr/bin/env sh

QSERV_RUN_DIR=%(QSERV_RUN_DIR)s
HOME=%(HOME)s

cp ${QSERV_RUN_DIR}/tmp/configure/my.cnf ${HOME}/.my.cnf
