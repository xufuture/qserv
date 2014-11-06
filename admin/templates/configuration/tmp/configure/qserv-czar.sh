#!/bin/bash

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
SQL_LOADER=${DIR}/tools/sql-loader.sh
SQL_FILE=qserv-czar.sql

MYSQLD_SOCK={{MYSQLD_SOCK}}
MYSQLD_PASS={{MYSQLD_PASS}}

MYSQL_CMD="${MYSQL_DIR}/bin/mysql --no-defaults -vvv --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

echo 
echo "-- Initializing Qserv czar database "
if [ -r ${SQL_LOADER} ]; then
    . ${SQL_LOADER}
else
    >&2 echo "Unable to source ${SQL_LOADER}"
    exit 1
fi

echo "INFO: Qserv Czar initialization SUCCESSFUL"
