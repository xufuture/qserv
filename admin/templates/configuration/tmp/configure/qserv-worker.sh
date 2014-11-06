#!/bin/bash

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
SQL_LOADER=${DIR}/tools/sql-loader.sh
SQL_FILE=qserv-worker.sql
MYSQLD_SOCK={{QSERV_RUN_DIR}}/var/lib/mysql-worker/mysql.sock

MYSQL_CMD="${MYSQL_DIR}/bin/mysql --no-defaults -vvv --user=${MYSQLD_USER} --sock=${MYSQLD_SOCK}"

mkdir -p {{QSERV_RUN_DIR}}/var/run/mysqld-worker

echo 
echo "-- Initializing Qserv wrk database "
if [ -r ${SQL_LOADER} ]; then
    . ${SQL_LOADER}
else
    >&2 echo "Unable to source ${SQL_LOADER}"
    exit 1
fi

echo "INFO: Qserv wrk initialization SUCCESSFUL"
