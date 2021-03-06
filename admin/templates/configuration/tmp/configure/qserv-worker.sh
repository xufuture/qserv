#!/bin/sh

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
SQL_LOADER=${DIR}/tools/sql-loader.sh
SQL_FILE=qserv-worker.sql

echo 
echo "-- Initializing Qserv Worker database "
if [ -r "${SQL_LOADER}" ]; then
    . "${SQL_LOADER}"
else
    >&2 echo "Unable to source ${SQL_LOADER}"
    exit 1
fi

echo "INFO: Qserv Worker initialization SUCCESSFUL"
